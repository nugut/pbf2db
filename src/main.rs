extern crate postgres;
extern crate osmpbfreader;
extern crate getopts;
extern crate chrono;
#[macro_use]
extern crate serde_derive;
extern crate toml;
extern crate redis;
use postgres::{Connection, TlsMode};
use std::process::exit;
use osmpbfreader::OsmObj;
use osmpbfreader::objects::Node;
use osmpbfreader::objects::Way;
use osmpbfreader::objects::Relation;
use osmpbfreader::objects::NodeId;
use std::thread;                                                                                                                                                            
use std::sync::mpsc;
use getopts::Options;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use redis::Commands;
use chrono::{Utc};
use std::time::{Instant};
use std::collections::BTreeSet;

fn main() {

    // get command line arguments 
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("c", "config", "path to config file", "NAME");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { 
            eprintln!("Error: {}", f.to_string());
            print_usage(&program, opts);
            exit(1);
        }
    };
    if matches.opt_present("h") || !matches.opt_present("c") {
        print_usage(&program, opts);
        exit(0);
    }

    let config = matches.opt_str("c");
    let config_file_path = match config {
        Some(s) => s,
        None => {
            print_usage(&program, opts);
            exit(1);
        }
    };

    let config: Config = read_config(&config_file_path);

	process_data(&config);

}

fn process_data(config: &Config) {
	
	log("create Redis connection");
	let redis_client = match redis::Client::open(config.main.redis_connection_url.as_str()) {
        Err(error) => {
            eprintln!("Can't create client for Redis on {}. Error: {}", config.main.redis_connection_url, error);
            exit(1);
        },
        Ok(cl) => cl
    };
	let mut redis_connection = match redis_client.get_connection() {
        Err(error) => {
            eprintln!("Can't connect to Redis on {}. Error: {}", config.main.redis_connection_url, error);
            exit(1);
        },
        Ok(c) => c 
    };
	if ! config.main.use_memory_for_derived_ids {
		if config.main.redis_flush_all {
			log("flush all data in Redis");
	    	let _ : () = redis::cmd("FLUSHALL").query(&mut redis_connection).unwrap();
		}
	}

	log("create Postgresql connection");
    let connection_url = config.main.pg_connection_url.clone();
    let conn = Connection::connect(connection_url, TlsMode::None);

    let conn = match conn {
        Err(error) => panic!("Exit due to connection error: {:?}", error), 
        Ok(connection) => connection
    };

    import_preprocess(&conn);

    let path = std::path::Path::new(&config.main.pbf_source);
    let pbf_file: File;
    match std::fs::File::open(&path) {
        Err(error) => {
            eprintln!("Can't open pbf source file: {}. Error: {}", config.main.pbf_source, error);
            exit(1);
        },
        Ok(f) => pbf_file = f 
    }

    let mut pbf = osmpbfreader::OsmPbfReader::new(pbf_file);
    let mut db_buffer = DbBuffer::new(&conn, &config.main);
	let mut related_ways = BTreeSet::new();
	let mut related_nodes = BTreeSet::new();

    let process_chain = [
		ProcessStage::ProcessCityNodes,
		ProcessStage::ProcessRoadWays,
		ProcessStage::ProcessCityBorderWays,
		ProcessStage::ProcessRelations, 
		ProcessStage::ProcessRelatedWays, 
		ProcessStage::ProcessRelatedNodes
	];

    for stage in process_chain.iter() {
		
		let start_stage_time = Instant::now();

        let stage_description = match stage {
			ProcessStage::ProcessCityNodes => "city nodes",
			ProcessStage::ProcessRoadWays => "road ways",
			ProcessStage::ProcessCityBorderWays => "city border ways",
            ProcessStage::ProcessRelations => "relations",
            ProcessStage::ProcessRelatedWays => "relation related ways",
            ProcessStage::ProcessRelatedNodes => "way/relation related nodes"
        };
        log(format!("process stage: {}", stage_description));

		pbf.rewind().expect("Failed to rewind file");

        for obj in pbf.iter() {
	
            let obj = obj.unwrap_or_else(|e| { eprintln!("{:?}", e); exit(1); });
            
            match obj {
                OsmObj::Node(ref node) => { 
                    loop { match stage {
						ProcessStage::ProcessCityNodes => {
							if ! is_desired_city_node(&node) { break; }
							db_buffer.save_node(&node);
							// comment this if not need node tags
                            for tag in node.tags.iter() {
                                db_buffer.save_node_tag(&node, &tag);
                            }	
						},
                        ProcessStage::ProcessRelatedNodes => {
							if config.main.use_memory_for_derived_ids {
								if ! is_desired_related_node_in_set(&node, &related_nodes) { break; }	
							} else {
								if ! is_desired_related_node(&node, &mut redis_connection) { break; }	
							}

                            db_buffer.save_node(&node);

                            // comment this if not need node tags
                            for tag in node.tags.iter() {
                                db_buffer.save_node_tag(&node, &tag);
                            }
                        },
                        _ => {} 
                    }; break; }
                },
                OsmObj::Way(ref way) => {
                    loop { match stage {
						ProcessStage::ProcessRoadWays => {
							if ! is_desired_road_way(&way) { break; }
							save_way(&way, &config, &mut db_buffer, &mut related_nodes, &mut redis_connection);	
						},
						ProcessStage::ProcessCityBorderWays => {
							if ! is_desired_city_border_way(&way) { break; }							
							save_way(&way, &config, &mut db_buffer, &mut related_nodes, &mut redis_connection);
						},
                        ProcessStage::ProcessRelatedWays => {
							if config.main.use_memory_for_derived_ids {
								if ! is_desired_related_way_in_set(&way, &related_ways) { break; }
							} else {
								if ! is_desired_related_way(&way, &mut redis_connection) { break; }	
							}
							save_way(&way, &config, &mut db_buffer, &mut related_nodes, &mut redis_connection);
                        },
                        _ => {} 
                    }; break; }
                },
                OsmObj::Relation(ref relation) => {
                    loop { match stage {
                        ProcessStage::ProcessRelations => {
	
                            if ! is_desired_relation(&relation) { break; }

                            db_buffer.save_relation(&relation); 

                            db_buffer.delete_relation_members(&relation);

                            let mut member_counter = 1;
                            for ref_obj in relation.refs.iter() {
                                let member_type;
                                let member_id: i64;
                                match ref_obj.member {
                                    osmpbfreader::OsmId::Node(osmpbfreader::objects::NodeId(id)) => {
                                        member_id = id;
                                        member_type = "node";
                                    },
                                    osmpbfreader::OsmId::Way(osmpbfreader::objects::WayId(id)) => {
                                        member_id = id;
                                        member_type = "way";
                                    }
                                    osmpbfreader::OsmId::Relation(osmpbfreader::objects::RelationId(id)) => {
                                        member_id = id;
                                        member_type = "relation";
                                    }
                                }

								if config.main.use_memory_for_derived_ids {
									db_buffer.save_relation_member_use_set(
										relation.id.0, member_type, member_id, &ref_obj.role, member_counter, 
										&mut related_nodes, &mut related_ways 
									);	
								} else {
									db_buffer.save_relation_member(relation.id.0, member_type, member_id, &ref_obj.role, member_counter, &mut redis_connection);	
								}
                                member_counter = member_counter + 1;
                            }

                            for tag in relation.tags.iter() {
                                db_buffer.save_relation_tag(&relation, &tag);
                            }
                        },
                        _ => {} 
                    }; break; }
                }
            }
        }
        // flush buffers and wait when all workers finish
        db_buffer.wait_sql_workers();

		// clear storages
		related_nodes.clear();
		related_ways.clear();

		let duration = start_stage_time.elapsed();
		log(format!("elapsed time for stage \"{}\" is {:?}", stage_description, duration));
    }

    import_postprocess(&conn);
}

fn log<T> (msg: T) where T: std::fmt::Display {
	let now = Utc::now();
	println!("{}: {}", now, msg);
}

fn read_config(config_file_path: &str) -> Config {
	let mut config_contents = String::new();
    match File::open(config_file_path) {
        Err(error) => {
            eprintln!("Can't open config file {}. Error: {}", config_file_path, error);
            exit(1);
        },
        Ok(mut f) => {
            match f.read_to_string(&mut config_contents) {
                Err(error) => {
                    eprintln!("Can't read config file {}. Error: {}", config_file_path, error);
                    exit(1);
                },
                Ok(_) => {
                    match toml::from_str(&config_contents) {
                        Err(error) => {
                            eprintln!("Config parser error: {}", error);
                            exit(1);
                        },
                        Ok(config_struct) => {
                            return config_struct;
                        }
                    }
                }
            }
        }
    }	
}

fn save_way(way: &Way, config: &Config, db_buffer: &mut DbBuffer, related_nodes: &mut BTreeSet<i64>, redis_connection: &mut redis::Connection) {
	db_buffer.save_way(&way);
	let mut nd_counter: i64 = 1;
    for node_id in way.nodes.iter() {
		if config.main.use_memory_for_derived_ids {
			db_buffer.save_way_node_use_set(way, &node_id, nd_counter, related_nodes);	
		} else {
			db_buffer.save_way_node(way, &node_id, nd_counter, redis_connection);	
		}
        nd_counter = nd_counter + 1;
    }

    for tag in way.tags.iter() {
        db_buffer.save_way_tag(way, &tag);
    }
}


// data structures

#[derive(Deserialize)]
pub struct Config {
    main: MainConfigSection,
}

#[derive(Deserialize)]
pub struct MainConfigSection {
    pbf_source: String,
    pg_connection_url: String,
    number_of_sql_workers: usize,
    flush_count: usize,
    redis_connection_url: String,
	redis_flush_all: bool,
	use_memory_for_derived_ids: bool
}

#[derive(Debug)]
pub enum ProcessStage {
	ProcessCityNodes,
	ProcessRoadWays,
	ProcessCityBorderWays,
    ProcessRelations,
    ProcessRelatedWays,
    ProcessRelatedNodes
}

pub struct DbBuffer<'a> {
    pub connection: &'a Connection,
    pub bulk_nodes: Vec<String>,
    pub bulk_node_tags: Vec<String>,
    pub bulk_ways: Vec<String>,
    pub bulk_way_nodes: Vec<String>,
    pub bulk_way_tags: Vec<String>,
    pub bulk_relations: Vec<String>,
    pub bulk_relation_members: Vec<String>,
    pub bulk_relation_tags: Vec<String>,
    pub flush_all: bool,
    pub flush_count: usize,
    pub bulk_counter: usize,
    pub endpoints: (mpsc::Sender<usize>, mpsc::Receiver<usize>),
    pub sql_workers: Vec<usize>,
    pub next_worker_num: usize,
    pub config: &'a MainConfigSection
}

pub enum BufferItemType {
    Node,
    NodeTag,
    Way,
    WayNode,
    WayTag,
    Relation,
    RelationMember,
    RelationTag
}

impl<'a> DbBuffer<'a> {

    pub fn new(conn: &'a Connection, config: &'a MainConfigSection) -> DbBuffer<'a> {

        DbBuffer { 
            connection: conn,
            bulk_nodes: vec![],
            bulk_node_tags: vec![],
            bulk_ways: vec![],
            bulk_way_nodes: vec![],
            bulk_way_tags: vec![],
            bulk_relations: vec![],
            bulk_relation_members: vec![],
            bulk_relation_tags: vec![],
            flush_all: false,
            flush_count: config.flush_count,
            bulk_counter: 0,
            endpoints: mpsc::channel(),
            sql_workers: vec!(0;config.number_of_sql_workers),
            next_worker_num: 1,
            config: config
        } 
    }

    pub fn bulk_end(&mut self) {
       self.bulk_counter = self.bulk_counter + 1;
       if !self.flush_all && (self.bulk_counter < self.flush_count) {
            return;
       }
       self.flush();
    }

	
    pub fn save_node(&mut self, node: &Node) {
        // in PHP lat and lon: intval(floatval($latitude)*1e7
        let s = format!("({}, {}, {})", node.id.0, node.decimicro_lat, node.decimicro_lon);
        self.bulk_nodes.push(s);    
        self.bulk_end();
    }

	
    pub fn save_node_tag(&mut self, node: &Node, tag: &(&String, &String)) {
        let (key, value) = *tag;
        // escape ' in keys and values
        let s = format!("({}, '{}', '{}')", node.id.0, key.replace("'", "''"), value.replace("'", "''"));
        self.bulk_node_tags.push(s);  
        self.bulk_end();
    }

	
    pub fn save_way(&mut self, way: &Way) {
        let s = format!("({})", way.id.0);
        self.bulk_ways.push(s);
        self.bulk_end();
    }

	
    pub fn save_way_node(&mut self, way: &Way, node_id: &NodeId, sequence_id: i64, redis: &mut redis::Connection) {
        let s = format!("({}, {}, {})", way.id.0, node_id.0, sequence_id);
        self.bulk_way_nodes.push(s);
        self.bulk_end();
        let key = format!("n-{}", node_id.0);
        redis_set_value(redis, &key, "1").expect("Failed to set value in Redis");
    }

	pub fn save_way_node_use_set(&mut self, way: &Way, node_id: &NodeId, sequence_id: i64, node_set: &mut BTreeSet<i64>) {
		let s = format!("({}, {}, {})", way.id.0, node_id.0, sequence_id);
        self.bulk_way_nodes.push(s);
        self.bulk_end();
		node_set.insert(node_id.0);	
	}

	
    pub fn save_way_tag(&mut self, way: &Way, tag: &(&String, &String)) {
        let (key, value) = *tag;
        // escape ' in keys and values
        let s = format!("({}, '{}', '{}')", way.id.0, key.replace("'", "''"), value.replace("'", "''"));
        self.bulk_way_tags.push(s);  
        self.bulk_end();
    }

	
    pub fn save_relation(&mut self, relation: &Relation) {
        let s = format!("({})", relation.id.0);
        self.bulk_relations.push(s);
        self.bulk_end();
    }

	
    pub fn delete_relation_members(&mut self, relation: &Relation) {
        let query_str = format!("DELETE FROM current_relation_members WHERE id={}", relation.id.0);
        match self.connection.execute(query_str.as_ref(), &[]) {
            Err(error) => panic!("Exit due to relation members delete. Error: {}", error),
            Ok(_) => {}
        }
    }

	
    pub fn save_relation_member(&mut self, relation_id: i64, member_type: &str, member_id: i64, member_role: &str, sequence_id: i64, redis: &mut redis::Connection) {
        let s = format!("({}, '{}', {}, '{}', {})", relation_id, member_type, member_id, member_role, sequence_id);
        self.bulk_relation_members.push(s);
        self.bulk_end();
        // add members to redis
        if member_type == "way" {
            let key = format!("w-{}", member_id);
            redis_set_value(redis, &key, "1").expect("Failed to set value in Redis");
        } else if member_type == "node" {
            let key = format!("n-{}", member_id);
            redis_set_value(redis, &key, "1").expect("Failed to set value in Redis");
        }
    }

	pub fn save_relation_member_use_set(&mut self, relation_id: i64, member_type: &str, member_id: i64, member_role: &str, sequence_id: i64, 
		node_set: &mut BTreeSet<i64>, way_set: &mut BTreeSet<i64>) {
		
		let s = format!("({}, '{}', {}, '{}', {})", relation_id, member_type, member_id, member_role, sequence_id);
        self.bulk_relation_members.push(s);
        self.bulk_end();
        // add members to redis
        if member_type == "way" {
            way_set.insert(member_id);
        } else if member_type == "node" {
			node_set.insert(member_id);
        }
	}

	
    pub fn save_relation_tag(&mut self, relation: &Relation, tag: &(&String, &String)) {
        let (key, value) = *tag;
        // escape ' in keys and values
        let s = format!("({}, '{}', '{}')", relation.id.0, key.replace("'", "''"), value.replace("'", "''"));
        self.bulk_relation_tags.push(s);  
        self.bulk_end();
    }

    pub fn flush(&mut self) {
        self.bulk_counter = 0;
        if (self.flush_all && self.bulk_nodes.len() > 0) || self.bulk_nodes.len() >= self.flush_count {
            let query_head = "INSERT INTO current_nodes (id, latitude, longitude) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id) DO UPDATE SET latitude = EXCLUDED.latitude,  longitude = EXCLUDED.longitude".to_string();
            let item_type = BufferItemType::Node;
            let error_msg = "INSERT NODE".to_string();
            let success_msg = "nodes".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_nodes.clear();
        } 
        if (self.flush_all && self.bulk_node_tags.len() > 0) || self.bulk_node_tags.len() >= self.flush_count {
            let query_head = "INSERT INTO current_node_tags (id, k, v) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id, k) DO UPDATE SET v = EXCLUDED.v".to_string();
            let item_type = BufferItemType::NodeTag;
            let error_msg = "INSERT NODE TAG".to_string();
            let success_msg = "node tags".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_node_tags.clear();
        } 
        if (self.flush_all && self.bulk_ways.len() > 0) || self.bulk_ways.len() >= self.flush_count {
            let query_head = "INSERT INTO current_ways (id) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id) DO NOTHING".to_string();
            let item_type = BufferItemType::Way;
            let error_msg = "INSERT WAY".to_string();
            let success_msg = "ways".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_ways.clear();
        } 
        if (self.flush_all && self.bulk_way_nodes.len() > 0) || self.bulk_way_nodes.len() >= self.flush_count {
            let query_head = "INSERT INTO current_way_nodes (id, node_id, sequence_id) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id, node_id, sequence_id) DO NOTHING".to_string();
            let item_type = BufferItemType::WayNode;
            let error_msg = "INSERT WAY NODE".to_string();
            let success_msg = "way nodes".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_way_nodes.clear();
        } 
        if (self.flush_all && self.bulk_way_tags.len() > 0) || self.bulk_way_tags.len() >= self.flush_count {
            let query_head = "INSERT INTO current_way_tags (id, k, v) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id, k) DO UPDATE SET v = EXCLUDED.v".to_string();
            let item_type = BufferItemType::WayTag;
            let error_msg = "INSERT WAY TAG".to_string();
            let success_msg = "way tags".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_way_tags.clear();
        } 
        if (self.flush_all && self.bulk_relations.len() > 0) || self.bulk_relations.len() >= self.flush_count {
            let query_head = "INSERT INTO current_relations (id) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id) DO NOTHING".to_string();
            let item_type = BufferItemType::Relation;
            let error_msg = "INSERT RELATION".to_string();
            let success_msg = "relations".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_relations.clear();
        } 
        if (self.flush_all && self.bulk_relation_members.len() > 0) || self.bulk_relation_members.len() >= self.flush_count {
            let query_head = "INSERT INTO current_relation_members(id, member_type, member_id, member_role, sequence_id) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id, member_type, member_id, member_role) DO NOTHING".to_string();
            let item_type = BufferItemType::RelationMember;
            let error_msg = "INSERT RELATION MEMBER".to_string();
            let success_msg = "relation members".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_relation_members.clear();
        } 
        if (self.flush_all && self.bulk_relation_tags.len() > 0) || self.bulk_relation_tags.len() >= self.flush_count {
            let query_head = "INSERT INTO current_relation_tags (id, k, v) VALUES ".to_string();
            let query_tail = " ON CONFLICT (id, k) DO UPDATE SET v = EXCLUDED.v".to_string();
            let item_type = BufferItemType::RelationTag;
            let error_msg = "INSERT RELATION TAG".to_string();
            let success_msg = "relation tags".to_string();
            self.insert_objects(&query_head, &query_tail, &item_type, &error_msg, &success_msg);
            self.bulk_relation_tags.clear();
        }

        if self.flush_all {
            self.flush_all = false;
        }
    }

    pub fn insert_objects(&mut self, query_head: &str, query_tail: &str, item_type: &BufferItemType, error_msg: &str, success_msg: &str) {
        let mut query_str = String::from(query_head);
        let mut data_idx: usize = 0;
        let error_message = String::from(error_msg);
        let success_message = String::from(success_msg);

        let bulk_items = match item_type {
            BufferItemType::Node => &self.bulk_nodes,
            BufferItemType::NodeTag => &self.bulk_node_tags,
            BufferItemType::Way => &self.bulk_ways,
            BufferItemType::WayNode => &self.bulk_way_nodes,
            BufferItemType::WayTag => &self.bulk_way_tags,
            BufferItemType::Relation => &self.bulk_relations,
            BufferItemType::RelationMember => &self.bulk_relation_members,
            BufferItemType::RelationTag => &self.bulk_relation_tags
        };

        for data in bulk_items.iter() {
            query_str.push_str(data);        
            if data_idx < (bulk_items.len() - 1) {
                query_str.push_str(",");
            }
            data_idx = data_idx + 1;
        }
        query_str.push_str(query_tail);

        // execute queries in parallel threads
        loop {
            match self.sql_workers.iter().position(|&v| v == 0) {
                Some(free_worker_index) => {
                    self.sql_workers[free_worker_index] = self.next_worker_num;
                    let tx = mpsc::Sender::clone(&self.endpoints.0);
                    let worker_num = self.next_worker_num;
                    self.next_worker_num += 1;
                    let query_string = query_str.clone();
                    let bulk_items_len = bulk_items.len();
                    let connection_url = self.config.pg_connection_url.clone();
                    thread::spawn(move || {
                        // create new connection, because Connection is not thread safe
                        let conn = Connection::connect(connection_url, TlsMode::None);
                        let conn = match conn {
                            Err(error) => panic!("Exit due to connection error: {:?}", error), 
                            Ok(connection) => connection
                        };
                        let updates = conn.execute(query_string.as_ref(), &[]);
                        match updates {
                            Err(error) => panic!("Exit due to {} error: {:?}", error_message, error),
                            Ok(_) => {
                                log(format!("insert {} {}", bulk_items_len, success_message));
                            }
                        }
                        tx.send(worker_num).unwrap();
                    });
                    break;
                },
                _ => {}
            }

            match self.endpoints.1.recv() {
                Ok(ended_worker_num) => {
                    match self.sql_workers.iter().position(|&v| v == ended_worker_num) {
                        Some(i) => self.sql_workers[i] = 0,
                        _ => panic!("Something wrong with threads: ended_worker not found in list")
                    }
                },
               _ => {}
            }
        }

    }

	
    pub fn wait_sql_workers(&mut self) {
        self.flush_all = true;
        self.flush();
        loop {
            match self.sql_workers.iter().position(|&v| v != 0) {
                Some(_) => {
                    match self.endpoints.1.recv() {
                        Ok(ended_worker_num) => {
                            match self.sql_workers.iter().position(|&v| v == ended_worker_num) {
                                Some(i) => self.sql_workers[i] = 0,
                                _ => {} 
                            }
                        },
                        _ => {} 
                    }
                },
                _ => break 
            }
        }
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} -c CONFIG_FILE", program);
    print!("{}", opts.usage(&brief));
}


fn is_desired_relation(relation: &Relation) -> bool {
   let rel = relation.clone();
   let object: OsmObj = OsmObj::Relation(rel);
    
   // find countries and regions, city boundaries
   let used_admin_levels = ["2", "3", "4", "5", "6", "7", "8", "9", "10"];
   if find_tag_by_key_value(&object, "type", "boundary") 
       && find_tag_by_key_value(&object, "boundary", "administrative")
       && find_tag_by_key_value_array(&object, "admin_level", &used_admin_levels) { 

           return true;
       }
   // find city borders
   let used_city_types = ["city", "suburb", "borough", "town", "village", "hamlet", "township", "locality"];
   if find_tag_by_key_value_array(&object, "place", &used_city_types) || find_tag_by_key_value_array(&object, "border_type", &used_city_types) {
        return true;     
   }

   return false;
}

fn is_desired_road_way(way: &Way) -> bool {
   let w = way.clone();
   let object: OsmObj = OsmObj::Way(w);
   // find ways for transport
   let used_way_types = [
        "motorway", "trunk", "primary", "secondary", "tertiary", "unclassified", "residential", "service",
        "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link"
   ];
   if find_tag_by_key_value(&object, "route", "ferry") // ferries
       || find_tag_by_key_value_array(&object, "highway", &used_way_types) // automobile roads
       {
        return true;
   }
   false
}

fn is_desired_city_border_way(way: &Way) -> bool {
   let w = way.clone();
   let object: OsmObj = OsmObj::Way(w);	
   let used_city_types = ["city", "suburb", "borough", "town", "village", "hamlet"];
   if find_tag_by_key_value_array(&object, "place", &used_city_types) {
        return true;
   }
   false
}

fn is_desired_related_way(way: &Way, redis_connection: &mut redis::Connection) -> bool {
   let way_id: i64 = match way.id {
       osmpbfreader::objects::WayId(id) => id
   };
   // If way is relation member, then add to database
   let redis_key = format!("w-{}", way_id);
   let value: i32 = redis_connection.get(&redis_key).unwrap_or(0i32);
   if value > 0 {
        // delete way from redis: clear space
        redis_del_value(redis_connection, &redis_key).expect("Failed to delete way id from Redis");
        return true;
   }

   false	
}

fn is_desired_related_way_in_set(way: &Way, set: &BTreeSet<i64>) -> bool {
	if set.contains(&way.id.0) { return true; }	
	false	
}


fn is_desired_city_node(node: &Node) -> bool {
   let n = node.clone();
   let object: OsmObj = OsmObj::Node(n);
   // cities
   let used_city_types = ["city", "suburb", "borough", "town", "village", "hamlet"];
   if find_tag_by_key(&object, "name") && find_tag_by_key_value_array(&object, "place", &used_city_types) {
        return true;
   }
   false	
}


fn is_desired_related_node(node: &Node, redis_connection: &mut redis::Connection) -> bool {
   // If relation or way member, then add in database
   let redis_key = format!("n-{}", node.id.0);
   let value: i32 = redis_connection.get(&redis_key).unwrap_or(0i32);
   if value > 0 {
        // delete node from redis: clear space
        redis_del_value(redis_connection, &redis_key).expect("Failed to delete node id from Redis");
        return true;
   }

   false
}

fn is_desired_related_node_in_set(node: &Node, set: &BTreeSet<i64>) -> bool {
	if set.contains(&node.id.0) { return true; }	
	false
}

fn find_tag_by_key(object: &OsmObj, search_key: &str) -> bool {

    for tag in object.tags().iter() {
        let (key, _) = tag;
        if key == search_key {
            return true;
        }
    }
    false
}

fn find_tag_by_key_value(object: &OsmObj, search_key: &str, search_value: &str) -> bool {

    for tag in object.tags().iter() {
        let (key, value) = tag;
        if key == search_key && value == search_value {
            return true;
        }
    }
    false
}

fn find_tag_by_key_value_array(object: &OsmObj, search_key: &str, search_values: &[&str]) -> bool {
    for tag in object.tags().iter() {
        let (key, value) = tag;
        if key == search_key {
            for search_value in search_values.iter() {
                if value == search_value { return true; }
            }
        }
    }
    false
}

fn import_preprocess(conn: &Connection) {
	log("clear Postgresql database");
    conn.execute("DELETE FROM current_nodes", &[]).unwrap();
    conn.execute("DELETE FROM current_node_tags", &[]).unwrap();
    conn.execute("DELETE FROM current_relation_members", &[]).unwrap();
    conn.execute("DELETE FROM current_relation_tags", &[]).unwrap();
    conn.execute("DELETE FROM current_relations", &[]).unwrap();
    conn.execute("DELETE FROM current_way_nodes", &[]).unwrap();
    conn.execute("DELETE FROM current_way_tags", &[]).unwrap();
    conn.execute("DELETE FROM current_ways", &[]).unwrap();
    // set tables as UNLOGGED: for speed
	log("set Postgres tables UNLOGGED: for write speed");
    conn.execute("alter table current_node_tags set unlogged", &[]).unwrap();
    conn.execute("alter table current_nodes set unlogged", &[]).unwrap();
    conn.execute("alter table current_relation_members set unlogged", &[]).unwrap();
    conn.execute("alter table current_relation_tags set unlogged", &[]).unwrap();
    conn.execute("alter table current_relations set unlogged", &[]).unwrap();
    conn.execute("alter table current_way_nodes set unlogged", &[]).unwrap();
    conn.execute("alter table current_way_tags set unlogged", &[]).unwrap();
    conn.execute("alter table current_ways set unlogged", &[]).unwrap();
}

fn import_postprocess(conn: &Connection) {
	log("set Postgres tables LOGGED");
    conn.execute("alter table current_node_tags set logged", &[]).unwrap();
    conn.execute("alter table current_nodes set logged", &[]).unwrap();
    conn.execute("alter table current_relation_members set logged", &[]).unwrap();
    conn.execute("alter table current_relation_tags set logged", &[]).unwrap();
    conn.execute("alter table current_relations set logged", &[]).unwrap();
    conn.execute("alter table current_way_nodes set logged", &[]).unwrap();
    conn.execute("alter table current_way_tags set logged", &[]).unwrap();
    conn.execute("alter table current_ways set logged", &[]).unwrap();
}

fn redis_set_value(con: &mut redis::Connection, key: &str, value: &str) -> redis::RedisResult<()> {
    let _ : () = con.set(key, value)?;
    Ok(())
}

fn redis_del_value(con: &mut redis::Connection, key: &str) -> redis::RedisResult<()> {
    let _ : () = con.del(key)?;
    Ok(())
}
