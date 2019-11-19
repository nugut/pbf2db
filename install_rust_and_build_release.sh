#!/bin/bash                                                                                                                                                                 
                                                                                                                                                                            
if [[ -z $(which rustup) ]]
then
    # install rust by 'rustup' tool (standard and recommended)
    curl https://sh.rustup.rs -sSf | sh -s -- -y
fi

rustup self update
rustup update

if [[ -z $(which rustc) ]]
then
    echo Rust NOT installed
    exit 1
else
    echo Rust installed
fi

echo Build release
cd "$(dirname "$0")"
cargo build --release
