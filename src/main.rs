#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;
mod redis;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let redis = Arc::new(redis::Redis{data: Mutex::new(HashMap::new()), list: Mutex::new(HashMap::new())});

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let redis = redis.clone();
        thread::spawn(move || {
            redis.handle_request(&mut stream);
        });
    }
}
