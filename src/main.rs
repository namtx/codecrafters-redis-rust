#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        thread::spawn(move || {
            loop {
                let mut buffer = [0; 1024];
                stream.read(&mut buffer).unwrap();
                stream.write(b"+PONG\r\n").unwrap();
            }
        });
    }
}
