use std::io::{Read, Write};
// Uncomment this block to pass the first stage
use std::net::{TcpListener, TcpStream};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_stream(stream).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

static PONG: &'static str = "+PONG\r\n";

/// process a single stream, read a command and send back the value.
fn handle_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    stream.write(PONG.as_bytes())?;

    Ok(())
}