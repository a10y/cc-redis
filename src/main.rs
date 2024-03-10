use std::net::{TcpListener, TcpStream};

use redis_starter_rust::proto::core::Protocol;
use redis_starter_rust::proto::resp2::{ClientMessage, ProtocolError, Resp2, ServerMessage};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a thread per client.
                std::thread::spawn(move || {
                    handle_stream(stream).expect("handling client message parsing");
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

/// process a single stream, read a command and send back the value.
fn handle_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut protocol = Resp2::from(&mut stream)?;

    loop {
        let msg = match protocol.read_message() {
            Ok(msg) => msg,
            Err(inner) => {
                return match inner {
                    ProtocolError::ClientConnectionClosed => {
                        println!("Client disconnected");

                        Ok(())
                    }
                    _ => Err(inner.into()),
                };
            }
        };

        println!("received {msg:?}");

        match msg {
            ClientMessage::Ping => {
                println!("Replying with PONG");
                protocol.write_message(&ServerMessage::SimpleString("PONG".to_string()))?;
            }
            ClientMessage::Echo(echo) => {
                println!("Replying with {echo}");
                protocol.write_message(&ServerMessage::BulkString(echo))?;
            }
            ClientMessage::Command(inner) => {
                // Nothing to do. We do not handle this.
                println!("We do not implement 'COMMAND {inner}', ignoring.");
            }
        }
    }
}
