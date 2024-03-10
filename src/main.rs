use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use redis_starter_rust::proto::core::Protocol;
use redis_starter_rust::proto::resp2::{ClientMessage, ProtocolError, Resp2, ServerMessage};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let dict: HashMap<String, Value> = HashMap::new();
    let shared_dict = Arc::new(Mutex::new(dict));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a thread per client.
                let shared_dict = shared_dict.clone();
                std::thread::spawn(move || {
                    handle_stream(stream, shared_dict).expect("handling client message parsing");
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Clone)]
struct Value {
    value: String,
    insertion_time: Instant,
    expiry_millis: u32,
}

/// process a single stream, read a command and send back the value.
fn handle_stream(
    mut stream: TcpStream,
    shared_dict: Arc<Mutex<HashMap<String, Value>>>,
) -> anyhow::Result<()> {
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
            ClientMessage::Set(name, val, options) => {
                let value = Value {
                    value: val,
                    insertion_time: Instant::now(),
                    expiry_millis: options.px.unwrap_or_else(|| u32::MAX),
                };

                {
                    let mut locked = shared_dict.lock().unwrap();
                    locked.insert(name, value);
                }

                protocol.write_message(&ServerMessage::SimpleString("OK".to_string()))?;
            }
            ClientMessage::Get(name) => {
                let value = {
                    let mut locked = shared_dict.lock().unwrap();

                    match locked.get(&name) {
                        None => None,
                        Some(value) => {
                            let since_millis = Instant::now()
                                .duration_since(value.insertion_time)
                                .as_millis();

                            // Drop expired items.
                            if since_millis > value.expiry_millis as u128 {
                                locked.remove(&name);

                                None
                            } else {
                                Some(value.clone())
                            }
                        }
                    }
                };

                if let Some(value) = value {
                    protocol.write_message(&ServerMessage::BulkString(value.value.clone()))?;
                } else {
                    protocol.write_message(&ServerMessage::NullString)?;
                }
            }
        }
    }
}
