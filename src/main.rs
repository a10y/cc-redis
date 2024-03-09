use std::io::{BufRead, BufReader, Write};
// Uncomment this block to pass the first stage
use std::net::{TcpListener, TcpStream};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a thread per client.
                std::thread::spawn(move || {
                    handle_stream(stream).unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

static PONG: &[u8] = b"+PONG\r\n";

/// process a single stream, read a command and send back the value.
fn handle_stream(stream: TcpStream) -> anyhow::Result<()> {
    let mut command = String::new();
    let mut stream = BufReader::new(stream);

    loop {
        let n_bytes = stream.read_line(&mut command)?;

        println!("COMMAND: {command}");

        if n_bytes == 0 {
            break;
        }

        if command.to_ascii_lowercase().starts_with("ping") {
            stream.get_mut().write(PONG)?;
        }
    }

    println!("empty input");

    Ok(())
}