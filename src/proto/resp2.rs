use std::io;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;

/// Maximum length of a single line in the protocol.
/// This is used to track our buffer size.
pub const MAX_LINE: usize = 2048;

#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("Expected '{0}', found '{1}'")]
    ParsingFailed(String, String),
    #[error("client connection closed")]
    ClientConnectionClosed,
    #[error("Io error: {0}")]
    Io(#[from] io::Error),
    #[error("generic error: {0}")]
    Generic(#[from] anyhow::Error),
    #[error("integer parsing failed: {0}")]
    ParseInt(#[from] ParseIntError),
    #[error("failed to read data as utf-8: {0}")]
    ReadUtf8(#[from] FromUtf8Error),
}

/// RESP3 protocol core type.
pub struct Resp2<T>
where
    T: Read + Write,
{
    reader: BufReader<T>,
    writer: BufWriter<T>,
}

impl Resp2<TcpStream> {
    // Wrap two values
    pub fn from(wrapped: &mut TcpStream) -> anyhow::Result<Self> {
        let reader = BufReader::new(wrapped.try_clone()?);
        let writer = BufWriter::new(wrapped.try_clone()?);

        Ok(Self { reader, writer })
    }
}

#[derive(Debug, Clone)]
pub enum ClientMessage {
    Ping,
    Echo(String),
    Command(String),
    Set(String, String, SetOptions),
    Get(String),
}

#[derive(Debug, Default, Clone)]
pub struct SetOptions {
    pub px: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum ServerMessage {
    NullString,
    BulkString(String),
    SimpleString(String),
}

impl<T: Read + Write> super::core::Protocol for Resp2<T> {
    type ClientMessage = ClientMessage;
    type ServerMessage = ServerMessage;
    type Error = ProtocolError;

    // TODO(aduffy): should fail if missing \r, so should split based on \r\n and not just \n
    fn read_message(&mut self) -> Result<Self::ClientMessage, Self::Error> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        let line = line.trim_end();

        println!("READ PROTOCOL LINE: '{line}'");
        if line.is_empty() {
            return Err(ProtocolError::ClientConnectionClosed);
        }

        return if line.starts_with("*") {
            // Bulk String parser
            let rest = &line[1..];
            let num_elems = usize::from_str(rest)?;
            println!("PARSING {num_elems} bulk string elems");

            let mut elements = Vec::new();
            for _ in 0..num_elems {
                let elem = read_bulk_str_element(&mut self.reader)?;
                println!("PUSHING {elem}");
                elements.push(elem);
            }

            let client_message: ClientMessage = elements.try_into()?;

            Ok(client_message)
        } else {
            // Inline command string pathway
            let elements: Vec<_> = line.split_ascii_whitespace().collect();
            let client_message: ClientMessage = elements.try_into()?;

            Ok(client_message)
        };
    }

    fn write_message(&mut self, msg: &Self::ServerMessage) -> Result<(), Self::Error> {
        match msg {
            ServerMessage::NullString => {
                self.writer.write(b"$-1\r\n")?;
                self.writer.flush()?;

                Ok(())
            }
            ServerMessage::BulkString(bulk) => {
                // Send an '$', the length of the string, \r\n, the text, and then \r\n
                let binary = bulk.as_bytes();
                self.writer.write(b"$")?;
                self.writer.write(binary.len().to_string().as_bytes())?;
                self.writer.write(b"\r\n")?;
                self.writer.write(binary)?;
                self.writer.write(b"\r\n")?;
                self.writer.flush()?;

                Ok(())
            }
            ServerMessage::SimpleString(simple) => {
                // Send a '+', the message, and '\r\n'
                self.writer.write(b"+")?;
                self.writer.write(simple.as_bytes())?;
                self.writer.write(b"\r\n")?;
                self.writer.flush()?;

                Ok(())
            }
        }
    }
}

// We have a reader, read a single token and figure it out.
fn read_bulk_str_element<B: Read>(stream: &mut BufReader<B>) -> Result<String, ProtocolError> {
    read_literal(stream, "$")?;
    let length = read_length(stream)?;
    let data = read_n(stream, length)?;

    // Consume the trailing CRLF
    stream.consume(2);

    Ok(data)
}

fn read_literal<B: Read>(stream: &mut BufReader<B>, value: &str) -> Result<(), ProtocolError> {
    let mut buf = vec![0; value.bytes().len()];
    stream.read_exact(&mut buf)?;

    if buf != value.as_bytes() {
        return Err(ProtocolError::ParsingFailed(
            value.to_string(),
            String::from_utf8(buf)?.to_string(),
        ));
    }

    Ok(())
}

fn read_length<B: Read>(stream: &mut BufReader<B>) -> Result<usize, ProtocolError> {
    let mut buf = String::new();
    stream.read_line(&mut buf)?;

    let buf = buf.trim_end();

    // Ensure that the next byte works properly.
    let length = usize::from_str(&buf)?;
    Ok(length)
}

/// Read N characters from the stream.
fn read_n<B: Read>(stream: &mut BufReader<B>, n: usize) -> Result<String, ProtocolError> {
    let mut buf = vec![0; n];
    stream.read_exact(&mut buf[..])?;

    let stringified = String::from_utf8(buf)?;

    Ok(stringified)
}

impl<S: AsRef<str>> TryFrom<Vec<S>> for ClientMessage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<S>) -> Result<Self, Self::Error> {
        let element = match value.first() {
            None => {
                return Err(anyhow::anyhow!("expected command list to be non-empty"));
            }
            Some(first) => first,
        };

        match element.as_ref().to_lowercase().as_str() {
            "ping" => Ok(ClientMessage::Ping),
            "echo" => {
                // Foist some of the rest parameters here instead.
                let rest = &value[1..]
                    .iter()
                    .map(|elem| elem.as_ref().to_string())
                    .collect::<Vec<_>>()
                    .join(" ");

                Ok(ClientMessage::Echo(rest.clone()))
            }
            "command" => {
                let rest = value[1].as_ref().to_string();

                Ok(ClientMessage::Command(rest))
            }
            "set" => {
                let name = value[1].as_ref().to_string();
                let val = value[2].as_ref().to_string();

                let mut options = SetOptions::default();
                if value.len() >= 5 {
                    let option_key = value[3].as_ref().to_ascii_lowercase();
                    let option_value = value[4].as_ref().to_ascii_lowercase();

                    match option_key.as_str() {
                        "px" => {
                            let option_value = u32::from_str(option_value.as_str())?;
                            options.px = Some(option_value);
                        }
                        _ => {
                            println!("unrecognized option {option_key}: ignoring");
                        }
                    }
                }

                Ok(ClientMessage::Set(name, val, options))
            }
            "get" => {
                let rest = value[1].as_ref().to_string();

                Ok(ClientMessage::Get(rest))
            }
            _ => Err(anyhow::anyhow!("Invalid command")),
        }
    }
}
