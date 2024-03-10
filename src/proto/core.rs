///! Redis stream protocol implementation.
pub trait Protocol {
    type ClientMessage;
    type ServerMessage;
    type Error: std::error::Error;

    /// Parse a message from the stream.
    /// Mutates self by tracking cursor information in the process.
    fn read_message(&mut self) -> Result<Self::ClientMessage, Self::Error>;

    fn write_message(&mut self, msg: &Self::ServerMessage) -> Result<(), Self::Error>;
}
