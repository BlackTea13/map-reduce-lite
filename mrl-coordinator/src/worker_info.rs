use std::net::SocketAddr;

#[derive(Debug)]
pub struct WorkerInfo {
    pub addr: SocketAddr,
}

impl WorkerInfo {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}
