use super::memory::{reapi::Digest, reapi::Directory, MemoryCasService};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

pub struct TestCasServer {
    endpoint: String,
    service: MemoryCasService,
    shutdown: Option<oneshot::Sender<()>>,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
}

impl TestCasServer {
    pub fn start() -> Self {
        Self::start_with("e2e", "test-token")
    }

    pub fn start_with(instance_name: &str, token: &str) -> Self {
        let service = MemoryCasService::new(instance_name, token);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind in-memory CAS");
        listener
            .set_nonblocking(true)
            .expect("configure in-memory CAS listener");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let server_service = service.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("create CAS runtime");
            runtime.block_on(async move {
                let incoming =
                    TcpListenerStream::new(tokio::net::TcpListener::from_std(listener).unwrap());
                server_service
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("run in-memory CAS");
            });
            let _ = done_tx.send(());
        });

        Self {
            endpoint,
            service,
            shutdown: Some(shutdown_tx),
            done: done_rx,
            thread: Some(thread),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn token(&self) -> &str {
        self.service.token()
    }

    pub fn insert_blob(&self, data: impl Into<Vec<u8>>) -> Digest {
        self.service.insert_blob(data)
    }

    pub fn insert_directory(&self, directory: &Directory) -> Digest {
        self.service.insert_directory(directory)
    }

    pub fn blob(&self, hash: &str) -> Option<Vec<u8>> {
        self.service.test_blob(hash)
    }

    pub fn write_count(&self, hash: &str) -> usize {
        self.service.test_write_count(hash)
    }

    pub fn read_count(&self, hash: &str) -> usize {
        self.service.test_read_count(hash)
    }

    pub fn reject_batch_write(&self, hash: impl Into<String>) {
        self.service.test_reject_batch_write(hash.into());
    }
}

impl Drop for TestCasServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if self.done.recv_timeout(Duration::from_secs(10)).is_ok() {
            if let Some(thread) = self.thread.take() {
                thread.join().expect("join in-memory CAS thread");
            }
        }
    }
}
