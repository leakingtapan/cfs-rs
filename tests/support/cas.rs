use cfs::hash::sha256;
use futures::{stream, Stream, StreamExt};
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::mpsc::{self as sync_mpsc, Receiver};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod bytestream {
    tonic::include_proto!("google.bytestream");
}

pub mod reapi {
    tonic::include_proto!("build.bazel.remote.execution.v2");
}

use bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use reapi::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use reapi::{
    BatchUpdateBlobsRequest, BatchUpdateBlobsResponse, Digest, Directory, FindMissingBlobsRequest,
    FindMissingBlobsResponse, GetTreeRequest, GetTreeResponse,
};

const TOKEN: &str = "test-token";

#[derive(Clone, Default)]
struct CasService {
    blobs: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    writes: Arc<Mutex<HashMap<String, usize>>>,
}

pub struct InMemoryCas {
    endpoint: String,
    service: CasService,
    shutdown: Option<oneshot::Sender<()>>,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
}

impl InMemoryCas {
    pub fn start() -> Self {
        let service = CasService::default();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind in-memory CAS");
        listener
            .set_nonblocking(true)
            .expect("configure in-memory CAS listener");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (done_tx, done_rx) = sync_mpsc::channel();
        let server_service = service.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("create CAS runtime");
            runtime.block_on(async move {
                let incoming =
                    TcpListenerStream::new(tokio::net::TcpListener::from_std(listener).unwrap());
                Server::builder()
                    .add_service(ByteStreamServer::new(server_service.clone()))
                    .add_service(ContentAddressableStorageServer::new(server_service))
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
        TOKEN
    }

    pub fn insert_blob(&self, data: impl Into<Vec<u8>>) -> Digest {
        let data = data.into();
        let digest = digest(&data);
        self.service
            .blobs
            .write()
            .unwrap()
            .insert(digest.hash.clone(), data);
        digest
    }

    pub fn insert_directory(&self, directory: &Directory) -> Digest {
        self.insert_blob(directory.encode_to_vec())
    }

    pub fn blob(&self, hash: &str) -> Option<Vec<u8>> {
        self.service.blobs.read().unwrap().get(hash).cloned()
    }

    pub fn write_count(&self, hash: &str) -> usize {
        *self.service.writes.lock().unwrap().get(hash).unwrap_or(&0)
    }
}

impl Drop for InMemoryCas {
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

fn digest(data: &[u8]) -> Digest {
    Digest {
        hash: sha256(data),
        size_bytes: data.len() as i64,
    }
}

fn require_auth<T>(request: &Request<T>) -> Result<(), Status> {
    let value = request
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization"))?;
    if value == format!("Bearer {}", TOKEN).as_str() {
        Ok(())
    } else {
        Err(Status::unauthenticated("invalid authorization"))
    }
}

fn parse_digest(resource_name: &str) -> Result<Digest, Status> {
    if !resource_name.starts_with("e2e/") || !resource_name.contains("/blobs/") {
        return Err(Status::invalid_argument("invalid resource name"));
    }
    let mut parts = resource_name.rsplit('/');
    let size_bytes = parts
        .next()
        .ok_or_else(|| Status::invalid_argument("resource has no size"))?
        .parse()
        .map_err(|_| Status::invalid_argument("resource has invalid size"))?;
    let hash = parts
        .next()
        .ok_or_else(|| Status::invalid_argument("resource has no hash"))?
        .to_string();
    Ok(Digest { hash, size_bytes })
}

fn validate_blob(expected: &Digest, data: &[u8]) -> Result<(), Status> {
    let actual = digest(data);
    if actual == *expected {
        Ok(())
    } else {
        Err(Status::invalid_argument("content does not match digest"))
    }
}

#[tonic::async_trait]
impl ByteStream for CasService {
    type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        require_auth(&request)?;
        let request = request.into_inner();
        let digest = parse_digest(&request.resource_name)?;
        let blobs = self.blobs.read().unwrap();
        let data = blobs
            .get(&digest.hash)
            .ok_or_else(|| Status::not_found("blob not found"))?;
        validate_blob(&digest, data)?;
        if request.read_offset < 0 || request.read_offset > data.len() as i64 {
            return Err(Status::out_of_range("invalid read offset"));
        }
        let start = request.read_offset as usize;
        let end = if request.read_limit == 0 {
            data.len()
        } else if request.read_limit < 0 {
            return Err(Status::invalid_argument("invalid read limit"));
        } else {
            data.len().min(start + request.read_limit as usize)
        };
        let responses = data[start..end]
            .chunks(8192)
            .map(|chunk| {
                Ok(ReadResponse {
                    data: chunk.to_vec(),
                })
            })
            .collect::<Vec<_>>();
        Ok(Response::new(Box::pin(stream::iter(responses))))
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_auth(&request)?;
        let mut stream = request.into_inner();
        let mut resource_name = None;
        let mut data = Vec::new();
        let mut finished = false;

        while let Some(request) = stream.next().await.transpose()? {
            if finished {
                return Err(Status::invalid_argument("data sent after finish_write"));
            }
            if request.write_offset != data.len() as i64 {
                return Err(Status::invalid_argument("non-contiguous write offset"));
            }
            match &resource_name {
                Some(name)
                    if !request.resource_name.is_empty() && name != &request.resource_name =>
                {
                    return Err(Status::invalid_argument("resource name changed"));
                }
                None => resource_name = Some(request.resource_name.clone()),
                _ => {}
            }
            data.extend(request.data);
            finished = request.finish_write;
        }

        if !finished {
            return Err(Status::invalid_argument("write was not finalized"));
        }
        let digest = parse_digest(
            resource_name
                .as_deref()
                .ok_or_else(|| Status::invalid_argument("empty write stream"))?,
        )?;
        validate_blob(&digest, &data)?;
        self.blobs
            .write()
            .unwrap()
            .insert(digest.hash.clone(), data);
        *self.writes.lock().unwrap().entry(digest.hash).or_default() += 1;
        Ok(Response::new(WriteResponse {
            committed_size: digest.size_bytes,
        }))
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        require_auth(&request)?;
        let digest = parse_digest(&request.into_inner().resource_name)?;
        let complete = self.blobs.read().unwrap().contains_key(&digest.hash);
        Ok(Response::new(QueryWriteStatusResponse {
            committed_size: if complete { digest.size_bytes } else { 0 },
            complete,
        }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasService {
    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        require_auth(&request)?;
        let request = request.into_inner();
        if request.instance_name != "e2e" {
            return Err(Status::invalid_argument("invalid instance name"));
        }
        let blobs = self.blobs.read().unwrap();
        let missing_blob_digests = request
            .blob_digests
            .into_iter()
            .filter(|digest| !blobs.contains_key(&digest.hash))
            .collect();
        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        require_auth(&request)?;
        let request = request.into_inner();
        if request.instance_name != "e2e" {
            return Err(Status::invalid_argument("invalid instance name"));
        }
        for update in request.requests {
            let digest = update
                .digest
                .ok_or_else(|| Status::invalid_argument("missing digest"))?;
            validate_blob(&digest, &update.data)?;
            self.blobs
                .write()
                .unwrap()
                .insert(digest.hash.clone(), update.data);
            *self.writes.lock().unwrap().entry(digest.hash).or_default() += 1;
        }
        Ok(Response::new(BatchUpdateBlobsResponse {}))
    }

    type GetTreeStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send>>;

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        require_auth(&request)?;
        let request = request.into_inner();
        if request.instance_name != "e2e" {
            return Err(Status::invalid_argument("invalid instance name"));
        }
        let page_size = if request.page_size <= 0 {
            usize::MAX
        } else {
            request.page_size as usize
        };
        let root = request
            .root_digest
            .ok_or_else(|| Status::invalid_argument("missing root digest"))?;
        let blobs = self.blobs.read().unwrap();
        let mut pending = VecDeque::from([root]);
        let mut directories = Vec::new();
        while let Some(digest) = pending.pop_front() {
            let bytes = blobs
                .get(&digest.hash)
                .ok_or_else(|| Status::not_found("directory not found"))?;
            validate_blob(&digest, bytes)?;
            let directory = Directory::decode(bytes.as_slice())
                .map_err(|_| Status::invalid_argument("invalid directory"))?;
            pending.extend(
                directory
                    .directories
                    .iter()
                    .filter_map(|node| node.digest.clone()),
            );
            directories.push(directory);
        }
        let last_page = directories.len().saturating_sub(1) / page_size;
        let responses = directories
            .chunks(page_size)
            .enumerate()
            .map(|(index, page)| {
                Ok(GetTreeResponse {
                    directories: page.to_vec(),
                    next_page_token: if index == last_page {
                        String::new()
                    } else {
                        (index + 1).to_string()
                    },
                })
            })
            .collect::<Vec<_>>();
        Ok(Response::new(Box::pin(stream::iter(responses))))
    }
}
