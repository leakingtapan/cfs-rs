use crate::hash::sha256;
use futures::{stream, Stream, StreamExt};
use prost::Message;
#[cfg(feature = "test-utils")]
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
#[cfg(feature = "test-utils")]
use std::sync::Mutex;
use std::sync::{Arc, RwLock};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod bytestream {
    include!("generated/google.bytestream.rs");
}

pub mod reapi {
    include!("generated/build.bazel.remote.execution.v2.rs");
}

use bazel_remote_apis_rs::google::rpc::Status as RpcStatus;
use bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use reapi::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use reapi::{
    batch_update_blobs_response, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse, Digest,
    Directory, FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest, GetTreeResponse,
};

#[derive(Clone)]
pub struct MemoryCasService {
    blobs: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    #[cfg(feature = "test-utils")]
    writes: Arc<Mutex<HashMap<String, usize>>>,
    #[cfg(feature = "test-utils")]
    reads: Arc<Mutex<HashMap<String, usize>>>,
    #[cfg(feature = "test-utils")]
    rejected_batch_writes: Arc<RwLock<HashSet<String>>>,
    instance_name: String,
    token: String,
}

impl MemoryCasService {
    pub fn new(instance_name: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            blobs: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "test-utils")]
            writes: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "test-utils")]
            reads: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "test-utils")]
            rejected_batch_writes: Arc::new(RwLock::new(HashSet::new())),
            instance_name: instance_name.into(),
            token: token.into(),
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn insert_blob(&self, data: impl Into<Vec<u8>>) -> Digest {
        let data = data.into();
        let digest = digest(&data);
        self.blobs
            .write()
            .unwrap()
            .insert(digest.hash.clone(), data);
        digest
    }

    pub fn insert_directory(&self, directory: &Directory) -> Digest {
        self.insert_blob(directory.encode_to_vec())
    }

    pub async fn serve_with_incoming_shutdown<I, IO, IE, F>(
        self,
        incoming: I,
        shutdown: F,
    ) -> Result<(), tonic::transport::Error>
    where
        I: futures::Stream<Item = Result<IO, IE>> + Send + 'static,
        IO: tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + tonic::transport::server::Connected
            + Send
            + Unpin
            + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>>,
        F: Future<Output = ()> + Send + 'static,
    {
        Server::builder()
            .add_service(ByteStreamServer::new(self.clone()))
            .add_service(ContentAddressableStorageServer::new(self))
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
    }

    fn require_auth<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let value = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization"))?;
        if value == format!("Bearer {}", self.token).as_str() {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid authorization"))
        }
    }

    fn parse_digest(&self, resource_name: &str) -> Result<Digest, Status> {
        let prefix = if self.instance_name.is_empty() {
            "/".to_string()
        } else {
            format!("{}/", self.instance_name)
        };
        if !resource_name.starts_with(&prefix) || !resource_name.contains("/blobs/") {
            return Err(Status::invalid_argument("invalid resource name"));
        }
        parse_digest(resource_name)
    }

    fn require_instance(&self, instance_name: &str) -> Result<(), Status> {
        if instance_name == self.instance_name {
            Ok(())
        } else {
            Err(Status::invalid_argument("invalid instance name"))
        }
    }

    #[cfg(feature = "test-utils")]
    pub(crate) fn test_blob(&self, hash: &str) -> Option<Vec<u8>> {
        self.blobs.read().unwrap().get(hash).cloned()
    }

    #[cfg(feature = "test-utils")]
    pub(crate) fn test_write_count(&self, hash: &str) -> usize {
        *self.writes.lock().unwrap().get(hash).unwrap_or(&0)
    }

    #[cfg(feature = "test-utils")]
    pub(crate) fn test_read_count(&self, hash: &str) -> usize {
        *self.reads.lock().unwrap().get(hash).unwrap_or(&0)
    }

    #[cfg(feature = "test-utils")]
    pub(crate) fn test_reject_batch_write(&self, hash: String) {
        self.rejected_batch_writes.write().unwrap().insert(hash);
    }

    #[cfg(feature = "test-utils")]
    fn rejects_batch_write(&self, hash: &str) -> bool {
        self.rejected_batch_writes.read().unwrap().contains(hash)
    }

    #[cfg(not(feature = "test-utils"))]
    fn rejects_batch_write(&self, _hash: &str) -> bool {
        false
    }
}

fn digest(data: &[u8]) -> Digest {
    Digest {
        hash: sha256(data),
        size_bytes: data.len() as i64,
    }
}

fn parse_digest(resource_name: &str) -> Result<Digest, Status> {
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
impl ByteStream for MemoryCasService {
    type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        self.require_auth(&request)?;
        let request = request.into_inner();
        let digest = self.parse_digest(&request.resource_name)?;
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
        #[cfg(feature = "test-utils")]
        {
            *self
                .reads
                .lock()
                .unwrap()
                .entry(digest.hash.clone())
                .or_default() += 1;
        }
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
        self.require_auth(&request)?;
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
        let digest = self.parse_digest(
            resource_name
                .as_deref()
                .ok_or_else(|| Status::invalid_argument("empty write stream"))?,
        )?;
        validate_blob(&digest, &data)?;
        self.blobs
            .write()
            .unwrap()
            .insert(digest.hash.clone(), data);
        #[cfg(feature = "test-utils")]
        {
            *self.writes.lock().unwrap().entry(digest.hash).or_default() += 1;
        }
        Ok(Response::new(WriteResponse {
            committed_size: digest.size_bytes,
        }))
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        self.require_auth(&request)?;
        let digest = self.parse_digest(&request.into_inner().resource_name)?;
        let complete = self.blobs.read().unwrap().contains_key(&digest.hash);
        Ok(Response::new(QueryWriteStatusResponse {
            committed_size: if complete { digest.size_bytes } else { 0 },
            complete,
        }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for MemoryCasService {
    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        self.require_auth(&request)?;
        let request = request.into_inner();
        self.require_instance(&request.instance_name)?;
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
        self.require_auth(&request)?;
        let request = request.into_inner();
        self.require_instance(&request.instance_name)?;
        let mut responses = Vec::with_capacity(request.requests.len());
        for update in request.requests {
            let digest = update.digest;
            let result = match digest.as_ref() {
                None => Err(Status::invalid_argument("missing digest")),
                Some(digest) if self.rejects_batch_write(&digest.hash) => {
                    Err(Status::internal("injected batch upload failure"))
                }
                Some(digest) => validate_blob(digest, &update.data),
            };

            if result.is_ok() {
                let digest = digest.as_ref().unwrap();
                self.blobs
                    .write()
                    .unwrap()
                    .insert(digest.hash.clone(), update.data);
                #[cfg(feature = "test-utils")]
                {
                    *self
                        .writes
                        .lock()
                        .unwrap()
                        .entry(digest.hash.clone())
                        .or_default() += 1;
                }
            }
            let status = match result {
                Ok(()) => RpcStatus {
                    code: 0,
                    message: String::new(),
                    details: Vec::new(),
                },
                Err(error) => RpcStatus {
                    code: error.code() as i32,
                    message: error.message().to_string(),
                    details: Vec::new(),
                },
            };
            responses.push(batch_update_blobs_response::Response {
                digest,
                status: Some(status),
            });
        }
        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }

    type GetTreeStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send>>;

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        self.require_auth(&request)?;
        let request = request.into_inner();
        self.require_instance(&request.instance_name)?;
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
