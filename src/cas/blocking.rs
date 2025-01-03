use super::auth::AuthInterceptor;
use super::configs::Configs;
use crate::git::get_git_root;
use crate::git::get_lfs_object_path;
use crate::git::git_lfs_fetch;
use crate::lfs::LfsFile;
use anyhow::Result;
use bazel_remote_apis_rs::build::bazel::remote::execution::v2::content_addressable_storage_client::*;
use bazel_remote_apis_rs::build::bazel::remote::execution::v2::*;
use bazel_remote_apis_rs::google::bytestream::byte_stream_client::ByteStreamClient;
use bazel_remote_apis_rs::google::bytestream::{ReadRequest, WriteRequest};
use futures::Stream;
use prost::Message;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::str;
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use tokio::fs::File;
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use uuid::Uuid;

type CasClient = ContentAddressableStorageClient<InterceptedService<Channel, AuthInterceptor>>;

type BsClient = ByteStreamClient<InterceptedService<Channel, AuthInterceptor>>;

pub struct Client {
    inner: CasClient,
    bs_client: BsClient,
    rt: Runtime,
}

impl Client {
    pub fn new() -> Result<Client> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let inner = rt.block_on(create_cas_client())?;
        let bs_client = rt.block_on(create_bs_client())?;
        Ok(Self {
            inner,
            rt,
            bs_client,
        })
    }

    /// get all the directories descended from the requested root digest
    pub fn get_tree(&mut self, hash: &str, size: i64) -> Result<Vec<Directory>> {
        self.rt
            .block_on(read_directories(&mut self.inner, hash, size))
    }

    /// get the content of a single blob
    // pub fn read_blob(&mut self, hash: &str, size: i64) -> Result<&Vec<u8>> {
    //     let blob = self
    //         .rt
    //         .block_on(bs_read_blob(&mut self.bs_client, hash, size))?;
    //     let res = Vec::from(blob);
    //     return Ok(&res);
    // }

    /// get a single directory given the hash and size
    // pub fn get_dir(&mut self, hash: &str, size: i64) -> Result<Directory> {
    //     let dir_bytes = self.read_blob(hash, size)?;
    //     Directory::decode(&mut Cursor::new(dir_bytes)).map_err(|e| e.into())
    // }

    pub fn write_blob(&mut self, digest: &Digest, buff: &[u8]) -> Result<()> {
        self.rt
            .block_on(bs_write_blob(&mut self.bs_client, digest, buff.to_vec()))
    }

    /// writes a large file into CAS
    pub fn write_file(&mut self, digest: &Digest, path: &Path) -> Result<()> {
        self.rt.block_on(bs_write_file(
            &mut self.bs_client,
            &digest,
            path.to_path_buf(),
        ))
    }
}

#[derive(Debug, Clone)]
pub enum WriteTask {
    WriteFile(WriteFile),

    WriteBlob(WriteBlob),
}

#[derive(Debug, Clone)]
pub struct WriteBlob {
    digest: Digest,
    buff: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WriteFile {
    digest: Digest,
    path: PathBuf,
}

pub struct NonBlockingClient {
    /// the sender
    sender: mpsc::Sender<WriteTask>,
}

pub fn spawn_receiver() -> (mpsc::Sender<WriteTask>, JoinHandle<()>) {
    let (send, recv) = mpsc::channel(1024);
    let (ft_send, ft_recv) = mpsc::channel(1024);
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.spawn(async move {
            filter_loop(recv, ft_send).await;
        });

        rt.block_on(async move {
            receiver_loop(ft_recv).await;
        });
    });

    (send, handle)
}

async fn identical_filter_loop(mut recv: mpsc::Receiver<WriteTask>, send: mpsc::Sender<WriteTask>) {
    while let Some(task) = recv.recv().await {
        // println!("task: {:?}", task);
        send.send(task).await;
    }
    //println!("identical filter loop done");
}

async fn filter_loop(mut recv: mpsc::Receiver<WriteTask>, send: mpsc::Sender<WriteTask>) {
    let mut pending = vec![];
    let mut bt_client = create_cas_client().await.unwrap();
    while let Some(task) = recv.recv().await {
        pending.push(task);

        // max gRPC message size is 4MiB and each request is (digest 64B + instance name 6B)
        if pending.len() >= 50000 {
            let mut filtered = vec![];
            filtered.append(&mut pending);
            let filtered = find_missing_blobs(&mut bt_client, filtered).await;

            for t in filtered {
                let res = send.send(t).await;
                if res.is_err() {
                    println!("failed to send the message {:?}", res.unwrap_err());
                    break;
                }
            }
        }
    }

    let mut filtered = vec![];
    filtered.append(&mut pending);
    let filtered = find_missing_blobs(&mut bt_client, filtered).await;

    for t in filtered {
        let res = send.send(t).await;
        if res.is_err() {
            println!("failed to send the message {:?}", res.unwrap_err());
        }
    }
    //println!("Missing blobs filter loop done");
}

async fn find_missing_blobs(bt_client: &mut CasClient, pending: Vec<WriteTask>) -> Vec<WriteTask> {
    let mut digests = vec![];
    for p in pending.clone() {
        match p {
            WriteTask::WriteBlob(WriteBlob { digest, .. }) => digests.push(digest),
            WriteTask::WriteFile(WriteFile { digest, .. }) => digests.push(digest),
        }
    }

    let request = FindMissingBlobsRequest {
        instance_name: instance_name(),
        blob_digests: digests,
    };

    let resp = bt_client.find_missing_blobs(request).await;
    if resp.is_err() {
        println!("failed to find missing blobs {}", resp.unwrap_err());
        return vec![];
    }
    let resp = resp.unwrap();

    let mut missing_digests = HashSet::new();
    for d in &resp.get_ref().missing_blob_digests {
        missing_digests.insert(d.hash.clone());
    }

    println!(
        "find {}/{} missing digests",
        missing_digests.len(),
        pending.len()
    );

    pending
        .into_iter()
        .filter(|e| {
            let d = match e {
                WriteTask::WriteBlob(WriteBlob { digest, .. }) => digest,
                WriteTask::WriteFile(WriteFile { digest, .. }) => digest,
            };
            missing_digests.contains(&d.hash)
        })
        .collect()
}

/// The default max is 4MB
/// use 3MB to account for protocol overhead
const GRPC_MAX_MESSGE_SIZE: i64 = 3 * 1024 * 1024;

/// Limit the batch size to 2000 to avoid BatchUpdateBlob bug:
const MAX_PENDING_REQUIEST_COUNT: usize = 2000;

async fn receiver_loop(mut recv: mpsc::Receiver<WriteTask>) {
    // bytesteam client is used for streaming large objects
    let mut bs_client = create_bs_client().await.unwrap();

    // batch client is used for batching small objects
    let mut bt_client = create_cas_client().await.unwrap();

    let mut pending = vec![];
    let mut pending_size: i64 = 0;
    let mut ready = vec![];

    while let Some(task) = recv.recv().await {
        match task {
            WriteTask::WriteFile(w) => {
                // special case to fetch missing git lfs objects
                // ideally this should be hide underneath so that file read
                // could be treated transparently
                let file = File::open(w.path.clone()).await;
                if file.is_err() {
                    continue;
                }
                let mut file = file.unwrap();
                // only need the first 100 bytes to determine file type
                // round up to 256 bytes
                // https://github.com/git-lfs/git-lfs/blob/main/docs/spec.md
                let mut buff = [0; 256];
                let res = file.read(&mut buff).await;
                if res.is_err() {
                    println!("Failed to read the file {}", res.unwrap_err());
                    continue;
                }

                let mut path = w.path.clone();
                if let Ok(lfs_file) = LfsFile::new(&mut Cursor::new(buff)) {
                    let parent_dir = &w.path.parent().unwrap();
                    let git_root = get_git_root(&parent_dir).unwrap();
                    let obj_path = get_lfs_object_path(git_root.clone(), lfs_file.hash.clone());
                    if !obj_path.exists() {
                        println!("digest {:?} is missing", w.digest);
                        // git lfs fetch -I requires relative path
                        let rel_path = w.path.strip_prefix(git_root.clone());
                        if rel_path.is_err() {
                            println!(
                                "failed to strip prefix {:?} {:?} {:?}",
                                w.path,
                                git_root.clone(),
                                rel_path.unwrap_err()
                            );
                            continue;
                        }
                        let rel_path = rel_path.unwrap();
                        let res = git_lfs_fetch(&git_root, &rel_path);
                        if res.is_err() {
                            println!("failed to fetch lfs object {}", res.unwrap_err());
                            continue;
                        }
                    }
                    path = obj_path;
                }

                // stream the large file out directly
                if w.digest.size_bytes > GRPC_MAX_MESSGE_SIZE {
                    bs_write_file(&mut bs_client, &w.digest, path).await;
                } else {
                    // read small files into memory
                    let file = File::open(path).await;
                    if file.is_err() {
                        continue;
                    }
                    let mut file = file.unwrap();
                    let mut buff = vec![];
                    let res = file.read_to_end(&mut buff).await;
                    if res.is_err() {
                        println!("Failed to read the file {}", res.unwrap_err());
                    } else {
                        if pending_size + w.digest.size_bytes >= GRPC_MAX_MESSGE_SIZE
                            || pending.len() > MAX_PENDING_REQUIEST_COUNT
                        {
                            ready.append(&mut pending);
                            pending_size = w.digest.size_bytes;
                        } else {
                            pending_size += w.digest.size_bytes;
                        }
                        pending.push(WriteBlob {
                            digest: w.digest,
                            buff: buff,
                        });
                    }
                }
            }
            WriteTask::WriteBlob(w) => {
                // stream out the large blob directly
                if w.digest.size_bytes > GRPC_MAX_MESSGE_SIZE {
                    bs_write_blob(&mut bs_client, &w.digest, w.buff).await;
                } else {
                    if pending_size + w.digest.size_bytes >= GRPC_MAX_MESSGE_SIZE
                        || pending.len() > MAX_PENDING_REQUIEST_COUNT
                    {
                        ready.append(&mut pending);
                        pending_size = w.digest.size_bytes;
                    } else {
                        pending_size += w.digest.size_bytes;
                    }
                    pending.push(WriteBlob {
                        digest: w.digest,
                        buff: w.buff,
                    });
                }
            }
        }

        if !ready.is_empty() {
            //println!("batching {} requests", ready.len());
            let mut requests = vec![];
            for t in ready {
                requests.push(batch_update_blobs_request::Request {
                    digest: Some(t.digest),
                    data: t.buff,
                    compressor: 0,
                });
            }
            let request = BatchUpdateBlobsRequest {
                instance_name: instance_name(),
                requests: requests,
            };
            let resp = bt_client.batch_update_blobs(request).await;
            if resp.is_err() {
                println!("failed to batch upload {}", resp.unwrap_err());
            }
            ready = vec![];
        }
    }

    // batch send the final remaining blobs before receiver exits
    if !pending.is_empty() {
        //println!("final batch {} requests", pending.len());
        for t in pending {
            let mut requests = vec![];
            requests.push(batch_update_blobs_request::Request {
                digest: Some(t.digest),
                data: t.buff,
                compressor: 0,
            });
            let request = BatchUpdateBlobsRequest {
                instance_name: instance_name(),
                requests: requests,
            };
            let resp = bt_client.batch_update_blobs(request).await;
            if resp.is_err() {
                println!("failed to batch upload {}", resp.unwrap_err());
            }
        }
    }
    //println!("receiver done");

    // Once all senders have gone out of scope,
    // the `.recv()` call returns None and it will
    // exit from the while loop and shut down the
    // thread.
}

impl NonBlockingClient {
    pub fn new(send: mpsc::Sender<WriteTask>) -> Result<NonBlockingClient> {
        Ok(Self { sender: send })
    }

    pub fn write_blob(&self, digest: &Digest, buff: Vec<u8>) -> Result<()> {
        self.sender
            .blocking_send(WriteTask::WriteBlob(WriteBlob {
                digest: digest.clone(),
                buff: buff,
            }))
            .map_err(|e| anyhow::Error::msg(format!("failed to send {:?}", e)))
    }

    /// writes a large file into CAS
    pub fn write_file(&self, digest: &Digest, path: &Path) -> Result<()> {
        let path = path.to_path_buf().clone();

        self.sender
            .blocking_send(WriteTask::WriteFile(WriteFile {
                digest: digest.clone(),
                path,
            }))
            .map_err(|e| anyhow::Error::msg(format!("failed to send {:?}", e)))
    }
}

/// CacheClient provide a CAS client interface with caching
pub struct CacheClient {
    cas_client: BsClient,

    rt: Runtime,

    /// simple but unbounded in memory cache
    cache: HashMap<String, Vec<u8>>,
}

impl CacheClient {
    pub fn new(configs: Configs) -> Result<CacheClient> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let cas_client = rt.block_on(create_bs_client())?;

        Ok(CacheClient {
            cache: HashMap::new(),
            cas_client,
            rt,
        })
    }

    /// get_tree is NOT cached atm
    // pub fn get_tree(&mut self, hash: &str, size: i64) -> Result<Vec<Directory>> {
    //     self.inner.get_tree(hash, size)
    // }

    // read_blob returns the reference to the memeory of the blob
    // need to avoid memory copy since we need to make it performance for
    // large files
    pub fn read_blob(&mut self, hash: &str, size: i64) -> Result<&Vec<u8>> {
        // rust only allow put mutable reference before immutable reference
        // see rustc --explain E0502
        // mutable ref
        if let Entry::Vacant(e) = self.cache.entry(hash.to_string()) {
            let blob = self
                .rt
                .block_on(bs_read_blob(&mut self.cas_client, hash, size))?;
            e.insert(blob);
        };

        // immutable ref
        return Ok(self.cache.get(hash).unwrap());
    }

    pub fn get_dir(&mut self, hash: &str, size: i64) -> Result<Directory> {
        let dir_bytes = self.read_blob(hash, size)?;
        Directory::decode(&mut Cursor::new(dir_bytes)).map_err(|e| e.into())
    }
}

pub(crate) async fn create_channel() -> Result<Channel> {
    //TODO: better system ca cert handling
    //let ca_cert = tokio::fs::read("/etc/ssl/certs/ca-certificates.crt").await?;
    let cas_endpoint = env::var("CAS_ENDPOINT")?;
    let ca_cert_path = env::var("CA_CERT_PATH")?;
    let ca_cert = tokio::fs::read(ca_cert_path).await?;
    let ca_cert = Certificate::from_pem(ca_cert);
    let tls = ClientTlsConfig::new().ca_certificate(ca_cert);
    let channel = Channel::from_shared(cas_endpoint)?
        .tls_config(tls)?
        .connect()
        .await?;

    Ok(channel)
}

pub(crate) async fn create_bs_client() -> Result<BsClient> {
    let channel = create_channel().await?;
    let interceptor = AuthInterceptor::new()?;

    Ok(ByteStreamClient::with_interceptor(channel, interceptor))
}

pub(crate) async fn create_cas_client() -> Result<CasClient> {
    let channel = create_channel().await?;
    let interceptor = AuthInterceptor::new()?;

    Ok(ContentAddressableStorageClient::with_interceptor(
        channel,
        interceptor,
    ))
}

//TODO: handle pagination
pub(crate) async fn read_directories(
    client: &mut CasClient,
    hash: &str,
    size: i64,
) -> Result<Vec<Directory>> {
    println!("read_directories {} {}", hash, size);
    let request = GetTreeRequest {
        instance_name: instance_name(),
        root_digest: Some(Digest {
            hash: hash.to_string(),
            size_bytes: size,
        }),
        page_size: 16,
        page_token: String::from(""),
    };
    let mut resp = client.get_tree(request).await?;
    let stream = resp.get_mut();

    let message = stream.message().await?;
    Ok(message.map_or(vec![], |resp| resp.directories))
}

/// Read the small blobs in batch. Do not use!
/// This API is bugged:
pub(crate) async fn batch_read_blob(
    client: &mut CasClient,
    hash: &str,
    size: i64,
) -> Result<Vec<u8>> {
    println!("batch_read_blob {} {}", hash, size);
    let request = BatchReadBlobsRequest {
        instance_name: instance_name(),
        digests: vec![Digest {
            hash: hash.to_string(),
            size_bytes: size,
        }],
        acceptable_compressors: vec![],
    };

    let resp = client.batch_read_blobs(request).await?;
    let resp = resp.get_ref();

    Ok(resp.responses[0].data.clone())
}

pub(crate) async fn bs_read_blob(client: &mut BsClient, hash: &str, size: i64) -> Result<Vec<u8>> {
    //println!("bs_read_blob {} {}", hash, size);
    let instance_name = instance_name();
    let resource_name = format!("{}/blobs/{}/{}", instance_name, hash, size);
    let request = ReadRequest {
        resource_name: resource_name,
        read_offset: 0,
        read_limit: 0,
    };

    let mut resp = client.read(request).await?;
    let stream = resp.get_mut();

    let mut content = vec![];
    loop {
        match stream.message().await? {
            Some(mut message) => content.append(&mut message.data),
            None => break,
        }
    }
    Ok(content)
}

// resource_name includes digests this means the digest has to
// be calculated before uploading the blob this also means we
// cannot compute the hash and upload the blob at the same time
pub(crate) async fn bs_write_blob(
    client: &mut BsClient,
    digest: &Digest,
    buff: Vec<u8>,
) -> Result<()> {
    println!("write_blob: {:?}", digest);
    let stream = WriteRequestStream::new(Cursor::new(buff), digest);

    bs_write(client, stream).await
}

pub(crate) async fn bs_write_file(
    client: &mut BsClient,
    digest: &Digest,
    path: PathBuf,
) -> Result<()> {
    println!("write_file: {:?} path: {:?}", digest, path);
    let f = File::open(path).await?;
    let stream = WriteRequestStream::new(f, digest);

    bs_write(client, stream).await
}

async fn bs_write<T: AsyncRead + Send + Unpin + 'static>(
    client: &mut BsClient,
    stream: WriteRequestStream<T>,
) -> Result<()> {
    client
        .write(stream)
        .await
        .map(|_v| ())
        .map_err(|e| anyhow::Error::msg(format!("failed to write stream {}", e)))
}

struct WriteRequestStream<T: AsyncRead + Send + Unpin> {
    read: T,
    resource_name: String,
    offset: i64,
}

impl<T: AsyncRead + Send + Unpin> WriteRequestStream<T> {
    fn new(read: T, digest: &Digest) -> WriteRequestStream<T> {
        let instance_name = instance_name();
        let uuid = Uuid::new_v4();
        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            instance_name, uuid, digest.hash, digest.size_bytes
        );
        WriteRequestStream {
            read: read,
            resource_name: resource_name,
            offset: 0,
        }
    }
}

impl<T: AsyncRead + Send + Unpin> Stream for WriteRequestStream<T> {
    type Item = WriteRequest;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buff = [0; 16 * 1024];
        let mut read_buff = ReadBuf::new(&mut buff);
        match Pin::new(&mut self.read).poll_read(cx, &mut read_buff) {
            Poll::Ready(_) => {
                let buff = read_buff.filled();
                let size = buff.len() as i64;
                // println!(
                //     "{} size: {} offset: {}",
                //     self.resource_name.clone(),
                //     size,
                //     self.offset
                // );
                self.offset += size;
                if size == 0 {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(WriteRequest {
                        resource_name: self.resource_name.clone(),
                        write_offset: self.offset,
                        finish_write: size == 0,
                        data: buff.to_vec(),
                    }))
                }
            }
            //TODO: when this happen?
            // Poll::Ready(Err(e)) => {
            // println!("Failed to read stream {}", e);
            // Poll::Pending
            // }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// instance_name returns the instance name for the CAS client
fn instance_name() -> String {
    match env::var("INSTANCE_NAME") {
        Ok(v) => v,
        Err(_) => String::from(""),
    }
}
