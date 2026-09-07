#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Digest {
    #[prost(string, tag = "1")]
    pub hash: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub size_bytes: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DigestFunction {}
/// Nested message and enum types in `DigestFunction`.
pub mod digest_function {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Value {
        Unknown = 0,
        Sha256 = 1,
        Sha1 = 2,
        Md5 = 3,
        Vso = 4,
        Sha384 = 5,
        Sha512 = 6,
        Murmur3 = 7,
        Sha256tree = 8,
        Blake3 = 9,
        Gitsha1 = 10,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Compressor {}
/// Nested message and enum types in `Compressor`.
pub mod compressor {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Value {
        Identity = 0,
        Zstd = 1,
        Deflate = 2,
        Brotli = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindMissingBlobsRequest {
    #[prost(string, tag = "1")]
    pub instance_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub blob_digests: ::prost::alloc::vec::Vec<Digest>,
    #[prost(enumeration = "digest_function::Value", tag = "3")]
    pub digest_function: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindMissingBlobsResponse {
    #[prost(message, repeated, tag = "2")]
    pub missing_blob_digests: ::prost::alloc::vec::Vec<Digest>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchUpdateBlobsRequest {
    #[prost(string, tag = "1")]
    pub instance_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub requests: ::prost::alloc::vec::Vec<batch_update_blobs_request::Request>,
    #[prost(enumeration = "digest_function::Value", tag = "5")]
    pub digest_function: i32,
}
/// Nested message and enum types in `BatchUpdateBlobsRequest`.
pub mod batch_update_blobs_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Request {
        #[prost(message, optional, tag = "1")]
        pub digest: ::core::option::Option<super::Digest>,
        #[prost(bytes = "vec", tag = "2")]
        pub data: ::prost::alloc::vec::Vec<u8>,
        #[prost(enumeration = "super::compressor::Value", tag = "3")]
        pub compressor: i32,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchUpdateBlobsResponse {
    #[prost(message, repeated, tag = "1")]
    pub responses: ::prost::alloc::vec::Vec<batch_update_blobs_response::Response>,
}
/// Nested message and enum types in `BatchUpdateBlobsResponse`.
pub mod batch_update_blobs_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Response {
        #[prost(message, optional, tag = "1")]
        pub digest: ::core::option::Option<super::Digest>,
        #[prost(message, optional, tag = "2")]
        pub status: ::core::option::Option<::bazel_remote_apis_rs::google::rpc::Status>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTreeRequest {
    #[prost(string, tag = "1")]
    pub instance_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub root_digest: ::core::option::Option<Digest>,
    #[prost(int32, tag = "3")]
    pub page_size: i32,
    #[prost(string, tag = "4")]
    pub page_token: ::prost::alloc::string::String,
    #[prost(enumeration = "digest_function::Value", tag = "5")]
    pub digest_function: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub digest: ::core::option::Option<Digest>,
    #[prost(bool, tag = "4")]
    pub is_executable: bool,
    #[prost(message, optional, tag = "6")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DirectoryNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub digest: ::core::option::Option<Digest>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SymlinkNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub target: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeProperty {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeProperties {
    #[prost(message, repeated, tag = "1")]
    pub properties: ::prost::alloc::vec::Vec<NodeProperty>,
    #[prost(message, optional, tag = "2")]
    pub mtime: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag = "3")]
    pub unix_mode: ::core::option::Option<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Directory {
    #[prost(message, repeated, tag = "1")]
    pub files: ::prost::alloc::vec::Vec<FileNode>,
    #[prost(message, repeated, tag = "2")]
    pub directories: ::prost::alloc::vec::Vec<DirectoryNode>,
    #[prost(message, repeated, tag = "3")]
    pub symlinks: ::prost::alloc::vec::Vec<SymlinkNode>,
    #[prost(message, optional, tag = "5")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTreeResponse {
    #[prost(message, repeated, tag = "1")]
    pub directories: ::prost::alloc::vec::Vec<Directory>,
    #[prost(string, tag = "2")]
    pub next_page_token: ::prost::alloc::string::String,
}
#[doc = r" Generated server implementations."]
pub mod content_addressable_storage_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ContentAddressableStorageServer."]
    #[async_trait]
    pub trait ContentAddressableStorage: Send + Sync + 'static {
        async fn find_missing_blobs(
            &self,
            request: tonic::Request<super::FindMissingBlobsRequest>,
        ) -> Result<tonic::Response<super::FindMissingBlobsResponse>, tonic::Status>;
        async fn batch_update_blobs(
            &self,
            request: tonic::Request<super::BatchUpdateBlobsRequest>,
        ) -> Result<tonic::Response<super::BatchUpdateBlobsResponse>, tonic::Status>;
        #[doc = "Server streaming response type for the GetTree method."]
        type GetTreeStream: futures_core::Stream<Item = Result<super::GetTreeResponse, tonic::Status>>
            + Send
            + 'static;
        async fn get_tree(
            &self,
            request: tonic::Request<super::GetTreeRequest>,
        ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ContentAddressableStorageServer<T: ContentAddressableStorage> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ContentAddressableStorage> ContentAddressableStorageServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ContentAddressableStorageServer<T>
    where
        T: ContentAddressableStorage,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs" => {
                    #[allow(non_camel_case_types)]
                    struct FindMissingBlobsSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<T: ContentAddressableStorage>
                        tonic::server::UnaryService<super::FindMissingBlobsRequest>
                        for FindMissingBlobsSvc<T>
                    {
                        type Response = super::FindMissingBlobsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FindMissingBlobsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).find_missing_blobs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FindMissingBlobsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs" => {
                    #[allow(non_camel_case_types)]
                    struct BatchUpdateBlobsSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<T: ContentAddressableStorage>
                        tonic::server::UnaryService<super::BatchUpdateBlobsRequest>
                        for BatchUpdateBlobsSvc<T>
                    {
                        type Response = super::BatchUpdateBlobsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BatchUpdateBlobsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).batch_update_blobs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BatchUpdateBlobsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree" => {
                    #[allow(non_camel_case_types)]
                    struct GetTreeSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<T: ContentAddressableStorage>
                        tonic::server::ServerStreamingService<super::GetTreeRequest>
                        for GetTreeSvc<T>
                    {
                        type Response = super::GetTreeResponse;
                        type ResponseStream = T::GetTreeStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetTreeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_tree(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetTreeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ContentAddressableStorage> Clone for ContentAddressableStorageServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ContentAddressableStorage> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ContentAddressableStorage> tonic::transport::NamedService
        for ContentAddressableStorageServer<T>
    {
        const NAME: &'static str = "build.bazel.remote.execution.v2.ContentAddressableStorage";
    }
}
