#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadRequest {
    #[prost(string, tag = "1")]
    pub resource_name: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub read_offset: i64,
    #[prost(int64, tag = "3")]
    pub read_limit: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadResponse {
    #[prost(bytes = "vec", tag = "10")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(string, tag = "1")]
    pub resource_name: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub write_offset: i64,
    #[prost(bool, tag = "3")]
    pub finish_write: bool,
    #[prost(bytes = "vec", tag = "10")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteResponse {
    #[prost(int64, tag = "1")]
    pub committed_size: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryWriteStatusRequest {
    #[prost(string, tag = "1")]
    pub resource_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryWriteStatusResponse {
    #[prost(int64, tag = "1")]
    pub committed_size: i64,
    #[prost(bool, tag = "2")]
    pub complete: bool,
}
#[doc = r" Generated server implementations."]
pub mod byte_stream_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ByteStreamServer."]
    #[async_trait]
    pub trait ByteStream: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Read method."]
        type ReadStream: futures_core::Stream<Item = Result<super::ReadResponse, tonic::Status>>
            + Send
            + 'static;
        async fn read(
            &self,
            request: tonic::Request<super::ReadRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status>;
        async fn write(
            &self,
            request: tonic::Request<tonic::Streaming<super::WriteRequest>>,
        ) -> Result<tonic::Response<super::WriteResponse>, tonic::Status>;
        async fn query_write_status(
            &self,
            request: tonic::Request<super::QueryWriteStatusRequest>,
        ) -> Result<tonic::Response<super::QueryWriteStatusResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ByteStreamServer<T: ByteStream> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ByteStream> ByteStreamServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ByteStreamServer<T>
    where
        T: ByteStream,
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
                "/google.bytestream.ByteStream/Read" => {
                    #[allow(non_camel_case_types)]
                    struct ReadSvc<T: ByteStream>(pub Arc<T>);
                    impl<T: ByteStream> tonic::server::ServerStreamingService<super::ReadRequest> for ReadSvc<T> {
                        type Response = super::ReadResponse;
                        type ResponseStream = T::ReadStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).read(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReadSvc(inner);
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
                "/google.bytestream.ByteStream/Write" => {
                    #[allow(non_camel_case_types)]
                    struct WriteSvc<T: ByteStream>(pub Arc<T>);
                    impl<T: ByteStream> tonic::server::ClientStreamingService<super::WriteRequest> for WriteSvc<T> {
                        type Response = super::WriteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::WriteRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).write(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WriteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/google.bytestream.ByteStream/QueryWriteStatus" => {
                    #[allow(non_camel_case_types)]
                    struct QueryWriteStatusSvc<T: ByteStream>(pub Arc<T>);
                    impl<T: ByteStream> tonic::server::UnaryService<super::QueryWriteStatusRequest>
                        for QueryWriteStatusSvc<T>
                    {
                        type Response = super::QueryWriteStatusResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryWriteStatusRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).query_write_status(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = QueryWriteStatusSvc(inner);
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
    impl<T: ByteStream> Clone for ByteStreamServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ByteStream> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ByteStream> tonic::transport::NamedService for ByteStreamServer<T> {
        const NAME: &'static str = "google.bytestream.ByteStream";
    }
}
