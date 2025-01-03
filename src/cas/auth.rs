use anyhow::Result;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use tonic::service::Interceptor;
use tonic::{metadata::MetadataValue, Request, Status};

#[derive(Clone)]
pub struct AuthInterceptor {
    token: String,
}

impl AuthInterceptor {
    pub fn new() -> Result<Self> {
        let home_dir = env::var("HOME")?;
        let token_path = format!("{}/.rbe-auth-token", home_dir);
        let mut token = String::new();
        let mut file = File::open(&token_path).map_err(|e| {
            anyhow::Error::msg(format!(
                "failed to open auth token {}: {}",
                token_path, e
            ))
        })?;
        file.read_to_string(&mut token)?;
        Ok(AuthInterceptor { token: token })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let bearer_token = format!("Bearer {}", self.token);
        let header_value = MetadataValue::from_str(&bearer_token)
            .map_err(|_e| Status::invalid_argument("auth token is invalid"))?;
        request
            .metadata_mut()
            .insert("authorization", header_value.clone());
        Ok(request)
    }
}
