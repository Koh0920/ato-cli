use tonic::Status;

use crate::error::{CapsuleError, Result};
use crate::tsnet::ipc::IpcTransport;
use crate::tsnet::proto::{StartRequest, StatusRequest, StopRequest, TsnetState as ProtoState};
use crate::tsnet::{TsnetConfig, TsnetEndpoint, TsnetHandle, TsnetState, TsnetStatus};

#[derive(Debug, Clone)]
pub struct TsnetClient {
    transport: IpcTransport,
}

impl TsnetClient {
    pub fn new(transport: IpcTransport) -> Self {
        Self { transport }
    }

    pub fn from_env() -> Result<Self> {
        Ok(Self::new(IpcTransport::from_env()?))
    }

    pub fn from_endpoint(endpoint: TsnetEndpoint) -> Result<Self> {
        Ok(Self::new(IpcTransport::from_endpoint(endpoint)?))
    }
}

#[async_trait::async_trait]
impl TsnetHandle for TsnetClient {
    async fn start(&self, config: TsnetConfig) -> Result<TsnetStatus> {
        let mut client = self.transport.connect_client().await?;
        let request = StartRequest {
            control_url: config.control_url,
            auth_key: config.auth_key,
            hostname: config.hostname,
            socks_port: config.socks_port as u32,
        };
        let response = client
            .start(request)
            .await
            .map_err(|err| map_status_error("start", err))?
            .into_inner();
        let message = if response.message.is_empty() {
            None
        } else {
            Some(response.message)
        };
        map_status(response.state, response.socks_port, message)
    }

    async fn stop(&self) -> Result<()> {
        let mut client = self.transport.connect_client().await?;
        client
            .stop(StopRequest { force: false })
            .await
            .map_err(|err| map_status_error("stop", err))?;
        Ok(())
    }

    async fn status(&self) -> Result<TsnetStatus> {
        let mut client = self.transport.connect_client().await?;
        let response = client
            .status(StatusRequest {})
            .await
            .map_err(|err| map_status_error("status", err))?
            .into_inner();
        let message = if response.last_error.is_empty() {
            None
        } else {
            Some(response.last_error)
        };
        map_status(response.state, response.socks_port, message)
    }
}

fn map_status(state: i32, socks_port: u32, message: Option<String>) -> Result<TsnetStatus> {
    let state = ProtoState::try_from(state).unwrap_or(ProtoState::Stopped);
    let mapped_state = match state {
        ProtoState::Stopped => TsnetState::Stopped,
        ProtoState::Starting => TsnetState::Starting,
        ProtoState::Running => TsnetState::Ready,
        ProtoState::Stopping => TsnetState::Stopped,
        ProtoState::Failed => TsnetState::Failed,
        ProtoState::Unspecified => TsnetState::Stopped,
    };
    let socks_port = if socks_port == 0 {
        None
    } else if socks_port <= u16::MAX as u32 {
        Some(socks_port as u16)
    } else {
        return Err(CapsuleError::SidecarResponse(format!(
            "invalid socks_port from sidecar: {}",
            socks_port
        )));
    };
    Ok(TsnetStatus {
        state: mapped_state,
        socks_port,
        message,
    })
}

fn map_status_error(action: &str, status: Status) -> CapsuleError {
    CapsuleError::SidecarRequest(action.to_string(), status.message().to_string())
}
