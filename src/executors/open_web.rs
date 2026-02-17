use anyhow::{Context, Result};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};

use capsule_core::router::ManifestData;
use capsule_core::CapsuleReporter;

use crate::reporters::CliReporter;

pub fn execute(plan: &ManifestData, reporter: std::sync::Arc<CliReporter>) -> Result<()> {
    let entrypoint = plan
        .execution_entrypoint()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("runtime=web target requires entrypoint"))?;
    let public = plan.targets_web_public();
    if public.is_empty() {
        anyhow::bail!(
            "runtime=web target '{}' requires non-empty public allowlist",
            plan.selected_target_label()
        );
    }
    if !is_public_allowed(&entrypoint, &public) {
        anyhow::bail!(
            "runtime=web target '{}' requires entrypoint '{}' to be included in public allowlist",
            plan.selected_target_label(),
            entrypoint
        );
    }

    let listener = TcpListener::bind("127.0.0.1:0").context("Failed to bind local web server")?;
    let addr = listener
        .local_addr()
        .context("Failed to resolve local web server address")?;

    let entry_uri = normalize_web_path(&entrypoint);
    let url = format!("http://127.0.0.1:{}{}", addr.port(), entry_uri);
    futures::executor::block_on(reporter.notify(format!(
        "🌐 Opening web target '{}' at {}",
        plan.selected_target_label(),
        url
    )))?;

    let _ = try_open_browser(&url);

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        let _ = serve_request(
            &mut stream,
            &plan.manifest_dir,
            &entrypoint,
            &public,
            plan.selected_target_label(),
        );
    }

    Ok(())
}

fn serve_request(
    stream: &mut TcpStream,
    root: &Path,
    entrypoint: &str,
    public: &[String],
    target_label: &str,
) -> Result<()> {
    let mut first_line = String::new();
    {
        let mut reader = BufReader::new(stream.try_clone()?);
        reader.read_line(&mut first_line)?;
        // Drain headers.
        loop {
            let mut header = String::new();
            let n = reader.read_line(&mut header)?;
            if n == 0 || header == "\r\n" {
                break;
            }
        }
    }

    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let raw_path = parts.next().unwrap_or("/");
    if method != "GET" && method != "HEAD" {
        write_response(stream, 405, "text/plain", b"Method Not Allowed")?;
        return Ok(());
    }

    let path = if raw_path == "/" {
        normalize_web_path(entrypoint)
    } else {
        raw_path.split('?').next().unwrap_or("/").to_string()
    };

    let cleaned = path.trim_start_matches('/');
    if !is_safe_relative_path(cleaned) {
        write_response(stream, 400, "text/plain", b"Bad Request")?;
        return Ok(());
    }

    if !is_public_allowed(cleaned, public) {
        let body = format!(
            "Forbidden: '{}' is not public for target {}",
            cleaned, target_label
        );
        write_response(stream, 403, "text/plain", body.as_bytes())?;
        return Ok(());
    }

    let file_path = root.join(cleaned);
    if !file_path.exists() || !file_path.is_file() {
        write_response(stream, 404, "text/plain", b"Not Found")?;
        return Ok(());
    }

    let mut body = Vec::new();
    fs::File::open(&file_path)?.read_to_end(&mut body)?;
    let content_type = mime_type_for(&file_path);
    write_response(stream, 200, content_type, &body)?;
    Ok(())
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        content_type,
        body.len()
    );
    stream.write_all(headers.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn is_safe_relative_path(path: &str) -> bool {
    let p = Path::new(path);
    !p.components().any(|c| {
        matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    })
}

fn normalize_web_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{}", trimmed)
    }
}

fn is_public_allowed(path: &str, public: &[String]) -> bool {
    public.iter().any(|rule| {
        let rule = rule.trim().trim_start_matches('/');
        if rule.is_empty() {
            return false;
        }
        if rule.ends_with("/**") {
            let prefix = rule.trim_end_matches("/**").trim_end_matches('/');
            path == prefix || path.starts_with(&format!("{}/", prefix))
        } else {
            path == rule
        }
    })
}

fn mime_type_for(path: &PathBuf) -> &'static str {
    match path.extension().and_then(|s| s.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}

fn try_open_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(url).spawn();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn();
    }
    Ok(())
}
