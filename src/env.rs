pub const ENV_NACELLE_PATH: &str = "CAPSULE_NACELLE_PATH";
pub const ENV_SIDECAR_PATH: &str = "CAPSULE_SIDECAR_PATH";
pub const ENV_DEV_MODE: &str = "CAPSULE_DEV_MODE";

pub fn read_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
