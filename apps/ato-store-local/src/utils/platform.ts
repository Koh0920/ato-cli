export function detectPlatform(): string {
  const platform = navigator.platform.toLowerCase();
  const ua = navigator.userAgent.toLowerCase();
  const arch = ua.includes("arm") || ua.includes("aarch64") ? "arm64" : "x64";
  if (platform.includes("mac")) {
    return `darwin/${arch}`;
  }
  if (platform.includes("win")) {
    return `windows/${arch}`;
  }
  return `linux/${arch}`;
}

export function toOsFilterLabel(osArch: string): "macos" | "linux" | "windows" | "other" {
  if (osArch.startsWith("darwin/")) {
    return "macos";
  }
  if (osArch.startsWith("windows/")) {
    return "windows";
  }
  if (osArch.startsWith("linux/")) {
    return "linux";
  }
  return "other";
}
