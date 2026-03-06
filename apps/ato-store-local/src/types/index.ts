export type TrustLevel = "verified" | "unverified" | "signed";

export interface StoreMetadata {
  iconPath?: string;
  text?: string;
  iconUrl?: string;
}

export interface CapsuleTarget {
  label: string;
  runtime: string;
  port: number | null;
  env: Record<string, string>;
  requiredEnv: string[];
}

export interface Capsule {
  id: string;
  scopedId: string;
  name: string;
  publisher: string;
  iconKey: "globe" | "package" | "zap" | "box";
  description: string;
  longDescription?: string;
  type: "webapp" | "cli" | "service";
  version: string;
  size: string;
  osArch: string[];
  envHints: Record<string, string>;
  readme: string;
  readmeSource?: "artifact" | "none";
  rawToml?: string;
  manifest?: unknown;
  targets: CapsuleTarget[];
  defaultTarget?: string;
  detailLoaded?: boolean;
  localPath: string;
  appUrl: string | null;
  trustLevel: TrustLevel;
  storeMetadata?: StoreMetadata;
}

export interface Process {
  id: string;
  name: string;
  pid: number;
  capsuleId: string;
  scopedId: string;
  active: boolean;
  status: "running" | "stopped" | "unknown";
  startedAt: string;
  lastSeenAt: string;
  runtime: string;
  targetLabel?: string;
}

export type LogLevel = "INFO" | "WARN" | "ERROR" | "SIGTERM";

export interface ProcessLogLine {
  index: number;
  timestamp: string;
  level: LogLevel | string;
  message: string;
}

export type OsFilter = "all" | "macos" | "linux" | "windows";
export type CatalogViewMode = "list" | "grid";
