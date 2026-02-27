const [rootArg, portArg, ...rest] = Deno.args;
if (!rootArg || !portArg) {
  console.error("usage: static_file_server.ts <root> <port> [--host <host>]");
  Deno.exit(2);
}

const root = await Deno.realPath(rootArg);
const port = Number.parseInt(portArg, 10);
if (!Number.isFinite(port) || port <= 0 || port > 65535) {
  console.error(`invalid port: ${portArg}`);
  Deno.exit(2);
}

let host = "127.0.0.1";
for (let i = 0; i < rest.length; i++) {
  if (rest[i] === "--host" && rest[i + 1]) {
    host = rest[i + 1];
    i += 1;
  }
}

const mimeByExt: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".txt": "text/plain; charset=utf-8",
};

const safeJoin = async (pathname: string): Promise<string | null> => {
  const decoded = decodeURIComponent(pathname);
  const normalized = decoded === "/" ? "/index.html" : decoded;
  const rel = normalized.replace(/^\/+/, "");
  const candidate = await Deno.realPath(`${root}/${rel}`).catch(() => null);
  if (!candidate) return null;
  if (!candidate.startsWith(root)) return null;
  return candidate;
};

const contentType = (path: string): string => {
  const idx = path.lastIndexOf(".");
  if (idx === -1) return "application/octet-stream";
  return mimeByExt[path.slice(idx).toLowerCase()] ?? "application/octet-stream";
};

Deno.serve({ hostname: host, port }, async (request) => {
  const url = new URL(request.url);
  const resolved = await safeJoin(url.pathname);
  if (!resolved) {
    return new Response("Not found", { status: 404 });
  }
  let filePath = resolved;
  const stat = await Deno.stat(filePath).catch(() => null);
  if (!stat) return new Response("Not found", { status: 404 });
  if (stat.isDirectory) {
    const indexPath = `${filePath}/index.html`;
    const indexStat = await Deno.stat(indexPath).catch(() => null);
    if (!indexStat || !indexStat.isFile) {
      return new Response("Not found", { status: 404 });
    }
    filePath = indexPath;
  }

  const file = await Deno.readFile(filePath).catch(() => null);
  if (!file) return new Response("Not found", { status: 404 });
  return new Response(file, {
    status: 200,
    headers: { "content-type": contentType(filePath) },
  });
});
