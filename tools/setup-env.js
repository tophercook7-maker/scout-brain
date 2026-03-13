import fs from "fs";
import http from "http";
import { exec } from "child_process";
import { platform } from "os";

const PORT = 3847;

const FORM_HTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Massive Brain — Supabase Setup</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; background: #1a1a1a; color: #e5e5e5; margin: 0; padding: 24px; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
    .card { background: #252525; border-radius: 12px; padding: 32px; max-width: 440px; width: 100%; border: 1px solid #333; }
    h1 { margin: 0 0 8px; font-size: 1.25rem; }
    p { margin: 0 0 24px; color: #999; font-size: 0.9rem; }
    label { display: block; font-size: 0.85rem; margin-bottom: 6px; color: #aaa; }
    input { width: 100%; padding: 12px; border-radius: 8px; border: 1px solid #444; background: #1a1a1a; color: #fff; font-size: 0.95rem; margin-bottom: 16px; }
    input:focus { outline: none; border-color: #3b82f6; }
    button { width: 100%; padding: 12px; border-radius: 8px; border: none; background: #3b82f6; color: white; font-size: 1rem; cursor: pointer; font-weight: 500; }
    button:hover { background: #2563eb; }
    .success { color: #4ade80; margin-top: 16px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Massive Brain — Supabase Setup</h1>
    <p>Enter your Supabase project URL and Anon Key.</p>
    <form method="POST" action="/">
      <label for="url">Supabase URL</label>
      <input type="url" id="url" name="url" placeholder="https://xxxxx.supabase.co" required>
      <label for="key">Anon Key</label>
      <input type="password" id="key" name="key" placeholder="eyJhbGciOiJIUzI1NiIs..." required>
      <button type="submit">Save to .env</button>
    </form>
    <p id="result"></p>
  </div>
</body>
</html>
`;

const SUCCESS_HTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Setup complete</title>
  <style>
    body { font-family: system-ui; background: #1a1a1a; color: #e5e5e5; margin: 0; padding: 24px; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
    .card { background: #252525; border-radius: 12px; padding: 32px; max-width: 360px; border: 1px solid #333; text-align: center; }
    h1 { color: #4ade80; margin: 0 0 8px; }
    p { color: #999; margin: 0; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Done</h1>
    <p>.env created. Restart the dev server.</p>
  </div>
</body>
</html>
`;

function parseFormBody(body) {
  return Object.fromEntries(new URLSearchParams(body));
}

function openBrowser(url) {
  const cmd = platform() === "darwin" ? "open" : platform() === "win32" ? "start" : "xdg-open";
  exec(`${cmd} "${url}"`);
}

async function runWithDialog() {
  const [urlArg, keyArg] = process.argv.slice(2);
  if (urlArg?.trim() && keyArg?.trim()) {
    return { url: urlArg.trim(), key: keyArg.trim() };
  }

  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      if (req.method === "GET") {
        res.writeHead(200, { "Content-Type": "text/html" });
        res.end(FORM_HTML);
        return;
      }
      if (req.method === "POST" && req.url === "/") {
        let body = "";
        req.on("data", (chunk) => (body += chunk));
        req.on("end", () => {
          const { url, key } = parseFormBody(body);
          res.writeHead(200, { "Content-Type": "text/html" });
          res.end(SUCCESS_HTML);
          server.close();
          resolve({ url: url?.trim(), key: key?.trim() });
        });
        return;
      }
      res.writeHead(404);
      res.end();
    });

    server.listen(PORT, "127.0.0.1", () => {
      const addr = `http://127.0.0.1:${PORT}`;
      console.log("\nOpening setup in browser…\n");
      openBrowser(addr);
    });
  });
}

async function runSetup() {
  const { url, key } = await runWithDialog();

  if (!url || !key) {
    console.error("URL and Anon Key are required.");
    process.exit(1);
  }

  const envContent = `VITE_SUPABASE_URL=${url}
VITE_SUPABASE_ANON_KEY=${key}
`;

  const envPath = `${process.cwd()}/.env`;
  fs.writeFileSync(envPath, envContent);

  console.log(".env file created successfully.");
  console.log("Restart the dev server after this.\n");
}

runSetup();
