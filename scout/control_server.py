"""
Massive Brain control server — lets the browser trigger the scout and read JSON.
Run: python3 control_server.py
Endpoints: GET /status, GET /scout-data, POST /run-scout
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUNNER = os.path.join(BASE_DIR, "run_morning.sh")
OPPS = os.path.join(BASE_DIR, "opportunities.json")
TODAY = os.path.join(BASE_DIR, "today.json")

HOST = "127.0.0.1"
PORT = 8766


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def do_OPTIONS(self):
        self._send(200, {"ok": True})

    def do_GET(self):
        if self.path == "/status":
            self._send(200, {"ok": True, "service": "scout-brain-control"})
            return

        if self.path == "/scout-data":
            today = load_json(TODAY, {
                "generated_at": None,
                "summary": "No scout run yet.",
                "top_opportunities": []
            })
            opps = load_json(OPPS, [])
            self._send(200, {
                "today": today,
                "opportunities": opps
            })
            return

        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/run-scout":
            if not os.path.exists(RUNNER):
                self._send(500, {"error": "run_morning.sh not found"})
                return

            try:
                proc = subprocess.run(
                    [RUNNER],
                    cwd=BASE_DIR,
                    capture_output=True,
                    text=True,
                    timeout=240
                )
                today = load_json(TODAY, {
                    "generated_at": None,
                    "summary": "No scout run yet.",
                    "top_opportunities": []
                })
                opps = load_json(OPPS, [])

                self._send(200, {
                    "ok": proc.returncode == 0,
                    "stdout": proc.stdout[-4000:] if proc.stdout else "",
                    "stderr": proc.stderr[-4000:] if proc.stderr else "",
                    "today": today,
                    "opportunities": opps
                })
                return
            except subprocess.TimeoutExpired:
                self._send(500, {"error": "Scout run timed out"})
                return
            except Exception as e:
                self._send(500, {"error": str(e)})
                return

        self._send(404, {"error": "not found"})


if __name__ == "__main__":
    print(f"Massive Brain control server running on http://{HOST}:{PORT}")
    HTTPServer((HOST, PORT), Handler).serve_forever()
