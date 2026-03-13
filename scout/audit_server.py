"""
Massive Brain — website audit server (scout).

Run from scout folder:
  cd massive-brain/scout && python3 audit_server.py

Used by the Dashboard "Analyze + Draft" flow: the app POSTs a URL here and gets
back facts, problems, and pitch angles for the email generator.

Endpoints:
  POST /audit  — body: { "url": "https://example.com" }  → { url, facts, problems, pitch }
  OPTIONS /audit — CORS preflight
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import re
import urllib.request
import urllib.error

HOST = "127.0.0.1"
PORT = 8765


def analyze_html(url: str, html: str):
    lower = html.lower()

    problems = []
    pitch = []
    facts = []

    # Platform clues
    if "weebly" in lower or "editmysite" in lower:
        facts.append("Detected older Weebly/EditMySite platform")
        problems.append("Site appears to use an older website platform/template")
        pitch.append("move to a cleaner custom layout with a more modern first impression")

    if "wix" in lower:
        facts.append("Detected Wix-related markup")
        problems.append("Site may rely on a generic builder layout")
        pitch.append("simplify the layout and tighten the mobile experience")

    if "squarespace" in lower:
        facts.append("Detected Squarespace-related markup")

    # Basic head/meta checks
    if 'name="viewport"' not in lower:
        facts.append("No viewport meta tag found")
        problems.append("Mobile layout may not be properly optimized")

    title_match = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
    if title_match:
        title = re.sub(r"\s+", " ", title_match.group(1)).strip()
        facts.append(f"Title tag: {title[:120]}")
    else:
        problems.append("No clear title tag found")

    meta_desc = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html, re.I
    )
    if meta_desc:
        facts.append("Meta description found")
    else:
        problems.append("No obvious meta description found")
        pitch.append("improve search appearance with stronger SEO basics")

    # Restaurant / menu clues
    menu_words = ["menu", "special", "breakfast", "lunch", "dinner", "carry out", "carryout"]
    menu_hits = sum(1 for w in menu_words if w in lower)
    if menu_hits >= 2:
        facts.append("Restaurant/menu content detected")

    # Phone / contact / directions clues
    if "tel:" in lower:
        facts.append("Tap-to-call link found")
    else:
        problems.append("No obvious tap-to-call phone link found")
        pitch.append("make phone contact easier for mobile visitors")

    if "google maps" in lower or "maps.google" in lower or "map" in lower:
        facts.append("Map/location references detected")
    else:
        pitch.append("make directions and location easier to find")

    # Heuristic clutter clues
    h2_count = len(re.findall(r"<h2\b", lower))
    br_count = lower.count("<br")
    if h2_count > 8 or br_count > 25:
        problems.append("Page may be text-heavy and harder to scan quickly on phones")
        pitch.append("reduce reading load and improve visual hierarchy")

    # Restaurant-specific help
    if menu_hits >= 2:
        pitch.append("make menu, hours, specials, and location easier to scan on mobile")

    if not problems:
        problems = [
            "Site could likely be made clearer and more conversion-focused",
            "Important actions may not stand out enough on phones",
        ]

    if not pitch:
        pitch = [
            "create a cleaner mobile-friendly layout",
            "make the main actions easier for visitors",
            "improve the overall first impression",
        ]

    return {
        "url": url,
        "facts": facts[:8],
        "problems": problems[:6],
        "pitch": pitch[:6],
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        if self.path != "/audit":
            self._send(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send(400, {"error": "invalid json"})
            return

        url = (data.get("url") or "").strip()
        if not url:
            self._send(400, {"error": "missing url"})
            return

        if not url.startswith("http://") and not url.startswith("https://"):
            url = "https://" + url

        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 MassiveBrainAuditor/0.8"},
            )
            with urllib.request.urlopen(req, timeout=12) as resp:
                content_type = resp.headers.get("Content-Type", "")
                body = resp.read(400000)
                html = body.decode("utf-8", errors="ignore")

            result = analyze_html(url, html)
            result["content_type"] = content_type
            self._send(200, result)

        except urllib.error.HTTPError as e:
            self._send(200, {
                "url": url,
                "facts": [f"HTTP error: {e.code}"],
                "problems": ["Website could not be fully fetched"],
                "pitch": ["still worth reviewing manually if this is a real lead"],
            })
        except Exception as e:
            self._send(200, {
                "url": url,
                "facts": [f"Fetch issue: {str(e)}"],
                "problems": ["Website could not be fully fetched"],
                "pitch": ["review manually and use a simpler outreach angle"],
            })


if __name__ == "__main__":
    print(f"Massive Brain audit server running on http://{HOST}:{PORT}")
    print("Used by Dashboard → Analyze + Draft (POST /audit with url)")
    HTTPServer((HOST, PORT), Handler).serve_forever()
