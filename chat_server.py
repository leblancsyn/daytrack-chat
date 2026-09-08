#!/usr/bin/env python3
import json
import os
import queue
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

PORT = int(os.environ.get("PORT", os.environ.get("DAYTRACK_PORT", "8123")))
HISTORY_LIMIT = 200
MESSAGE_MAX = 1000
NICK_MAX = 24
# Render-compatible host (0.0.0.0 works locally too)
HOST = os.environ.get("HOST", "0.0.0.0")
# Allow the app to call the chat server cross-origin
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.environ.get(
        "DAYTRACK_ORIGINS",
        "http://localhost:8123,http://192.168.11.92:8123",
    ).split(",")
    if o.strip()
]

mutex = threading.Lock()
clients = set()
messages = []
msg_counter = 0


def now_ts():
    return datetime.now().strftime("%H:%M")


def encode(msg):
    return ("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n").encode("utf-8")


def add_message(nick, text):
    global msg_counter
    msg = {"id": msg_counter, "nick": nick, "text": text, "ts": now_ts()}
    msg_counter += 1
    payload = encode(msg)
    with mutex:
        messages.append(msg)
        if len(messages) > HISTORY_LIMIT:
            del messages[: len(messages) - HISTORY_LIMIT]
        dead = []
        for c in clients:
            try:
                c.put_nowait(payload)
            except queue.Full:
                dead.append(c)
        for c in dead:
            clients.discard(c)
    return msg


def cors_origin(handler):
    origin = handler.headers.get("Origin")
    if not origin:
        return None
    if origin.rstrip("/") in ALLOWED_ORIGINS:
        return origin
    if "*" in ALLOWED_ORIGINS:
        return origin
    return None


def send_json(handler, obj, code=200):
    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    origin = cors_origin(handler)
    if origin:
        handler.send_header("Access-Control-Allow-Origin", origin)
    if handler.command == "OPTIONS":
        handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        handler.send_header(
            "Access-Control-Allow-Headers", "Content-Type"
        )
    handler.end_headers()
    handler.wfile.write(payload)


class Handler(BaseHTTPRequestHandler):
    server_version = "DayTrackServer/1.0"

    def do_OPTIONS(self):
        send_json(self, {"ok": True, "cors": True})

    def do_GET(self):
        # API routes always win
        base = urlparse(self.path).path
        if base == "/api/chat/stream":
            self.handle_stream()
            return
        if base == "/api/chat/history":
            self.handle_history()
            return
        if base.startswith("/api/"):
            send_json(self, {"error": "Not Found"}, 404)
            return
        # Only serve local static files when not deployed to the cloud
        if os.environ.get("RENDER") != "1":
            if self.serve_static(base):
                return
        send_json(self, {"error": "Not Found"}, 404)

    def do_POST(self):
        if urlparse(self.path).path == "/api/chat/send":
            self.handle_send()
            return
        send_json(self, {"error": "Not Found"}, 404)

    CONTENT = {
        ".html": "text/html; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".js": "application/javascript; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".png": "image/png",
        ".svg": "image/svg+xml",
        ".webmanifest": "application/manifest+json",
    }

    def serve_static(self, path):
        import inspect
        base_dir = os.path.dirname(os.path.abspath(inspect.getfile(Handler)))
        if path in ("", "/"):
            path = "/index.html"
        rel = path.lstrip("/")
        full = os.path.normpath(os.path.join(base_dir, rel))
        # Guard against path traversal
        if not full.startswith(os.path.normpath(base_dir)):
            return False
        if not os.path.isfile(full):
            return False
        ext = os.path.splitext(full)[1].lower()
        ctype = self.CONTENT.get(ext, "application/octet-stream")
        try:
            with open(full, "rb") as f:
                body = f.read()
        except OSError:
            return False
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        origin = cors_origin(self)
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.end_headers()
        self.wfile.write(body)
        return True

    def handle_history(self):
        with mutex:
            send_json(self, {"messages": list(messages)})

    def handle_send(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            send_json(self, {"error": "Bad request"}, 400)
            return
        nick = str(body.get("nick", "")).strip()[:NICK_MAX]
        text = str(body.get("text", "")).strip()[:MESSAGE_MAX]
        if not nick or not text:
            send_json(self, {"error": "Missing nickname or message"}, 400)
            return
        msg = add_message(nick, text)
        send_json(self, {"ok": True, "message": msg})

    def handle_stream(self):
        self.send_response(200)
        origin = cors_origin(self)
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-store")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        q = queue.Queue(maxsize=200)
        with mutex:
            clients.add(q)
            replay = list(messages)
        try:
            for m in replay:
                self.wfile.write(encode(m))
                self.wfile.flush()
            while True:
                try:
                    self.wfile.write(q.get(timeout=15))
                    self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            with mutex:
                clients.discard(q)

    def log_message(self, fmt, *args):
        pass


add_message("DayTrack", "Chat is live. Say hi to everyone!")

if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"DayTrack server running on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass