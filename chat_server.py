#!/usr/bin/env python3
import json
import os
import queue
import secrets
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("PORT", os.environ.get("DAYTRACK_PORT", "8123")))
HISTORY_LIMIT = 200
MESSAGE_MAX = 1000
NICK_MAX = 24
DM_ROOMS_LIMIT = 300
STORY_TTL = 24 * 60 * 60            # seconds
STORY_TEXT_MAX = 500
STORY_IMAGE_MAX = 700000            # base64 chars (~500 KB)
STORIES_LIMIT = 200
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
dm_rooms = {}     # room key -> {"msgs": [...], "counter": int}
dm_clients = {}   # room key -> set of queues
stories = []
story_counter = 0


def now_ts():
    return datetime.now().strftime("%H:%M")


def encode(msg):
    return ("data: " + json.dumps(msg, ensure_ascii=False) + "\n\n").encode("utf-8")


def public_msg(m):
    out = {"id": m["id"], "nick": m["nick"], "text": m["text"], "ts": m["ts"]}
    if m.get("room"):
        out["room"] = m["room"]
    return out


def dm_room_key(a, b):
    return "dm:" + "|".join(sorted([a, b]))


def broadcast_global(payload):
    dead = []
    for c in clients:
        try:
            c.put_nowait(payload)
        except queue.Full:
            dead.append(c)
    for c in dead:
        clients.discard(c)


def broadcast_dm(room, payload):
    qs = dm_clients.get(room)
    if not qs:
        return
    dead = []
    for c in qs:
        try:
            c.put_nowait(payload)
        except queue.Full:
            dead.append(c)
    for c in dead:
        dm_clients[room].discard(c)


def add_message(nick, text):
    global msg_counter
    msg = {
        "id": msg_counter,
        "nick": nick,
        "text": text,
        "ts": now_ts(),
        "token": secrets.token_urlsafe(12),
    }
    msg_counter += 1
    payload = encode(public_msg(msg))
    with mutex:
        messages.append(msg)
        if len(messages) > HISTORY_LIMIT:
            del messages[: len(messages) - HISTORY_LIMIT]
        broadcast_global(payload)
    return msg


def evict_dm_room_if_full():
    if len(dm_rooms) < DM_ROOMS_LIMIT:
        return
    key = next(iter(dm_rooms))
    dm_rooms.pop(key, None)
    dm_clients.pop(key, None)


def add_dm_message(room, nick, text):
    evict_dm_room_if_full()
    r = dm_rooms.get(room)
    if r is None:
        r = {"msgs": [], "counter": 0}
        dm_rooms[room] = r
    msg = {
        "id": r["counter"],
        "nick": nick,
        "text": text,
        "ts": now_ts(),
        "room": room,
        "token": secrets.token_urlsafe(12),
    }
    r["counter"] += 1
    r["msgs"].append(msg)
    if len(r["msgs"]) > HISTORY_LIMIT:
        del r["msgs"][0]
    broadcast_dm(room, encode(public_msg(msg)))
    return msg


def unsend_message(room, msg_id, token):
    if room:
        r = dm_rooms.get(room)
        if not r:
            return False
        for i, m in enumerate(r["msgs"]):
            if m["id"] == msg_id and m.get("token") == token:
                del r["msgs"][i]
                broadcast_dm(room, encode({"action": "delete", "rid": msg_id, "room": room}))
                return True
        return False
    for i, m in enumerate(messages):
        if m["id"] == msg_id and m.get("token") == token:
            del messages[i]
            broadcast_global(encode({"action": "delete", "rid": msg_id}))
            return True
    return False


def prune_stories():
    global stories
    cutoff = time.time() - STORY_TTL
    stories = [s for s in stories if s["ts"] > cutoff]
    if len(stories) > STORIES_LIMIT:
        stories = stories[len(stories) - STORIES_LIMIT:]


def add_story(nick, text, image):
    global story_counter
    with mutex:
        prune_stories()
        story = {
            "id": story_counter,
            "nick": nick,
            "text": text,
            "image": image,
            "ts": int(time.time() * 1000),
            "token": secrets.token_urlsafe(12),
        }
        story_counter += 1
        stories.append(story)
    return story


def public_story(s):
    return {"id": s["id"], "nick": s["nick"], "text": s["text"], "image": s["image"], "ts": s["ts"]}


def delete_story(story_id, token):
    with mutex:
        prune_stories()
        for i, s in enumerate(stories):
            if s["id"] == story_id and s["token"] == token:
                del stories[i]
                return True
    return False


def prune_loop():
    while True:
        time.sleep(300)
        with mutex:
            prune_stories()


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
        handler.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
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
        parsed = urlparse(self.path)
        base = parsed.path
        query = parse_qs(parsed.query)
        if base == "/api/chat/stream":
            room = (query.get("dm") or [""])[0]
            if room:
                self.handle_dm_stream(room)
            else:
                self.handle_stream()
            return
        if base == "/api/chat/history":
            room = (query.get("dm") or [""])[0]
            if room:
                self.handle_dm_history(room)
            else:
                self.handle_history()
            return
        if base == "/api/chat/nicks":
            self.handle_nicks()
            return
        if base == "/api/stories":
            self.handle_stories_get()
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
        path = urlparse(self.path).path
        if path == "/api/chat/send":
            self.handle_send()
            return
        if path == "/api/chat/unsend":
            self.handle_unsend()
            return
        if path == "/api/stories":
            self.handle_stories_post()
            return
        send_json(self, {"error": "Not Found"}, 404)

    def do_DELETE(self):
        if urlparse(self.path).path == "/api/stories":
            params = parse_qs(urlparse(self.path).query)
            try:
                story_id = int(params.get("id", ["-1"])[0])
            except ValueError:
                story_id = -1
            token = params.get("token", [""])[0]
            if delete_story(story_id, token):
                send_json(self, {"ok": True, "deleted": story_id})
            else:
                send_json(self, {"ok": False, "error": "Not found or not yours"}, 404)
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
            send_json(self, {"messages": [public_msg(m) for m in messages]})

    def handle_dm_history(self, room):
        with mutex:
            r = dm_rooms.get(room)
            out = [public_msg(m) for m in r["msgs"]] if r else []
        send_json(self, {"messages": out})

    def handle_nicks(self):
        with mutex:
            nicks = {m["nick"] for m in messages}
            for r in dm_rooms.values():
                for m in r["msgs"]:
                    nicks.add(m["nick"])
        send_json(self, {"nicks": sorted(nicks)})

    def handle_send(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            send_json(self, {"error": "Bad request"}, 400)
            return
        nick = str(body.get("nick", "")).strip()[:NICK_MAX]
        text = str(body.get("text", "")).strip()[:MESSAGE_MAX]
        target = str(body.get("to", "") or "").strip()[:NICK_MAX]
        if not nick or not text:
            send_json(self, {"error": "Missing nickname or message"}, 400)
            return
        if target and target != nick:
            room = dm_room_key(nick, target)
            with mutex:
                msg = add_dm_message(room, nick, text)
            send_json(self, {"ok": True, "message": public_msg(msg), "token": msg["token"], "room": room})
            return
        msg = add_message(nick, text)
        send_json(self, {"ok": True, "message": public_msg(msg), "token": msg["token"]})

    def handle_unsend(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            send_json(self, {"error": "Bad request"}, 400)
            return
        try:
            msg_id = int(body.get("id"))
        except (TypeError, ValueError):
            send_json(self, {"error": "Bad id"}, 400)
            return
        token = str(body.get("token", ""))
        room = str(body.get("room", "") or "")
        if not token:
            send_json(self, {"error": "Missing token"}, 400)
            return
        if unsend_message(room, msg_id, token):
            send_json(self, {"ok": True, "deleted": msg_id})
        else:
            send_json(self, {"ok": False, "error": "Not found or not yours"}, 404)

    def handle_stories_get(self):
        with mutex:
            prune_stories()
            out = [public_story(s) for s in reversed(stories)]
        send_json(self, {"stories": out})

    def handle_stories_post(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            send_json(self, {"error": "Bad request"}, 400)
            return
        nick = str(body.get("nick", "")).strip()[:NICK_MAX]
        text = str(body.get("text", "")).strip()[:STORY_TEXT_MAX]
        image = body.get("image") or None
        if image is not None and (
            not isinstance(image, str) or not image.startswith("data:image/")
        ):
            image = None
        if not nick:
            send_json(self, {"error": "Missing nickname"}, 400)
            return
        if not text and not image:
            send_json(self, {"error": "Add some text or a photo"}, 400)
            return
        if image and len(image) > STORY_IMAGE_MAX:
            send_json(self, {"error": "Photo too large"}, 413)
            return
        story = add_story(nick, text, image)
        send_json(self, {"ok": True, "story": story})

    def _stream_setup(self):
        self.send_response(200)
        origin = cors_origin(self)
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-store")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

    def _pump(self, q):
        while True:
            try:
                self.wfile.write(q.get(timeout=15))
                self.wfile.flush()
            except queue.Empty:
                self.wfile.write(b": ping\n\n")
                self.wfile.flush()

    def handle_stream(self):
        self._stream_setup()
        q = queue.Queue(maxsize=200)
        with mutex:
            clients.add(q)
            replay = [public_msg(m) for m in messages]
        try:
            for m in replay:
                self.wfile.write(encode(m))
                self.wfile.flush()
            self._pump(q)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            with mutex:
                clients.discard(q)

    def handle_dm_stream(self, room):
        self._stream_setup()
        q = queue.Queue(maxsize=200)
        with mutex:
            qs = dm_clients.get(room)
            if qs is None:
                qs = set()
                dm_clients[room] = qs
            qs.add(q)
            r = dm_rooms.get(room)
            replay = [public_msg(m) for m in r["msgs"]] if r else []
        try:
            for m in replay:
                self.wfile.write(encode(m))
                self.wfile.flush()
            self._pump(q)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            with mutex:
                qs = dm_clients.get(room)
                if qs is not None:
                    qs.discard(q)
                    if not qs:
                        dm_clients.pop(room, None)

    def log_message(self, fmt, *args):
        pass


add_message("DayTracker", "Chat is live. Say hi to everyone!")

if __name__ == "__main__":
    threading.Thread(target=prune_loop, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"DayTrack server running on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass