"""
WebWatch — Website Monitoring & Alert System
Backend: FastAPI + SQLite + APScheduler + Gemini AI
"""

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from difflib import unified_diff
from pathlib import Path
from typing import List, Optional, Set

import aiosqlite
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─── Config ───────────────────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "webwatch.db"
FRONTEND_PATH = Path(__file__).parent / "frontend"

# ─── Pydantic Models ──────────────────────────────────────────────────────────
class TrackerCreate(BaseModel):
    name: str
    url: str
    check_interval: int = 60  # minutes
    selector: Optional[str] = None

class TrackerUpdate(BaseModel):
    name: Optional[str] = None
    check_interval: Optional[int] = None
    selector: Optional[str] = None
    status: Optional[str] = None

class ApiKeyUpdate(BaseModel):
    api_key: str

# ─── WebSocket Manager ────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active_connections.discard(ws)

    async def broadcast(self, message: dict):
        dead = set()
        for ws in self.active_connections:
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                dead.add(ws)
        self.active_connections -= dead

manager = ConnectionManager()
scheduler = AsyncIOScheduler()
_api_key_cache: Optional[str] = None

# ─── Database ─────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trackers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                check_interval INTEGER DEFAULT 60,
                selector TEXT,
                status TEXT DEFAULT 'active',
                last_checked TIMESTAMP,
                last_hash TEXT,
                last_content TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tracker_id INTEGER REFERENCES trackers(id) ON DELETE CASCADE,
                old_content TEXT,
                new_content TEXT,
                diff TEXT,
                ai_summary TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tracker_id INTEGER REFERENCES trackers(id) ON DELETE CASCADE,
                tracker_name TEXT,
                message TEXT NOT NULL,
                type TEXT DEFAULT 'change',
                read INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.commit()

# ─── Core Checking Logic ──────────────────────────────────────────────────────
async def fetch_content(url: str, selector: Optional[str] = None) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        html = response.text

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "path"]):
        tag.decompose()

    if selector:
        element = soup.select_one(selector)
        text = element.get_text("\n", strip=True) if element else soup.get_text("\n", strip=True)
    else:
        text = soup.get_text("\n", strip=True)

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines)

def compute_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()

def compute_diff(old: str, new: str) -> str:
    diff = list(unified_diff(
        old.splitlines(keepends=True),
        new.splitlines(keepends=True),
        fromfile="previous", tofile="current", n=3
    ))
    return "".join(diff[:300])

async def get_api_key() -> Optional[str]:
    global _api_key_cache
    if _api_key_cache:
        return _api_key_cache
    env_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if env_key:
        _api_key_cache = env_key
        return _api_key_cache
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'gemini_api_key'")
        row = await cursor.fetchone()
        if row and row["value"]:
            _api_key_cache = row["value"]
            return _api_key_cache
    return None

# FIX: Use AsyncAnthropic and await the API call to avoid blocking the event loop
async def generate_ai_summary(url: str, diff: str) -> str:
    api_key = await get_api_key()

    if not api_key:
        return "No API key configured"

    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)

        model = genai.GenerativeModel(
            "models/gemini-3.5-flash" 
        )

        excerpt = diff[:3000]

        prompt = f"""
Summarize the website changes clearly.

URL:
{url}

Diff:
{excerpt}
"""

        print("USING UPDATED GEMINI MODEL")

        response = await asyncio.to_thread(
            model.generate_content,
            prompt
        )

        return response.text.strip()

    except Exception as e:
        print("GEMINI ERROR:", e)
        return f"AI unavailable: {str(e)}"
# ─── Monitor Check ─────────────────────────────────────────────────────────────
async def check_tracker(tracker_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trackers WHERE id = ? AND status = 'active'", (tracker_id,)
        )
        tracker = await cursor.fetchone()
        if not tracker:
            return

        url, selector = tracker["url"], tracker["selector"]
        now = datetime.utcnow().isoformat()

        try:
            content = await fetch_content(url, selector)
            content_hash = compute_hash(content)

            if tracker["last_hash"] and tracker["last_hash"] != content_hash:
                # Change detected!
                old_content = tracker["last_content"] or ""
                diff = compute_diff(old_content, content)
                ai_summary = await generate_ai_summary(url, diff)
                print("USING UPDATED GEMINI MODEL")
                await db.execute("""
                    INSERT INTO changes (tracker_id, old_content, new_content, diff, ai_summary)
                    VALUES (?, ?, ?, ?, ?)
                """, (tracker_id, old_content, content, diff, ai_summary))

                await db.execute("""
                    INSERT INTO notifications (tracker_id, tracker_name, message, type)
                    VALUES (?, ?, ?, 'change')
                """, (tracker_id, tracker["name"], f"Change detected on \"{tracker['name']}\""))

                await manager.broadcast({
                    "type": "change_detected",
                    "tracker_id": tracker_id,
                    "tracker_name": tracker["name"],
                    "url": url,
                    "summary": ai_summary,
                    "timestamp": now
                })

            await db.execute("""
                UPDATE trackers SET last_checked=?, last_hash=?, last_content=?, error_message=NULL
                WHERE id=?
            """, (now, content_hash, content, tracker_id))

            await db.commit()

            await manager.broadcast({
                "type": "tracker_checked",
                "tracker_id": tracker_id,
                "status": "active",
                "timestamp": now,
                "changed": tracker["last_hash"] is not None and tracker["last_hash"] != content_hash
            })

        except Exception as e:
            err = str(e)[:400]
            await db.execute("""
                UPDATE trackers SET last_checked=?, error_message=?, status='error' WHERE id=?
            """, (now, err, tracker_id))
            await db.commit()

            await manager.broadcast({
                "type": "check_error",
                "tracker_id": tracker_id,
                "tracker_name": tracker["name"],
                "error": err,
                "timestamp": now
            })

# ─── App Lifecycle ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    scheduler.start()

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, check_interval FROM trackers WHERE status = 'active'"
        )
        for t in await cursor.fetchall():
            scheduler.add_job(
                check_tracker,
                IntervalTrigger(minutes=int(t["check_interval"])),
                args=[t["id"]],
                id=f"tracker_{t['id']}",
                replace_existing=True,
            )

    yield
    scheduler.shutdown()

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(title="WebWatch", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

app.mount("/static", StaticFiles(directory=str(FRONTEND_PATH)), name="static")

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return FileResponse(FRONTEND_PATH / "index.html")

@app.get("/api/stats")
async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        total = (await (await db.execute("SELECT COUNT(*) c FROM trackers")).fetchone())["c"]
        active = (await (await db.execute("SELECT COUNT(*) c FROM trackers WHERE status='active'")).fetchone())["c"]
        errors = (await (await db.execute("SELECT COUNT(*) c FROM trackers WHERE status='error'")).fetchone())["c"]
        changes_today = (await (await db.execute(
            "SELECT COUNT(*) c FROM changes WHERE date(detected_at)=date('now')"
        )).fetchone())["c"]
        unread = (await (await db.execute(
            "SELECT COUNT(*) c FROM notifications WHERE read=0"
        )).fetchone())["c"]
        total_changes = (await (await db.execute("SELECT COUNT(*) c FROM changes")).fetchone())["c"]
        return {
            "total_trackers": total,
            "active_trackers": active,
            "error_trackers": errors,
            "changes_today": changes_today,
            "unread_notifications": unread,
            "total_changes": total_changes,
        }

@app.get("/api/trackers")
async def list_trackers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT t.*,
                (SELECT COUNT(*) FROM changes WHERE tracker_id=t.id) change_count,
                (SELECT detected_at FROM changes WHERE tracker_id=t.id ORDER BY detected_at DESC LIMIT 1) last_change
            FROM trackers t ORDER BY t.created_at DESC
        """)
        return [dict(r) for r in await cursor.fetchall()]

@app.post("/api/trackers", status_code=201)
async def create_tracker(data: TrackerCreate):
    # Validate URL
    if not data.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO trackers (name, url, check_interval, selector)
            VALUES (?, ?, ?, ?)
        """, (data.name, data.url, data.check_interval, data.selector))
        tracker_id = cursor.lastrowid
        await db.commit()

    scheduler.add_job(
        check_tracker,
        IntervalTrigger(minutes=data.check_interval),
        args=[tracker_id],
        id=f"tracker_{tracker_id}",
        replace_existing=True,
    )
    asyncio.create_task(check_tracker(tracker_id))
    return {"id": tracker_id, "message": "Tracker created — initial check queued"}

@app.get("/api/trackers/{tracker_id}")
async def get_tracker(tracker_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM trackers WHERE id=?", (tracker_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        return dict(row)

@app.put("/api/trackers/{tracker_id}")
async def update_tracker(tracker_id: int, data: TrackerUpdate):
    updates = {k: v for k, v in data.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "Nothing to update")

    set_clause = ", ".join(f"{k}=?" for k in updates)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(
            f"UPDATE trackers SET {set_clause} WHERE id=?",
            [*updates.values(), tracker_id]
        )
        await db.commit()

        # Reschedule if needed
        row = await (await db.execute("SELECT * FROM trackers WHERE id=?", (tracker_id,))).fetchone()
        try:
            scheduler.remove_job(f"tracker_{tracker_id}")
        except Exception:
            pass
        if row and row["status"] == "active":
            scheduler.add_job(
                check_tracker,
                IntervalTrigger(minutes=int(row["check_interval"])),
                args=[tracker_id],
                id=f"tracker_{tracker_id}",
                replace_existing=True,
            )
    return {"message": "Updated"}

@app.delete("/api/trackers/{tracker_id}")
async def delete_tracker(tracker_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM trackers WHERE id=?", (tracker_id,))
        await db.commit()
    try:
        scheduler.remove_job(f"tracker_{tracker_id}")
    except Exception:
        pass
    return {"message": "Deleted"}

@app.post("/api/trackers/{tracker_id}/check")
async def manual_check(tracker_id: int):
    asyncio.create_task(check_tracker(tracker_id))
    return {"message": "Check queued"}

@app.get("/api/trackers/{tracker_id}/history")
async def get_history(tracker_id: int, limit: int = 20):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT id, tracker_id, ai_summary, detected_at,
                LENGTH(old_content) old_size, LENGTH(new_content) new_size
            FROM changes WHERE tracker_id=? ORDER BY detected_at DESC LIMIT ?
        """, (tracker_id, limit))
        return [dict(r) for r in await cursor.fetchall()]

@app.get("/api/changes")
async def get_recent_changes(limit: int = 30):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT c.id, c.tracker_id, c.ai_summary, c.detected_at,
                t.name tracker_name, t.url tracker_url
            FROM changes c JOIN trackers t ON t.id=c.tracker_id
            ORDER BY c.detected_at DESC LIMIT ?
        """, (limit,))
        return [dict(r) for r in await cursor.fetchall()]

@app.get("/api/changes/{change_id}/diff")
async def get_diff(change_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM changes WHERE id=?", (change_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        result = dict(row)
        result.pop("old_content", None)
        result.pop("new_content", None)
        return result

@app.get("/api/notifications")
async def get_notifications(limit: int = 50):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM notifications ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in await cursor.fetchall()]

@app.post("/api/notifications/read-all")
async def mark_all_read():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE notifications SET read=1")
        await db.commit()
    return {"message": "All marked as read"}

@app.post("/api/notifications/{nid}/read")
async def mark_read(nid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE notifications SET read=1 WHERE id=?", (nid,))
        await db.commit()
    return {"message": "Marked as read"}

@app.get("/api/settings")
async def get_settings():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT key, value FROM settings")
        rows = await cursor.fetchall()
        result = {r["key"]: r["value"] for r in rows}
        env_key = os.environ.get("GEMINI_API_KEY", "").strip()
        result["ai_enabled"] = bool(env_key or "gemini_api_key" in result)
        return result

@app.post("/api/settings/api-key")
async def save_api_key(data: ApiKeyUpdate):
    global _api_key_cache
    clean_key = data.api_key.strip()
    _api_key_cache = clean_key  # update cache immediately
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('gemini_api_key', ?)",
            (clean_key,)
        )
        await db.commit()
    return {"message": "API key saved"}

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
@app.post("/api/changes/regenerate-summaries")
async def regenerate_summaries():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Find all changes with missing or error summaries
        cursor = await db.execute("""
            SELECT c.id, c.diff, t.url FROM changes c
            JOIN trackers t ON t.id = c.tracker_id
            WHERE c.ai_summary IS NULL
               OR c.ai_summary LIKE 'AI unavailable%'
               OR c.ai_summary LIKE '%gemini%'
               OR c.ai_summary LIKE '%invalid_request%'
        """)
        broken = await cursor.fetchall()

    fixed = 0
    for row in broken:
        if not row["diff"]:
            continue
        summary = await generate_ai_summary(row["url"], row["diff"])
        if summary:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE changes SET ai_summary = ? WHERE id = ?",
                    (summary, row["id"])
                )
                await db.commit()
            fixed += 1

    return {"fixed": fixed, "total": len(broken)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("monitor:app", host="0.0.0.0", port=8000, reload=False)