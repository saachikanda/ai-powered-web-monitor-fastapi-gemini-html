# 🔍 WebWatch — Website Monitoring & Alert System

A beautiful, real-time website change monitoring tool with AI-powered summaries.

---

## Features

- **Monitor any URL** — track prices, news, product pages, competitors, status pages
- **Periodic checks** — set intervals from 5 min to 24 hours per tracker
- **Change detection** — SHA-256 hash comparison + CSS selector support
- **Change history** — full diff viewer for every detected change
- **AI Summaries** — Claude analyzes diffs and writes plain-English summaries
- **Real-time alerts** — WebSocket-powered live notifications in the dashboard
- **Beautiful dashboard** — dark cyberpunk-inspired UI with live stats

---

## Quick Start

### 1. Install dependencies

```bash
cd webwatch
pip install -r requirements.txt
```

### 2. (Optional) Set Gemini API key for AI summaries

```bash
export GEMINI_API_KEY=sk-ant-... 
```

Or paste it in Settings → AI Settings after launching.

### 3. Run the server

```bash
python app.py
```

Open [http://localhost:8000](http://localhost:8000)

---

## Usage

1. Click **+ Add Tracker** on the Trackers page
2. Enter a name, URL, and how often to check it
3. Optionally enter a CSS selector (`.price`, `#stock-status`, `main`) to watch only part of the page
4. WebWatch runs an immediate first check, then on the interval you set
5. Changes appear in the **Changes** tab with AI summaries and a diff viewer
6. Real-time **toast notifications** pop up when changes are detected

---

## Project Structure

```
webwatch/
  app.py              # FastAPI backend (all-in-one)
  webwatch.db         # SQLite database (auto-created)
  requirements.txt
  frontend/
    index.html        # Single-page dashboard
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stats` | Dashboard statistics |
| GET | `/api/trackers` | List all trackers |
| POST | `/api/trackers` | Create tracker |
| PUT | `/api/trackers/{id}` | Update tracker |
| DELETE | `/api/trackers/{id}` | Delete tracker |
| POST | `/api/trackers/{id}/check` | Manual check now |
| GET | `/api/trackers/{id}/history` | Change history |
| GET | `/api/changes` | All recent changes |
| GET | `/api/changes/{id}/diff` | Diff + AI summary |
| GET | `/api/notifications` | All notifications |
| WS | `/ws` | Real-time events |

---

## Tech Stack

- **Backend**: Python, FastAPI, APScheduler, httpx, BeautifulSoup, aiosqlite
- **AI**: Google Gemini (claude-sonnet-4)
- **Frontend**: Vanilla HTML/CSS/JS, WebSocket
- **Database**: SQLite (zero config)