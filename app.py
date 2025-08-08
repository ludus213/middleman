import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

import aiosqlite
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from pydantic import BaseModel, Field

DATABASE_PATH = os.environ.get("MM_DB_PATH", os.path.join(os.path.dirname(__file__), "data.db"))
ADMIN_TOKEN = os.environ.get("MM_ADMIN_TOKEN", "changeme")

app = FastAPI()


class DeathEventIn(BaseModel):
    instance_id: str = Field(..., min_length=1)
    attacker: Optional[str] = None
    victim: str
    cause: str
    position: Optional[Dict[str, float]] = None
    time: Optional[str] = None


class ModActionIn(BaseModel):
    action: str
    player: str
    reason: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


class StrikeActionIn(BaseModel):
    player: str


class RestoreIn(BaseModel):
    player: str
    amount: int


class CommandQueueItem(BaseModel):
    id: int
    command: str
    payload: Dict[str, Any]


class PlayerStateIn(BaseModel):
    player: str
    strikes: int = 0
    banned: bool | int = 0
    vestige: int = 0


async def init_db():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS deaths (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id TEXT UNIQUE,
                attacker TEXT,
                victim TEXT,
                cause TEXT,
                pos_x REAL,
                pos_y REAL,
                pos_z REAL,
                time TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT,
                action TEXT,
                player TEXT,
                reason TEXT,
                extra_json TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT,
                command TEXT,
                payload_json TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS player_states (
                player TEXT PRIMARY KEY,
                strikes INTEGER,
                banned INTEGER,
                vestige INTEGER,
                updated TEXT
            )
            """
        )
        await db.commit()


@app.on_event("startup")
async def on_startup():
    await init_db()


async def require_admin(x_admin_token: str = Header(default="")):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


@app.post("/death")
async def post_death(event: DeathEventIn):
    when = event.time or datetime.utcnow().isoformat()
    pos_x = event.position.get("x") if event.position else None
    pos_y = event.position.get("y") if event.position else None
    pos_z = event.position.get("z") if event.position else None
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO deaths(instance_id, attacker, victim, cause, pos_x, pos_y, pos_z, time) VALUES(?,?,?,?,?,?,?,?)",
            [event.instance_id, event.attacker, event.victim, event.cause, pos_x, pos_y, pos_z, when],
        )
        await db.commit()
    return {"ok": True, "instance_id": event.instance_id}


@app.get("/deaths")
async def get_deaths(offset: int = 0, limit: int = 50):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM deaths ORDER BY id DESC LIMIT ? OFFSET ?",
            [limit, offset],
        )
        rows = await cursor.fetchall()
    return {"ok": True, "results": [dict(r) for r in rows]}


@app.get("/deaths/player/{player}")
async def get_deaths_by_player(player: str, offset: int = 0, limit: int = 50):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM deaths WHERE victim = ? OR attacker = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            [player, player, limit, offset],
        )
        rows = await cursor.fetchall()
    return {"ok": True, "results": [dict(r) for r in rows]}


@app.get("/deaths/instance/{instance_id}")
async def get_death_instance(instance_id: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM deaths WHERE instance_id = ?", [instance_id])
        row = await cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    return {"ok": True, "result": dict(row)}


@app.post("/mod/action", dependencies=[Depends(require_admin)])
async def post_mod_action(action: ModActionIn):
    when = datetime.utcnow().isoformat()
    extra_json = json.dumps(action.extra) if action.extra is not None else None
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT INTO mod_actions(time, action, player, reason, extra_json) VALUES(?,?,?,?,?)",
            [when, action.action, action.player, action.reason, extra_json],
        )
        await db.commit()
    return {"ok": True}


@app.get("/mod/actions", dependencies=[Depends(require_admin)])
async def list_mod_actions(offset: int = 0, limit: int = 50):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM mod_actions ORDER BY id DESC LIMIT ? OFFSET ?",
            [limit, offset],
        )
        rows = await cursor.fetchall()
    return {"ok": True, "results": [dict(r) for r in rows]}


async def enqueue_command(command: str, payload: Dict[str, Any]):
    when = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT INTO commands(time, command, payload_json) VALUES(?,?,?)",
            [when, command, json.dumps(payload)],
        )
        await db.commit()


@app.post("/command/restore", dependencies=[Depends(require_admin)])
async def command_restore(body: RestoreIn):
    await enqueue_command("restore", {"player": body.player, "amount": body.amount})
    return {"ok": True}


@app.post("/command/strike", dependencies=[Depends(require_admin)])
async def command_strike(body: StrikeActionIn):
    await enqueue_command("strike", {"player": body.player})
    return {"ok": True}


@app.post("/command/ban", dependencies=[Depends(require_admin)])
async def command_ban(body: StrikeActionIn):
    await enqueue_command("ban", {"player": body.player})
    return {"ok": True}


@app.post("/command/unban", dependencies=[Depends(require_admin)])
async def command_unban(body: StrikeActionIn):
    await enqueue_command("unban", {"player": body.player})
    return {"ok": True}


@app.post("/command/kick", dependencies=[Depends(require_admin)])
async def command_kick(body: StrikeActionIn):
    await enqueue_command("kick", {"player": body.player})
    return {"ok": True}


@app.get("/commands/poll")
async def poll_commands(limit: int = 25):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, time, command, payload_json FROM commands ORDER BY id ASC LIMIT ?",
            [limit],
        )
        rows = await cursor.fetchall()
    results: List[CommandQueueItem] = []
    for r in rows:
        payload = {}
        if r["payload_json"]:
            payload = json.loads(r["payload_json"])  # type: ignore
        results.append(CommandQueueItem(id=r["id"], command=r["command"], payload=payload))
    return {"ok": True, "results": [c.model_dump() for c in results]}


class AckIn(BaseModel):
    ids: List[int]


@app.post("/commands/ack")
async def ack_commands(body: AckIn):
    if not body.ids:
        return {"ok": True}
    q = "DELETE FROM commands WHERE id IN (" + ",".join(["?"] * len(body.ids)) + ")"
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(q, body.ids)
        await db.commit()
    return {"ok": True}


@app.post("/player/state")
async def update_player_state(body: PlayerStateIn):
    banned_int = 1 if bool(body.banned) else 0
    when = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO player_states(player, strikes, banned, vestige, updated)
            VALUES(?,?,?,?,?)
            ON CONFLICT(player) DO UPDATE SET
                strikes=excluded.strikes,
                banned=excluded.banned,
                vestige=excluded.vestige,
                updated=excluded.updated
            """,
            [body.player, int(body.strikes), banned_int, int(body.vestige), when],
        )
        await db.commit()
    return {"ok": True}


@app.get("/player/state")
async def get_player_state(player: str = Query(...)):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT player, strikes, banned, vestige, updated FROM player_states WHERE player = ?",
            [player],
        )
        row = await cursor.fetchone()
    if not row:
        return {"ok": True, "result": {"player": player, "strikes": 0, "banned": 0, "vestige": 0}}
    return {"ok": True, "result": dict(row)}


@app.get("/ping")
async def ping():
    return {"ok": True, "service": "middleman", "time": datetime.utcnow().isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
