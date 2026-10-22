"""Incremental event warehouse and product metrics."""
import argparse
import csv
import json
import sqlite3
from datetime import datetime, timedelta


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS dim_user(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS dim_date(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS dim_event_type(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS fact_events(
 event_id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES dim_user(id),
 date_id TEXT NOT NULL REFERENCES dim_date(id),event_type TEXT NOT NULL REFERENCES dim_event_type(id),
 occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_date_type ON fact_events(date_id,event_type,user_id);
CREATE TABLE IF NOT EXISTS ingest_state(id INTEGER PRIMARY KEY CHECK(id=1),high_water TEXT NOT NULL);
"""


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db


def load(db, rows):
    watermark_row = db.execute("SELECT high_water FROM ingest_state WHERE id=1").fetchone()
    watermark = watermark_row[0] if watermark_row else ""
    latest = watermark
    count = 0
    with db:
        for row in rows:
            eid, uid, stamp, kind = (row.get(key, "").strip() for key in ("event_id", "user_id", "occurred_at", "event_type"))
            if not all((eid, uid, stamp, kind)):
                raise ValueError("event_id, user_id, occurred_at, event_type are required")
            try:
                parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError()
            except ValueError:
                raise ValueError(f"invalid UTC timestamp: {stamp}") from None
            if stamp <= watermark:
                continue
            day = parsed.date().isoformat()
            for table, value in (("dim_user", uid), ("dim_date", day), ("dim_event_type", kind)):
                db.execute(f"INSERT OR IGNORE INTO {table}(id) VALUES(?)", (value,))
            cursor = db.execute("INSERT OR IGNORE INTO fact_events VALUES(?,?,?,?,?)", (eid, uid, day, kind, stamp))
            count += cursor.rowcount
            latest = max(latest, stamp)
        if latest:
            db.execute("INSERT INTO ingest_state(id,high_water) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET high_water=excluded.high_water", (latest,))
    return count


def metrics(db):
    dau = [dict(row) for row in db.execute("SELECT date_id AS day,COUNT(DISTINCT user_id) AS users FROM fact_events GROUP BY date_id ORDER BY date_id")]
    signed_at = {row["user_id"]: row["signup"] for row in db.execute("SELECT user_id,MIN(occurred_at) AS signup FROM fact_events WHERE event_type='signup' GROUP BY user_id")}
    signed = {uid: stamp[:10] for uid, stamp in signed_at.items()}
    active = {(row["user_id"], row["date_id"]) for row in db.execute("SELECT user_id,date_id FROM fact_events")}
    retention = []
    for day in sorted(set(signed.values())):
        cohort = [uid for uid, signup in signed.items() if signup == day]
        target = (datetime.fromisoformat(day) + timedelta(days=7)).date().isoformat()
        retained = sum((uid, target) in active for uid in cohort)
        retention.append({"signup_day": day, "users": len(cohort), "day7_retained": retained})
    purchased = {(row["user_id"], row["occurred_at"]) for row in db.execute("SELECT user_id,occurred_at FROM fact_events WHERE event_type='purchase'")}
    funnel = {"signed_up": len(signed), "purchased_after_signup": sum(any(uid == buyer and stamp >= signed_at[uid] for buyer, stamp in purchased) for uid in signed)}
    return {"daily_active_users": dau, "day7_retention": retention, "funnel": funnel}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["load", "report"])
    p.add_argument("file", nargs="?")
    p.add_argument("--db", default="analytics.db")
    a = p.parse_args()
    with connect(a.db) as db:
        if a.command == "load":
            if not a.file:
                p.error("load requires CSV path")
            with open(a.file, newline="", encoding="utf-8") as source:
                print(f"Loaded {load(db, csv.DictReader(source))} events")
        else:
            print(json.dumps(metrics(db), indent=2))


if __name__ == "__main__":
    main()

