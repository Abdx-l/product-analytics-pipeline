# Product Analytics Pipeline

Incrementally load product events into a SQLite star schema and compute daily active users, seven-day signup retention, and a signup-to-purchase funnel. A source event ID makes reloads idempotent; the high-water mark records the most recent timestamp seen.

```sh
python analytics.py load examples/events.csv --db analytics.db
python analytics.py report --db analytics.db
python -m unittest discover -s tests -v
```

Input CSV columns are `event_id,user_id,occurred_at,event_type`. Timestamps use ISO 8601 UTC strings. The loader validates rows and upserts dimensions in a transaction. The sample is small so every metric can be inspected by hand; larger traces can use the same input format.

