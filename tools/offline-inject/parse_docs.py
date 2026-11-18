#!/usr/bin/env python3
"""Queue parsing for offline-injected documents via the RAGFlow HTTP API.

Docs injected by inject.py land UNSTART (run="0") with no tasks; this script
lists them per knowledge base, seeds an API token in api_token (exact-match
lookup on the server, so a DB-inserted token authenticates), then POSTs
/api/v1/datasets/<kb_id>/chunks — the same endpoint the UI uses — so parsing
runs through the normal task executor (Redis queue, embedding, chunk store).

--runs picks which run states to (re)queue (default: unstart,fail; cancel
allowed). --restart-running additionally STOPs docs stuck in running via
DELETE on the same chunks endpoint — that sets Redis cancel flags and flips
run to cancel server-side, so the status reconciler stops rewriting it —
then re-queues them. Editing document.run by hand fights that reconciler
(ragflow_server syncs it from the task table every few seconds).

Standalone: needs only psycopg2 + requests.
"""

import argparse
import logging
import sys
import time

import psycopg2
import psycopg2.extras
import requests

# common/constants.py TaskStatus (document.run)
RUN_UNSTART, RUN_RUNNING, RUN_CANCEL, RUN_DONE, RUN_FAIL = "0", "1", "2", "3", "4"
RUN_LABELS = {RUN_UNSTART: "unstart", RUN_RUNNING: "running", RUN_CANCEL: "cancel", RUN_DONE: "done", RUN_FAIL: "fail"}

RUN_CODE = {label: code for code, label in RUN_LABELS.items()}
# states queueable via --runs; running docs must be STOPped first (--restart-running)
RUN_CHOICES = ("unstart", "fail", "cancel", "done")


def parse_runs(spec):
    """--runs "unstart,fail" -> ["0", "3"]; deduped, order preserved."""
    runs = []
    for part in spec.split(","):
        label = part.strip().lower()
        if label == RUN_LABELS[RUN_RUNNING]:
            raise SystemExit("run state 'running' can't be queued directly; use --restart-running to stop those docs first")
        if label not in RUN_CHOICES:
            raise SystemExit(f"unknown run state {part!r}; choose from: {', '.join(RUN_CHOICES)}")
        runs.append(RUN_CODE[label])
    if not runs:
        raise SystemExit("--runs must name at least one state")
    return list(dict.fromkeys(runs))


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("parse")


class DB:
    """psycopg2 connection that survives the server dropping idle sessions.

    Vastbase (openGauss) terminates sessions idle past session_timeout
    (default 10 min); queueing parses over HTTP can leave the DB connection
    idle far longer, so every statement retries once on a fresh connection."""

    def __init__(self, **kwargs):
        self.kwargs = dict(kwargs)
        self.kwargs.setdefault("connect_timeout", 10)
        self.conn = psycopg2.connect(**self.kwargs)

    def reset(self):
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = psycopg2.connect(**self.kwargs)

    def q_all(self, sql, params=()):
        for attempt in (1, 2):
            try:
                with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    return cur.fetchall()
            except psycopg2.OperationalError:
                if attempt == 2:
                    raise
                logger.warning("database session dropped; reconnecting and retrying")
                self.reset()

    def execute(self, sql, params=()):
        """Run a statement + commit. Callers keep statements idempotent
        (ON CONFLICT DO NOTHING): a retry after reconnect replays them."""
        for attempt in (1, 2):
            try:
                with self.conn.cursor() as cur:
                    cur.execute(sql, params)
                self.conn.commit()
                return
            except psycopg2.OperationalError:
                if attempt == 2:
                    raise
                logger.warning("database session dropped; reconnecting and retrying")
                self.reset()

    def close(self):
        self.conn.close()


def resolve_tenant(db, email):
    rows = db.q_all(
        'SELECT t.id, t.name FROM tenant t JOIN "user" u ON u.id = t.id WHERE u.email = %s',
        (email,),
    )
    if not rows:
        raise SystemExit(f"no tenant found for email {email}")
    return rows[0]


def pick_kbs(db, args):
    """Knowledge bases to parse: --kb-name patterns (exact or % wildcard),
    scoped to --tenant-email when given; default = every KB holding
    selectable documents (--runs, plus running when --restart-running)."""
    select_runs = list(args.run_codes)
    if args.restart_running:
        select_runs.append(RUN_RUNNING)
    scope, scope_params = "", []
    if args.tenant_email:
        scope = " AND kb.tenant_id = %s"
        scope_params.append(args.tenant_id)
    sql = f"""
        SELECT kb.id, kb.name, kb.tenant_id,
               SUM(CASE WHEN d.run = ANY(%s) THEN 1 ELSE 0 END) AS pending
        FROM knowledgebase kb LEFT JOIN document d ON d.kb_id = kb.id
        WHERE 1 = 1{scope}
        GROUP BY kb.id, kb.name, kb.tenant_id
        HAVING SUM(CASE WHEN d.run = ANY(%s) THEN 1 ELSE 0 END) > 0
        ORDER BY kb.name
    """
    # placeholder order in the SQL text: SELECT's CASE, WHERE's tenant scope,
    # then HAVING's CASE — params must follow that same order
    rows = db.q_all(sql, (select_runs, *scope_params, select_runs))
    if args.kb_name:
        wanted = []
        for pat in args.kb_name:
            op = "LIKE" if ("%" in pat or "_" in pat) else "="
            matched = db.q_all(
                f"""
                SELECT kb.id, kb.name, kb.tenant_id,
                       SUM(CASE WHEN d.run = ANY(%s) THEN 1 ELSE 0 END) AS pending
                FROM knowledgebase kb LEFT JOIN document d ON d.kb_id = kb.id
                WHERE kb.name {op} %s{scope}
                GROUP BY kb.id, kb.name, kb.tenant_id
                """,
                (select_runs, pat, *scope_params),
            )
            wanted.extend(matched)
        seen, rows = set(), []
        for r in wanted:
            if r["id"] not in seen:
                seen.add(r["id"])
                rows.append(r)
        missing = [p for p in args.kb_name if not any(p == r["name"] for r in rows) and "%" not in p and "_" not in p]
        if missing:
            raise SystemExit(f"knowledge base(s) not found or without selectable docs: {missing}")
    return rows


def ensure_token(db, tenant_id):
    """Idempotent per-tenant API token (composite PK tenant_id+token makes the
    deterministic name stable across runs)."""
    token = "injparse_" + tenant_id[:24]
    # Vastbase rejects ON CONFLICT without an explicit conflict target
    db.execute(
        "INSERT INTO api_token (tenant_id, token, source) VALUES (%s, %s, 'none') ON CONFLICT (tenant_id, token) DO NOTHING",
        (tenant_id, token),
    )
    return token


def api_bulk(args, method, kb, doc_ids, token, verb):
    """Call POST/DELETE on the chunks endpoint in batches; returns (ok_ids, errors)."""
    url = f"{args.api_url.rstrip('/')}/api/v1/datasets/{kb['id']}/chunks"
    headers = {"Authorization": f"Bearer {token}"}
    ok_ids, errors = [], []
    for i in range(0, len(doc_ids), args.batch):
        chunk = doc_ids[i : i + args.batch]
        try:
            resp = method(url, json={"document_ids": chunk}, headers=headers, timeout=120)
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            errors.append(f"{kb['name']}: batch {i // args.batch + 1}: {exc}")
            continue
        if body.get("code") != 0:
            errors.append(f"{kb['name']}: batch {i // args.batch + 1}: {body.get('message', body)}")
            continue
        ok_ids.extend(chunk)
        logger.info(f"  {kb['name']}: {verb} {len(ok_ids)}/{len(doc_ids)}")
        if args.pause:
            time.sleep(args.pause)
    return ok_ids, errors


def queue_kb(args, kb, doc_ids, token):
    """POST the parse endpoint in batches; returns (queued_ids, errors)."""
    return api_bulk(args, requests.post, kb, doc_ids, token, "queued")


def stop_kb(args, kb, doc_ids, token):
    """DELETE the chunks endpoint to STOP running docs: the server sets Redis
    cancel flags, flips run to cancel, and drops existing chunks — safe to
    re-parse right after."""
    return api_bulk(args, requests.delete, kb, doc_ids, token, "stopped")


def status_counts(db, kb_id, ids):
    """Run-status histogram for exactly the given document ids."""
    rows = db.q_all(
        "SELECT run, COUNT(*) AS n FROM document WHERE id = ANY(%s) GROUP BY run",
        (ids,),
    )
    return {r["run"]: r["n"] for r in rows}


def wait_done(db, kbs, queued_by_kb, args):
    """Poll until every THIS-RUN queued doc leaves unstart/running (so a batch
    that failed to queue can never hang the wait); then print failures."""
    waited = {kb_id: ids for kb_id, ids in queued_by_kb.items() if ids}
    started = time.time()
    while True:
        line, active = [], 0
        for kb in kbs:
            ids = waited.get(kb["id"])
            if not ids:
                continue
            c = status_counts(db, kb["id"], ids)
            active += c.get(RUN_UNSTART, 0) + c.get(RUN_RUNNING, 0)
            line.append(f"{kb['name']}: done={c.get(RUN_DONE, 0)}/{len(ids)} fail={c.get(RUN_FAIL, 0)} running={c.get(RUN_RUNNING, 0)}")
        logger.info("progress | " + " | ".join(line))
        if active == 0:
            break
        if args.timeout and time.time() - started > args.timeout:
            logger.error(f"--wait timed out after {args.timeout:.0f}s with {active} document(s) still unstart/running")
            sys.exit(1)
        time.sleep(args.interval)
    for kb in kbs:
        ids = waited.get(kb["id"])
        if not ids:
            continue
        fails = db.q_all(
            "SELECT name, progress_msg FROM document WHERE id = ANY(%s) AND run = %s ORDER BY name LIMIT 20",
            (ids, RUN_FAIL),
        )
        if fails:
            logger.warning(f"failed in {kb['name']} (first 20):")
            for f in fails:
                msg = (f["progress_msg"] or "").strip().splitlines()
                logger.warning(f"  {f['name']}: {msg[-1] if msg else 'no message'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dst-host", default="127.0.0.1", help="metadata DB host")
    ap.add_argument("--dst-port", type=int, default=5432)
    ap.add_argument("--dst-user", default="ragflow")
    ap.add_argument("--dst-password", default="")
    ap.add_argument("--dst-meta-db", default="rag_flow")
    ap.add_argument("--api-url", default="http://127.0.0.1:9380", help="ragflow server base URL")
    ap.add_argument("--token", default=None, help="existing API key; default: seed a stable one in api_token")
    ap.add_argument("--tenant-email", default=None, help="scope KBs to this tenant (owner of its API token)")
    ap.add_argument("--kb-name", action="append", default=None, help="KB name, exact or SQL LIKE pattern (repeatable); default: all KBs with selectable docs")
    ap.add_argument("--runs", default="unstart,fail", help=f"comma-separated run states to (re)queue: {', '.join(RUN_CHOICES)} (default: unstart,fail)")
    ap.add_argument("--restart-running", action="store_true", help="first STOP docs stuck in running via the API (cancel flags + run=cancel server-side), then re-queue them")
    ap.add_argument("--batch", type=int, default=5, help="documents per parse API call")
    ap.add_argument("--pause", type=float, default=0.5, help="seconds between API calls")
    ap.add_argument("--wait", action="store_true", help="poll until parsing finishes and report failures")
    ap.add_argument("--interval", type=float, default=10.0, help="poll interval for --wait")
    ap.add_argument("--timeout", type=float, default=0, help="--wait give-up threshold in seconds (0 = wait forever)")
    ap.add_argument("--dry-run", action="store_true", help="list per-KB selectable counts and exit")
    args = ap.parse_args()
    args.run_codes = parse_runs(args.runs)

    db = DB(
        host=args.dst_host,
        port=args.dst_port,
        user=args.dst_user,
        password=args.dst_password,
        dbname=args.dst_meta_db,
    )

    args.tenant_id = resolve_tenant(db, args.tenant_email)["id"] if args.tenant_email else None
    kbs = pick_kbs(db, args)
    if not kbs:
        logger.info(f"no knowledge base has {args.runs} document(s) — nothing to parse")
        return

    total = sum(k["pending"] for k in kbs)
    logger.info(f"{len(kbs)} knowledge base(s), {total} selectable document(s) ({args.runs}):")
    for kb in kbs:
        logger.info(f"  {kb['name']}  ({kb['pending']} docs, tenant {kb['tenant_id'][:8]}…)")
    if args.dry_run:
        return

    tokens = {t: ensure_token(db, t) if not args.token else args.token for t in {k["tenant_id"] for k in kbs}}
    all_ok = True
    queued_by_kb = {}
    select_runs = list(args.run_codes)
    if args.restart_running:
        select_runs.append(RUN_RUNNING)
    for kb in kbs:
        rows = db.q_all("SELECT id, run FROM document WHERE kb_id = %s AND run = ANY(%s)", (kb["id"], select_runs))
        if not rows:
            continue
        doc_ids = [r["id"] for r in rows if r["run"] != RUN_RUNNING]
        stop_short = 0
        if args.restart_running:
            stuck = [r["id"] for r in rows if r["run"] == RUN_RUNNING]
            if stuck:
                logger.info(f"stopping {len(stuck)} running doc(s) in {kb['name']} …")
                stopped, errors = stop_kb(args, kb, stuck, tokens[kb["tenant_id"]])
                for e in errors:
                    logger.error(e)
                # docs whose STOP failed stay running and would be rejected by
                # the parse endpoint; only re-queue the ones actually stopped
                doc_ids.extend(stopped)
                stop_short = len(stuck) - len(stopped)
        if not doc_ids:
            continue
        logger.info(f"queueing {kb['name']} ({len(doc_ids)} docs, batch {args.batch}) …")
        queued_ids, errors = queue_kb(args, kb, doc_ids, tokens[kb["tenant_id"]])
        queued_by_kb[kb["id"]] = queued_ids
        for e in errors:
            logger.error(e)
        all_ok = all_ok and not errors and not stop_short and len(queued_ids) == len(doc_ids)
    if not args.token:
        for tenant_id, token in tokens.items():
            logger.info(f"seeded API token for tenant {tenant_id[:8]}…: {token}")

    if args.wait:
        wait_done(db, kbs, queued_by_kb, args)
    db.close()
    if not all_ok:
        sys.exit(1)
    logger.info("PARSE SUMMARY: all requested documents queued" + (" and finished" if args.wait else ""))


if __name__ == "__main__":
    main()
