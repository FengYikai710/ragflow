#!/usr/bin/env python3
"""Queue parsing for offline-injected documents via the RAGFlow HTTP API.

Docs injected by inject.py land with NO ingestion_task row (derived status
UNSTART); this script lists them per knowledge base, seeds an API token in
api_token (exact-match lookup on the server, so a DB-inserted token
authenticates — user_id is mandatory or the server rejects it), then POSTs
/api/v1/datasets/<kb_id>/documents/parse — the same endpoint the UI uses —
so parsing runs through the normal ingestion executor (queue, embedding,
chunk store).

Status lives in ingestion_task.status (document.run is gone on main):
CREATED / SCHEDULED / RUNNING / STOPPING are active, COMPLETED / FAILED /
STOPPED terminal, and a document without a task row reads UNSTART.

--runs picks which states to (re)queue (default: unstart,fail; cancel and
done allowed). --restart-running additionally STOPs docs stuck in an active
state via POST .../documents/stop, then re-queues them.

Standalone: needs only psycopg2 + requests.
"""

import argparse
import logging
import sys
import time

import psycopg2
import psycopg2.extras
import requests

# internal/common/constants.go task statuses (ingestion_task.status).
# A document with no task row (or empty status) reads UNSTART.
ACTIVE_STATUSES = ("CREATED", "SCHEDULED", "RUNNING", "STOPPING")  # IsActiveTaskStatus
STATUS_UNSTART, STATUS_COMPLETED, STATUS_FAILED, STATUS_STOPPED = "UNSTART", "COMPLETED", "FAILED", "STOPPED"

# label -> ingestion statuses; "unstart" matches docs with no/empty task row
RUN_LABELS = {
    "unstart": (STATUS_UNSTART,),
    "running": ACTIVE_STATUSES,
    "fail": (STATUS_FAILED,),
    "cancel": (STATUS_STOPPED,),
    "done": (STATUS_COMPLETED,),
}
RUN_CHOICES = tuple(RUN_LABELS)
# statuses queueable via --runs; running docs must be STOPped first (--restart-running)
QUEUEABLE = tuple(l for l in RUN_CHOICES if l != "running")

# derived document ingestion status: no task row / empty status -> UNSTART
DOC_STATUS_EXPR = "COALESCE(NULLIF(t.status, ''), 'UNSTART')"


def parse_runs(spec):
    """--runs "unstart,fail" -> ("UNSTART", "FAILED"); deduped, order kept."""
    statuses = []
    for part in spec.split(","):
        label = part.strip().lower()
        if label == "running":
            raise SystemExit("state 'running' can't be queued directly; use --restart-running to stop those docs first")
        if label not in QUEUEABLE:
            raise SystemExit(f"unknown run state {part!r}; choose from: {', '.join(QUEUEABLE)}")
        statuses.extend(RUN_LABELS[label])
    if not statuses:
        raise SystemExit("--runs must name at least one state")
    return tuple(dict.fromkeys(statuses))


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


# documents joined with their latest ingestion task (fresh schemas have a
# unique index on ingestion_task.document_id; DISTINCT ON guards legacy
# multi-row history, keeping the newest task — same rule as the Go dao)
_DOC_WITH_STATUS = f"""
    document d
    LEFT JOIN (
        SELECT DISTINCT ON (document_id) document_id, status
        FROM ingestion_task
        ORDER BY document_id, create_time DESC, id DESC
    ) t ON t.document_id = d.id
"""


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
    select_statuses = list(args.run_statuses)
    if args.restart_running:
        select_statuses.extend(ACTIVE_STATUSES)
    scope, scope_params = "", []
    if args.tenant_email:
        scope = " AND kb.tenant_id = %s"
        scope_params.append(args.tenant_id)
    sql = f"""
        SELECT kb.id, kb.name, kb.tenant_id,
               SUM(CASE WHEN {DOC_STATUS_EXPR} = ANY(%s) THEN 1 ELSE 0 END) AS pending
        FROM knowledgebase kb LEFT JOIN {_DOC_WITH_STATUS} ON d.kb_id = kb.id
        WHERE 1 = 1{scope}
        GROUP BY kb.id, kb.name, kb.tenant_id
        HAVING SUM(CASE WHEN {DOC_STATUS_EXPR} = ANY(%s) THEN 1 ELSE 0 END) > 0
        ORDER BY kb.name
    """
    # placeholder order in the SQL text: SELECT's CASE, WHERE's tenant scope,
    # then HAVING's CASE — params must follow that same order
    rows = db.q_all(sql, (select_statuses, *scope_params, select_statuses))
    if args.kb_name:
        wanted = []
        for pat in args.kb_name:
            op = "LIKE" if ("%" in pat or "_" in pat) else "="
            matched = db.q_all(
                f"""
                SELECT kb.id, kb.name, kb.tenant_id,
                       SUM(CASE WHEN {DOC_STATUS_EXPR} = ANY(%s) THEN 1 ELSE 0 END) AS pending
                FROM knowledgebase kb LEFT JOIN {_DOC_WITH_STATUS} ON d.kb_id = kb.id
                WHERE kb.name {op} %s{scope}
                GROUP BY kb.id, kb.name, kb.tenant_id
                """,
                (select_statuses, pat, *scope_params),
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
    deterministic name stable across runs). user_id is REQUIRED: the server
    rejects tokens without a user identity; owner user shares the tenant id."""
    owner = db.q_all(
        "SELECT user_id FROM user_tenant WHERE tenant_id = %s AND role = 'owner' AND status = '1' LIMIT 1",
        (tenant_id,),
    )
    user_id = owner[0]["user_id"] if owner else tenant_id
    token = "injparse_" + tenant_id[:24]
    # Vastbase rejects ON CONFLICT without an explicit conflict target
    db.execute(
        "INSERT INTO api_token (tenant_id, user_id, token, source) VALUES (%s, %s, %s, 'none') ON CONFLICT (tenant_id, token) DO NOTHING",
        (tenant_id, user_id, token),
    )
    return token


def api_bulk(args, method, kb, doc_ids, token, verb, path):
    """POST an ingestion endpoint in batches; returns (ok_ids, errors)."""
    url = f"{args.api_url.rstrip('/')}/api/v1/datasets/{kb['id']}/{path}"
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
    return api_bulk(args, requests.post, kb, doc_ids, token, "queued", "documents/parse")


def stop_kb(args, kb, doc_ids, token):
    """POST .../documents/stop to STOP active docs: the server cancels the
    running ingestion task (-> STOPPED) — safe to re-parse right after."""
    return api_bulk(args, requests.post, kb, doc_ids, token, "stopped", "documents/stop")


def status_counts(db, kb_id, ids):
    """Ingestion-status histogram for exactly the given document ids."""
    rows = db.q_all(
        f"SELECT {DOC_STATUS_EXPR} AS status, COUNT(*) AS n FROM {_DOC_WITH_STATUS} WHERE d.kb_id = %s AND d.id = ANY(%s) GROUP BY 1",
        (kb_id, ids),
    )
    return {r["status"]: r["n"] for r in rows}


def wait_done(db, kbs, queued_by_kb, args):
    """Poll until every THIS-RUN queued doc leaves unstart/active (so a batch
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
            active += c.get(STATUS_UNSTART, 0) + sum(c.get(s, 0) for s in ACTIVE_STATUSES)
            line.append(f"{kb['name']}: done={c.get(STATUS_COMPLETED, 0)}/{len(ids)} fail={c.get(STATUS_FAILED, 0)} running={sum(c.get(s, 0) for s in ACTIVE_STATUSES)}")
        logger.info("progress | " + " | ".join(line))
        if active == 0:
            break
        if args.timeout and time.time() - started > args.timeout:
            logger.error(f"--wait timed out after {args.timeout:.0f}s with {active} document(s) still unstart/active")
            sys.exit(1)
        time.sleep(args.interval)
    for kb in kbs:
        ids = waited.get(kb["id"])
        if not ids:
            continue
        fails = db.q_all(
            f"SELECT d.name, d.progress_msg FROM {_DOC_WITH_STATUS} WHERE d.kb_id = %s AND d.id = ANY(%s) AND {DOC_STATUS_EXPR} = %s ORDER BY d.name LIMIT 20",
            (kb["id"], ids, STATUS_FAILED),
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
    ap.add_argument("--runs", default="unstart,fail", help=f"comma-separated states to (re)queue: {', '.join(QUEUEABLE)} (default: unstart,fail)")
    ap.add_argument("--restart-running", action="store_true", help="first STOP docs stuck in an active state via the API (POST documents/stop), then re-queue them")
    ap.add_argument("--batch", type=int, default=5, help="documents per parse API call")
    ap.add_argument("--pause", type=float, default=0.5, help="seconds between API calls")
    ap.add_argument("--wait", action="store_true", help="poll until parsing finishes and report failures")
    ap.add_argument("--interval", type=float, default=10.0, help="poll interval for --wait")
    ap.add_argument("--timeout", type=float, default=0, help="--wait give-up threshold in seconds (0 = wait forever)")
    ap.add_argument("--dry-run", action="store_true", help="list per-KB selectable counts and exit")
    args = ap.parse_args()
    args.run_statuses = parse_runs(args.runs)

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
    select_statuses = list(args.run_statuses)
    if args.restart_running:
        select_statuses.extend(ACTIVE_STATUSES)
    for kb in kbs:
        rows = db.q_all(
            f"SELECT d.id, {DOC_STATUS_EXPR} AS status FROM {_DOC_WITH_STATUS} WHERE d.kb_id = %s AND {DOC_STATUS_EXPR} = ANY(%s)",
            (kb["id"], select_statuses),
        )
        if not rows:
            continue
        doc_ids = [r["id"] for r in rows if r["status"] not in ACTIVE_STATUSES]
        stop_short = 0
        if args.restart_running:
            stuck = [r["id"] for r in rows if r["status"] in ACTIVE_STATUSES]
            if stuck:
                logger.info(f"stopping {len(stuck)} active doc(s) in {kb['name']} …")
                stopped, errors = stop_kb(args, kb, stuck, tokens[kb["tenant_id"]])
                for e in errors:
                    logger.error(e)
                # docs whose STOP failed stay active and would be rejected by
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
