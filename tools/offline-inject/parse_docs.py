#!/usr/bin/env python3
"""Queue parsing for offline-injected documents via the RAGFlow HTTP API.

Docs injected by inject.py land UNSTART (run="0") with no tasks; this script
lists them per knowledge base, seeds an API token in api_token (exact-match
lookup on the server, so a DB-inserted token authenticates), then POSTs
/api/v1/datasets/<kb_id>/chunks — the same endpoint the UI uses — so parsing
runs through the normal task executor (Redis queue, embedding, chunk store).

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("parse")


def q_all(conn, sql, params=()):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def resolve_tenant(conn, email):
    rows = q_all(
        conn,
        'SELECT t.id, t.name FROM tenant t JOIN "user" u ON u.id = t.id WHERE u.email = %s',
        (email,),
    )
    if not rows:
        raise SystemExit(f"no tenant found for email {email}")
    return rows[0]


def pick_kbs(conn, args):
    """Knowledge bases to parse: --kb-name patterns (exact or % wildcard),
    scoped to --tenant-email when given; default = every KB holding
    run='0' documents (in scope)."""
    scope, scope_params = "", []
    if args.tenant_email:
        scope = " AND kb.tenant_id = %s"
        scope_params.append(args.tenant_id)
    sql = f"""
        SELECT kb.id, kb.name, kb.tenant_id,
               SUM(CASE WHEN d.run = %s THEN 1 ELSE 0 END) AS unstarted
        FROM knowledgebase kb LEFT JOIN document d ON d.kb_id = kb.id
        WHERE 1 = 1{scope}
        GROUP BY kb.id, kb.name, kb.tenant_id
        HAVING SUM(CASE WHEN d.run = %s THEN 1 ELSE 0 END) > 0
        ORDER BY kb.name
    """
    # placeholder order in the SQL text: SELECT's CASE, WHERE's tenant scope,
    # then HAVING's CASE — params must follow that same order
    rows = q_all(conn, sql, (RUN_UNSTART, *scope_params, RUN_UNSTART))
    if args.kb_name:
        wanted = []
        for pat in args.kb_name:
            op = "LIKE" if ("%" in pat or "_" in pat) else "="
            matched = q_all(
                conn,
                f"""
                SELECT kb.id, kb.name, kb.tenant_id,
                       SUM(CASE WHEN d.run = %s THEN 1 ELSE 0 END) AS unstarted
                FROM knowledgebase kb LEFT JOIN document d ON d.kb_id = kb.id
                WHERE kb.name {op} %s{scope}
                GROUP BY kb.id, kb.name, kb.tenant_id
                """,
                (RUN_UNSTART, pat, *scope_params),
            )
            wanted.extend(matched)
        seen, rows = set(), []
        for r in wanted:
            if r["id"] not in seen:
                seen.add(r["id"])
                rows.append(r)
        missing = [p for p in args.kb_name if not any(p == r["name"] for r in rows) and "%" not in p and "_" not in p]
        if missing:
            raise SystemExit(f"knowledge base(s) not found or without unstarted docs: {missing}")
    return rows


def ensure_token(conn, tenant_id):
    """Idempotent per-tenant API token (composite PK tenant_id+token makes the
    deterministic name stable across runs)."""
    token = "injparse_" + tenant_id[:24]
    with conn.cursor() as cur:
        # Vastbase rejects ON CONFLICT without an explicit conflict target
        cur.execute(
            "INSERT INTO api_token (tenant_id, token, source) VALUES (%s, %s, 'none') ON CONFLICT (tenant_id, token) DO NOTHING",
            (tenant_id, token),
        )
    conn.commit()
    return token


def queue_kb(args, kb, doc_ids, token):
    """POST the parse endpoint in batches; returns (queued_ids, errors)."""
    url = f"{args.api_url.rstrip('/')}/api/v1/datasets/{kb['id']}/chunks"
    headers = {"Authorization": f"Bearer {token}"}
    queued_ids, errors = [], []
    for i in range(0, len(doc_ids), args.batch):
        chunk = doc_ids[i : i + args.batch]
        try:
            resp = requests.post(url, json={"document_ids": chunk}, headers=headers, timeout=120)
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            errors.append(f"{kb['name']}: batch {i // args.batch + 1}: {exc}")
            continue
        if body.get("code") != 0:
            errors.append(f"{kb['name']}: batch {i // args.batch + 1}: {body.get('message', body)}")
            continue
        queued_ids.extend(chunk)
        logger.info(f"  {kb['name']}: queued {len(queued_ids)}/{len(doc_ids)}")
        if args.pause:
            time.sleep(args.pause)
    return queued_ids, errors


def status_counts(conn, kb_id, ids):
    """Run-status histogram for exactly the given document ids."""
    rows = q_all(
        conn,
        "SELECT run, COUNT(*) AS n FROM document WHERE id = ANY(%s) GROUP BY run",
        (ids,),
    )
    return {r["run"]: r["n"] for r in rows}


def wait_done(conn, kbs, queued_by_kb, args):
    """Poll until every THIS-RUN queued doc leaves unstart/running (so a batch
    that failed to queue can never hang the wait); then print failures."""
    waited = {kb["id"]: ids for kb, ids in queued_by_kb.items() if ids}
    started = time.time()
    while True:
        line, active = [], 0
        for kb in kbs:
            ids = waited.get(kb["id"])
            if not ids:
                continue
            c = status_counts(conn, kb["id"], ids)
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
        fails = q_all(
            conn,
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
    ap.add_argument("--kb-name", action="append", default=None, help="KB name, exact or SQL LIKE pattern (repeatable); default: all KBs with unstarted docs")
    ap.add_argument("--batch", type=int, default=5, help="documents per parse API call")
    ap.add_argument("--pause", type=float, default=0.5, help="seconds between API calls")
    ap.add_argument("--wait", action="store_true", help="poll until parsing finishes and report failures")
    ap.add_argument("--interval", type=float, default=10.0, help="poll interval for --wait")
    ap.add_argument("--timeout", type=float, default=0, help="--wait give-up threshold in seconds (0 = wait forever)")
    ap.add_argument("--dry-run", action="store_true", help="list per-KB unstarted counts and exit")
    args = ap.parse_args()

    conn = psycopg2.connect(
        host=args.dst_host,
        port=args.dst_port,
        user=args.dst_user,
        password=args.dst_password,
        dbname=args.dst_meta_db,
    )

    args.tenant_id = resolve_tenant(conn, args.tenant_email)["id"] if args.tenant_email else None
    kbs = pick_kbs(conn, args)
    if not kbs:
        logger.info("no knowledge base has unstarted (run='0') documents — nothing to parse")
        return

    total = sum(k["unstarted"] for k in kbs)
    logger.info(f"{len(kbs)} knowledge base(s), {total} unstarted document(s):")
    for kb in kbs:
        logger.info(f"  {kb['name']}  ({kb['unstarted']} docs, tenant {kb['tenant_id'][:8]}…)")
    if args.dry_run:
        return

    tokens = {t: ensure_token(conn, t) if not args.token else args.token for t in {k["tenant_id"] for k in kbs}}
    all_ok = True
    queued_by_kb = {}
    for kb in kbs:
        doc_ids = [r["id"] for r in q_all(conn, "SELECT id FROM document WHERE kb_id = %s AND run = %s", (kb["id"], RUN_UNSTART))]
        if not doc_ids:
            continue
        logger.info(f"queueing {kb['name']} ({len(doc_ids)} docs, batch {args.batch}) …")
        queued_ids, errors = queue_kb(args, kb, doc_ids, tokens[kb["tenant_id"]])
        queued_by_kb[kb] = queued_ids
        for e in errors:
            logger.error(e)
        all_ok = all_ok and not errors and len(queued_ids) == len(doc_ids)
    if not args.token:
        for tenant_id, token in tokens.items():
            logger.info(f"seeded API token for tenant {tenant_id[:8]}…: {token}")

    if args.wait:
        wait_done(conn, kbs, queued_by_kb, args)
    conn.close()
    if not all_ok:
        sys.exit(1)
    logger.info("PARSE SUMMARY: all requested documents queued" + (" and finished" if args.wait else ""))


if __name__ == "__main__":
    main()
