#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language and the permissions and
#  limitations under the License.
#

import contextlib
import logging
import os
import re
import json
import time
import copy
import threading

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import execute_values
import pandas as pd

from common import settings
from common.constants import PAGERANK_FLD, TAG_FLD
from common.decorator import singleton
from common.doc_store.doc_store_base import (
    DocStoreConnection,
    MatchExpr,
    MatchTextExpr,
    MatchDenseExpr,
    FusionExpr,
    OrderByExpr,
)
from common.file_utils import get_project_base_directory
from common.float_utils import get_float

logger = logging.getLogger('ragflow.vastbase_conn')


def get_table_exists(conn: psycopg2.extensions.connection, table_name: str) -> bool:
    """Get a table exists from a connection."""
    with conn.cursor() as cur:
        cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %s
        )
        """, (table_name,))
        return cur.fetchone()[0]


def get_table_instance(conn: psycopg2.extensions.connection, table_name: str):
    """Get a table columns from a connection."""
    with conn.cursor() as cur:
        check_table_exists_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %s
        )
        """
        logger.debug(f"Checking if table {table_name} exists with SQL: {check_table_exists_sql}")
        cur.execute(check_table_exists_sql, (table_name,))
        table_exists = cur.fetchone()[0]
        if table_exists:
            table_columns_sql = """
            SELECT column_name, data_type, column_default, is_nullable
            FROM information_schema.columns
            WHERE table_name=%s
            """
            logger.debug(f"Fetching columns for table {table_name} with SQL: {table_columns_sql}")
            cur.execute(table_columns_sql, (table_name,))
            return cur.fetchall()
        else:
            return None


def field_keyword(field_name: str):
    # The "docnm_kwd" field is always a string, not list.
    if field_name == "source_id" or (field_name.endswith("_kwd") and field_name != "docnm_kwd" and field_name != "knowledge_graph_kwd"):
        return True
    return False


def equivalent_condition_to_str(condition: dict, table_instance=None) -> str | None:
    assert "_id" not in condition
    clmns = {}
    if table_instance:
        for n, ty, de, _ in table_instance:
            clmns[n] = (ty, de)

    def exists(cln):
        nonlocal clmns
        assert cln in clmns, f"'{cln}' should be in '{clmns}'."
        ty, de = clmns[cln]
        if ty.lower().find("cha"):
            if not de:
                de = ""
            return f" {cln}!='{de}' "
        return f"{cln}!={de}"

    cond = list()
    for k, v in condition.items():
        if not isinstance(k, str) or k in ["kb_id"] or not v:
            continue
        if field_keyword(k):
            pass
        elif isinstance(v, list):
            inCond = list()
            for item in v:
                if isinstance(item, str):
                    item = item.replace("'", "''")
                    inCond.append(f"'{item}'")
                else:
                    inCond.append(str(item))
            if inCond:
                strInCond = ", ".join(inCond)
                strInCond = f"{k} IN ({strInCond})"
                cond.append(strInCond)
        elif k == "must_not":
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if kk == "exists":
                        cond.append("NOT (%s)" % exists(vv))
        elif isinstance(v, str):
            cond.append(f"{k}='{v}'")
        elif k == "exists":
            cond.append(exists(v))
        else:
            cond.append(f"{k}={str(v)}")
    return " AND ".join(cond) if cond else "1=1"


def concat_dataframes(df_list: list[pd.DataFrame], select_fields: list[str]) -> pd.DataFrame:
    df_list2 = [df for df in df_list if not df.empty]
    if df_list2:
        return pd.concat(df_list2, axis=0).reset_index(drop=True)

    schema = []
    for field_name in select_fields:
        if field_name == 'score()':
            schema.append('SCORE')
        elif field_name == 'similarity()':
            schema.append('SIMILARITY')
        else:
            schema.append(field_name)
    return pd.DataFrame(columns=schema)


@singleton
class VBConnection(DocStoreConnection):
    def __init__(self):
        self.dbName = settings.VB.get("db_name", "rag_flow")
        vb_host = settings.VB.get("host", "vastbase")
        vb_port = settings.VB.get("port", 5432)
        vb_user = settings.VB.get("user", "rag_flow")
        vb_password = settings.VB.get("password", "infini_rag_flow")

        self.connPool = None

        logger.info(f"Use Vastbase with floatvector at {vb_host}:{vb_port} as the doc engine.")

        # Try to connect to Vastbase
        for _ in range(24):
            try:
                connPool = pool.ThreadedConnectionPool(
                    minconn=5,
                    maxconn=20,
                    host=vb_host,
                    port=vb_port,
                    user=vb_user,
                    password=vb_password,
                    database=self.dbName,
                    keepalives=1,
                    keepalives_idle=60,
                    keepalives_interval=30,
                    keepalives_count=10,
                )

                # Test connection
                conn = connPool.getconn()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.close()
                finally:
                    if conn:
                        connPool.putconn(conn)
                self.connPool = connPool
                break
            except Exception as e:
                logger.warning(f"{str(e)}. Waiting Vastbase {vb_host}:{vb_port} to be healthy.")
                time.sleep(5)

        if self.connPool is None:
            msg = f"Vastbase {vb_host}:{vb_port} is unhealthy in 120s."
            logger.error(msg)
            raise Exception(msg)

        logger.info(f"Vastbase {vb_host}:{vb_port} is healthy.")

        self._start_pool_health_check()

    def _start_pool_health_check(self):
        """Background thread: check all pool connections every 60 seconds."""
        def _health_check_loop():
            while True:
                time.sleep(60)
                try:
                    self._check_all_connections()
                except Exception as e:
                    logger.warning(f"VASTBASE pool health check error: {e}")

        thread = threading.Thread(target=_health_check_loop, daemon=True, name="vb-pool-health")
        thread.start()
        logger.debug("VASTBASE pool health check thread started (interval=60s).")

    def _check_all_connections(self):
        """Verify every connection in the pool; discard dead ones."""
        max_to_check = 30  # Safety limit above maxconn=20
        checked = 0
        discarded = 0
        for _ in range(max_to_check):
            try:
                conn = self.connPool.getconn()
            except pool.PoolError:
                break
            checked += 1
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                self.connPool.putconn(conn)
            except Exception:
                discarded += 1
                logger.warning(f"VASTBASE pool health check: discarding dead connection ({discarded}/{checked})")
                try:
                    self.connPool.putconn(conn)
                except Exception:
                    pass
        if discarded:
            logger.info(f"VASTBASE pool health check: {checked} checked, {discarded} discarded, "
                        f"current pool size: {checked - discarded}")

    @contextlib.contextmanager
    def get_conn(self):
        """Get a live connection from the pool, retrying up to 3 times."""
        max_attempts = 3
        last_error = None
        for attempt in range(max_attempts):
            conn = self.connPool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            except Exception as e:
                logger.warning(
                    f"VASTBASE dead connection discarded (attempt {attempt + 1}/{max_attempts}): {e}"
                )
                last_error = e
                try:
                    self.connPool.putconn(conn)
                except Exception:
                    pass
                if attempt < max_attempts - 1:
                    time.sleep(0.1)
                continue
            try:
                yield conn
            except Exception as e:
                logger.error(f"Error in Vastbase connection: {str(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                try:
                    self.connPool.putconn(conn)
                except Exception:
                    pass
            return
        raise ConnectionError(
            f"VASTBASE failed to get a live connection after {max_attempts} attempts. "
            f"Last error: {last_error}"
        )

    """
    Database operations
    """

    def db_type(self) -> str:
        return "vastbase"

    def health(self) -> dict:
        """Return the health status of the database."""
        with self.get_conn() as vb_conn:
            try:
                with vb_conn.cursor() as cur:
                    cur.execute("SELECT vb_version()")
                    cur.fetchone()
                res = {
                    "type": "vastbase",
                    "status": "green",
                    "error": ""
                }
            except Exception as e:
                res = {
                    "type": "vastbase",
                    "status": "red",
                    "error": str(e)
                }
            return res

    """
    Table operations
    """

    def create_idx(self, index_name: str, dataset_id: str, vector_size: int, parser_id: str = None):
        """Create a table and necessary indexes for vector storage"""
        table_name = f"{index_name}_{dataset_id}"
        with self.get_conn() as vb_conn:
            fp_mapping = os.path.join(
                get_project_base_directory(), "conf", "vastbase_mapping.json"
            )
            if not os.path.exists(fp_mapping):
                raise Exception(f"Mapping file not found at {fp_mapping}")
            schema = json.load(open(fp_mapping))
            vector_name = f"q_{vector_size}_vec"

            columns = []
            # Process field definitions from mapping
            for field_name, field_info in schema.items():
                field_type = field_info["type"]
                field_default = field_info['default']
                columns.append(sql.SQL("{field_name} {field_type} DEFAULT {field_default}").format(
                    field_name=sql.Identifier(field_name),
                    field_type=sql.SQL(field_type),
                    field_default=sql.Literal(field_default)
                ))

            # Add vector field
            columns.append(sql.SQL("{vector_name} floatvector({vectorSize})").format(
                vector_name=sql.Identifier(vector_name),
                vectorSize=sql.SQL(str(vector_size))
            ))

            # Create table
            create_table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns}
            )
            """).format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(columns)
            )

            with vb_conn.cursor() as cur:
                cur.execute(create_table_sql)
                logging.debug(f"VASTBASE create table SQL: {create_table_sql.as_string(vb_conn)}")
                # Create vector index using HNSW (Hierarchical Navigable Small World) index
                create_q_vex_idx_sql = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table_name} USING hnsw ({vector_name} floatvector_cosine_ops)
                WITH (m=16, ef_construction=50)
                """).format(
                    index_name=sql.Identifier(f'q_vec_idx_{table_name}'),
                    table_name=sql.Identifier(table_name),
                    vector_name=sql.Identifier(vector_name)
                )
                logger.debug(f"VASTBASE create vector index SQL: {create_q_vex_idx_sql.as_string(vb_conn)}")
                cur.execute(create_q_vex_idx_sql)
                vb_conn.commit()

                # Create full-text indexes — try both PG GIN and MySQL FULLTEXT syntax.
                # Done after commit so failures don't roll back the table + vector index.
                text_idx_fields = [
                    "title_tks",
                    "title_sm_tks",
                    "important_kwd",
                    "important_tks",
                    "question_tks",
                    "content_ltks",
                    "content_sm_ltks"
                ]
                # Try PG-compatible syntax first (GIN + to_tsvector)
                pg_fts_ok = False
                try:
                    field_list = sql.SQL(', ').join(
                        sql.SQL("to_tsvector('cn_tokenizer', {})").format(sql.Identifier(f))
                        for f in text_idx_fields
                    )
                    pg_fts_sql = sql.SQL("""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {table_name} USING gin({field_list})
                    """).format(
                        index_name=sql.Identifier(f'text_gin_idx_{table_name}'),
                        table_name=sql.Identifier(table_name),
                        field_list=field_list
                    )
                    logging.debug(f"VASTBASE create PG fulltext index SQL: {pg_fts_sql.as_string(vb_conn)}")
                    cur.execute(pg_fts_sql)
                    pg_fts_ok = True
                except Exception as e:
                    logging.warning(f"PG GIN fulltext index failed, trying MySQL syntax: {e}")
                    # vb_conn.rollback()

                if not pg_fts_ok:
                    # Fallback: MySQL-compatible FULLTEXT index per field.
                    # VB's MySQL mode supports ALTER TABLE ... ADD FULLTEXT INDEX.
                    for f in text_idx_fields:
                        try:
                            mysql_fts_sql = sql.SQL("""
                                ALTER TABLE {table_name}
                                ADD FULLTEXT INDEX {index_name} ({field_name})
                            """).format(
                                table_name=sql.Identifier(table_name),
                                index_name=sql.Identifier(f'{f}_fulltext_idx_{table_name}'),
                                field_name=sql.Identifier(f)
                            )
                            logging.debug(f"VASTBASE create MySQL fulltext index SQL: {mysql_fts_sql.as_string(vb_conn)}")
                            cur.execute(mysql_fts_sql)
                        except Exception as e2:
                            logging.warning(
                                f"Failed to create fulltext index for {f}: {e2}, "
                                f"vector search will work without it"
                            )
                            vb_conn.rollback()
                vb_conn.commit()
        logger.info(
            f"VASTBASE created table {table_name}, vector size {vector_size}"
        )

    def delete_idx(self, index_name: str, dataset_id: str):
        """Drop the table for the given index and knowledgebase"""
        table_name = f"{index_name}_{dataset_id}"
        with self.get_conn() as vb_conn:
            with vb_conn.cursor() as cur:
                drop_index_sql = sql.SQL("DROP TABLE IF EXISTS {table_name}").format(
                    table_name=sql.Identifier(table_name)
                )
                logger.debug(f"VASTBASE drop table SQL: {drop_index_sql.as_string(vb_conn)}")
                cur.execute(drop_index_sql)
                vb_conn.commit()
        logger.info(f"VASTBASE dropped table {table_name}")

    def create_doc_meta_idx(self, index_name: str):
        """
        Create a document metadata table.

        Table name pattern: ragflow_doc_meta_{tenant_id}
        - Per-tenant metadata table for storing document metadata fields
        """
        table_name = index_name
        with self.get_conn() as vb_conn:
            if get_table_exists(vb_conn, table_name):
                return True
            fp_mapping = os.path.join(
                get_project_base_directory(), "conf", "doc_meta_vastbase_mapping.json"
            )
            if not os.path.exists(fp_mapping):
                logger.error(f"Document metadata mapping file not found at {fp_mapping}")
                return False
            schema = json.load(open(fp_mapping))
            columns = []
            for field_name, field_info in schema.items():
                field_type = field_info["type"]
                field_default = field_info['default']
                columns.append(sql.SQL("{field_name} {field_type} DEFAULT {field_default}").format(
                    field_name=sql.Identifier(field_name),
                    field_type=sql.SQL(field_type),
                    field_default=sql.Literal(field_default)
                ))
            create_table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns}
            )
            """).format(
                table_name=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(columns)
            )
            with vb_conn.cursor() as cur:
                cur.execute(create_table_sql)
                logger.debug(f"VASTBASE create doc_meta table SQL: {create_table_sql.as_string(vb_conn)}")
                vb_conn.commit()
            logger.info(f"VASTBASE created document metadata table {table_name}")
            return True

    def index_exist(self, index_name: str, dataset_id: str) -> bool:
        """Check if the table exists for the given index and knowledgebase"""
        table_name = f"{index_name}_{dataset_id}"
        with self.get_conn() as vb_conn:
            exists = get_table_exists(vb_conn, table_name)
            return exists

    """
    CRUD operations
    """

    def search(
            self, select_fields: list[str],
            highlight_fields: list[str],
            condition: dict,
            match_expressions: list[MatchExpr],
            order_by: OrderByExpr,
            offset: int,
            limit: int,
            index_names: str | list[str],
            knowledgebase_ids: list[str],
            agg_fields: list[str] = [],
            rank_feature: dict | None = None
    ) -> tuple[pd.DataFrame, int]:
        """
        TODO: Vastbase doesn't provide highlight
        """
        if isinstance(index_names, str):
            index_names = index_names.split(",")
        assert isinstance(index_names, list) and len(index_names) > 0
        with self.get_conn() as vb_conn:
            df_list = list()
            table_list = list()
            output = select_fields.copy()
            for essential_field in ["id"]:
                if essential_field not in select_fields:
                    output.append(essential_field)
            score_func = ""
            score_column = ""
            for matchExpr in match_expressions:
                if isinstance(matchExpr, MatchTextExpr):
                    score_func = "score()"
                    score_column = "SCORE"
                    break
            if not score_func:
                for matchExpr in match_expressions:
                    if isinstance(matchExpr, MatchDenseExpr):
                        score_func = "similarity()"
                        score_column = "SIMILARITY"
                        break
            if match_expressions:
                if PAGERANK_FLD not in output:
                    output.append(PAGERANK_FLD)
            output = [f for f in output if f not in ["_score", "row_id()"]]

            # Prepare expressions common to all tables
            filter_cond = None
            filter_fulltext = None
            filter_vector = None
            if condition:
                for indexName in index_names:
                    table_name = f"{indexName}_{knowledgebase_ids[0]}"
                    table_instance = get_table_instance(vb_conn, table_name)
                    if table_instance:
                        filter_cond = equivalent_condition_to_str(condition, table_instance)
                        break

            vector_similarity_weight = 0.5
            for matchExpr in match_expressions:
                if isinstance(matchExpr, MatchTextExpr):
                    minimum_should_match = matchExpr.extra_options.get("minimum_should_match", 0.0)
                    if isinstance(minimum_should_match, float):
                        minimum_should_match = str(int(minimum_should_match * 100)) + "%"
                    if filter_cond and "filter" not in matchExpr.extra_options:
                        matchExpr.extra_options.update({"filter": filter_cond})
                    pattern = r'[~^]\d+(?:\.\d+)?'
                    matching_text = re.sub(pattern, '', matchExpr.matching_text)
                    fields = list()
                    pattern = r'^(.+?)(?:\^(\d+(?:\.\d+)?))?$'
                    for field in matchExpr.fields:
                        match = re.match(pattern, field)
                        if match:
                            field_name = match.group(1)
                            field_weight = match.group(2) if match.group(2) else "1"
                            fields.append((field_name, field_weight))
                    if fields:
                        filter_fulltext = sql.SQL(' AND ').join([sql.SQL("{column} @~@ {matching_text}").format(
                            column=sql.Identifier(field_name),
                            matching_text=sql.Literal(f"{matching_text} @<PARAMS:MINIMUM_SHOULD_MATCH={minimum_should_match} PARAMS:BOOST={field_weight}>@")
                        ) for field_name, field_weight in fields])
                        if filter_cond:
                            filter_fulltext = sql.SQL("({filter_cond}) AND ({filter_fulltext})").format(
                                filter_cond=sql.SQL(filter_cond),
                                filter_fulltext=filter_fulltext
                            )
                    logger.debug(f"VASTBASE search MatchTextExpr: {json.dumps(matchExpr.__dict__)}")
                elif isinstance(matchExpr, MatchDenseExpr):
                    similarity = matchExpr.extra_options.get("similarity")
                    if similarity is not None:
                        filter_vector = sql.SQL("1 - ({vec_col} <=> {vec}) >= {similarity}").format(
                            vec_col=sql.Identifier(matchExpr.vector_column_name),
                            vec=sql.Literal([float(v) for v in matchExpr.embedding_data]),
                            similarity=sql.Literal(similarity),
                        )
                    logger.debug(f"VASTBASE search MatchDenseExpr: {json.dumps(matchExpr.__dict__)}")
                elif isinstance(matchExpr, FusionExpr):
                    if isinstance(matchExpr, FusionExpr) and matchExpr.method == "weighted_sum" and "weights" in matchExpr.fusion_params:
                        assert len(match_expressions) == 3 and isinstance(match_expressions[0], MatchTextExpr) and isinstance(
                            match_expressions[1],
                            MatchDenseExpr) and isinstance(
                            match_expressions[2], FusionExpr)
                        weights = matchExpr.fusion_params["weights"]
                        vector_similarity_weight = float(weights.split(",")[1])
                    logger.debug(f"VASTBASE search FusionExpr: {json.dumps(matchExpr.__dict__)}")

            order_by_expr_list = list()
            if order_by.fields:
                for order_field in order_by.fields:
                    if order_field[1] == 0:
                        order_by_expr_list.append((order_field[0], "ASC"))
                    else:
                        order_by_expr_list.append((order_field[0], "DESC"))

            total_hits_count = 0
            # Scatter search tables and gather the results
            for indexName in index_names:
                for knowledgebaseId in knowledgebase_ids:
                    table_name = f"{indexName}_{knowledgebaseId}"
                    try:
                        table_exists = get_table_exists(vb_conn, table_name)
                        if not table_exists:
                            logger.warning(f"Table {table_name} not found, skipping...")
                            continue
                    except Exception:
                        logger.warning(f"Error checking table {table_name}, skipping...")
                        continue
                    table_list.append(table_name)
                    select_fields_sql = sql.SQL(', ').join([sql.Identifier(field) for field in output])
                    sql_expr = None
                    filter_fulltext_expr = None
                    filter_vector_expr = None
                    if len(match_expressions) > 0:
                        for matchExpr in match_expressions:
                            if isinstance(matchExpr, MatchTextExpr):
                                if filter_fulltext is None:
                                    continue
                                filter_fulltext_expr = sql.SQL("""
                                SELECT {select_fields}, (bm25_score/MAX(bm25_score) OVER()) as "SCORE"
                                FROM (SELECT {select_fields}, bm25_score() as bm25_score
                                FROM {table_name}
                                WHERE {filter_fulltext}
                                ORDER BY bm25_score DESC
                                LIMIT {limit})
                                """).format(
                                    select_fields=select_fields_sql,
                                    table_name=sql.Identifier(table_name),
                                    filter_fulltext=filter_fulltext,
                                    limit=sql.Literal(matchExpr.topn)
                                )
                                sql_expr = filter_fulltext_expr
                            elif isinstance(matchExpr, MatchDenseExpr):
                                if filter_vector is None:
                                    continue
                                filter_vector_expr = sql.SQL("""
                                SELECT {select_fields}, (1-({vec_col}<=>{vec})) AS "SIMILARITY"
                                FROM {table_name}
                                WHERE {filter_vector}
                                ORDER BY {vec_col}<=>{vec}
                                LIMIT {limit}
                                """).format(
                                    select_fields=select_fields_sql,
                                    vec_col=sql.Identifier(matchExpr.vector_column_name),
                                    vec=sql.Literal([float(v) for v in matchExpr.embedding_data]),
                                    table_name=sql.Identifier(table_name),
                                    filter_vector=filter_vector,
                                    limit=sql.Literal(matchExpr.topn)
                                )
                                if not sql_expr:
                                    sql_expr = filter_vector_expr
                            elif isinstance(matchExpr, FusionExpr):
                                sql_expr = sql.SQL("""
                                WITH filter_fulltext AS ({filter_fulltext_expr}),
                                     filter_vector AS ({filter_vector_expr})
                                SELECT {select_fields}, (COALESCE(a."SCORE", 0) * {fulltext_weight} + COALESCE(b."SIMILARITY", 0) * {vector_similarity_weight}) AS "SCORE"
                                FROM filter_fulltext a
                                FULL OUTER JOIN filter_vector b
                                ON a.id = b.id
                                ORDER BY (COALESCE(a."SCORE", 0) * {fulltext_weight} + COALESCE(b."SIMILARITY", 0) * {vector_similarity_weight}) DESC
                                LIMIT {limit}
                                """).format(
                                    filter_fulltext_expr=filter_fulltext_expr,
                                    filter_vector_expr=filter_vector_expr,
                                    select_fields=sql.SQL(', ').join([sql.SQL("COALESCE(a.{field},b.{field}) AS {field}").format(
                                        field=sql.Identifier(field)
                                        ) for field in output]),
                                    fulltext_weight=sql.Literal(1 - vector_similarity_weight),
                                    vector_similarity_weight=sql.Literal(vector_similarity_weight),
                                    score_column=sql.Identifier(score_column),
                                    limit=sql.Literal(matchExpr.topn)
                                )
                    else:
                        if filter_cond and len(filter_cond) > 0:
                            sql_expr = sql.SQL("""
                            SELECT {select_fields}
                            FROM {table_name}
                            WHERE {filter_clause}
                            """).format(
                                select_fields=select_fields_sql,
                                table_name=sql.Identifier(table_name),
                                filter_clause=sql.SQL(filter_cond)
                            )
                    sql_query = sql.SQL("""
                    SELECT *
                    FROM ({sub_query})
                    """).format(
                        sub_query=sql_expr
                    )
                    if order_by.fields:
                        sql_query = sql.SQL("""
                        {sql_query}
                        ORDER BY {order_clause}
                        """).format(
                            sql_query=sql_query,
                            order_clause=sql.SQL(', ').join([sql.SQL('{field} {sort}').format(
                                field=sql.Identifier(field),
                                sort=sql.SQL(sort)
                            ) for field, sort in order_by_expr_list])
                        )
                    sql_query = sql.SQL("""
                    {sql_query}
                    LIMIT {limit} OFFSET {offset}""").format(
                        sql_query=sql_query,
                        limit=sql.Literal(limit),
                        offset=sql.Literal(offset)
                    )
                    with vb_conn.cursor() as cur:
                        logger.debug(f"Executing SQL query: {sql_query.as_string(vb_conn)}")
                        cur.execute(sql_query)
                        column_names = [desc[0] for desc in cur.description]
                        rows = cur.fetchall()
                        if rows:
                            total_hits_count += cur.rowcount
                        kb_res = pd.DataFrame(rows, columns=column_names)
                        logger.debug(f"VASTBASE search table: {str(table_list)}, result: {str(kb_res)}")
                        df_list.append(kb_res)

        res = concat_dataframes(df_list, output)
        if match_expressions:
            res['Sum'] = res[score_column] + res[PAGERANK_FLD]
            res = res.sort_values(by='Sum', ascending=False).reset_index(drop=True).drop(columns=['Sum'])
            res = res.head(limit)
        logger.debug(f"VASTBASE search final result: {str(res)}")
        return res, total_hits_count

    def get(
            self, data_id: str, index_name: str, dataset_ids: list[str]
    ) -> dict | None:
        with self.get_conn() as vb_conn:
            df_list = list()
            assert isinstance(dataset_ids, list)
            table_list = list()
            for knowledgebaseId in dataset_ids:
                table_name = f"{index_name}_{knowledgebaseId}"
                table_list.append(table_name)
                table_exists = get_table_exists(vb_conn, table_name)
                if not table_exists:
                    logger.warning(
                        f"Table not found: {table_name}, this knowledge base isn't created in Vastbase. Maybe it is created in other document engine.")
                    continue
                with vb_conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT * FROM {table_name} WHERE id = %s").format(
                        table_name=sql.Identifier(table_name)
                    ), (data_id,))
                    column_names = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                kb_res = pd.DataFrame(rows, columns=column_names)
                logger.debug(f"VASTBASE get table: {str(table_list)}, result: {str(kb_res)}")
                df_list.append(kb_res)
            res = concat_dataframes(df_list, ["id"])
            res_fields = self.get_fields(res, res.columns.tolist())
            return res_fields.get(data_id, None)

    def insert(
            self, documents: list[dict], index_name: str, dataset_id: str = None
    ) -> list[str]:
        with self.get_conn() as vb_conn:
            table_name = f"{index_name}_{dataset_id}"
            table_instance = get_table_instance(vb_conn, table_name)
            if not table_instance:
                # Need to create the table
                vector_size = 0
                patt = re.compile(r"q_(?P<vector_size>\d+)_vec")
                for k in documents[0].keys():
                    m = patt.match(k)
                    if m:
                        vector_size = int(m.group("vector_size"))
                        break
                if vector_size == 0:
                    raise ValueError("Cannot infer vector size from documents")
                self.create_idx(index_name, dataset_id, vector_size)
                table_instance = get_table_instance(vb_conn, table_name)

            if not table_instance:
                raise ValueError(f"Table {table_name} does not exist in Vastbase.")
            # embedding fields can't have a default value....
            embedding_clmns = []
            for n, ty, _, _ in table_instance:
                r = re.search(r"Embedding\([a-z]+,([0-9]+)\)", ty)
                if not r:
                    continue
                embedding_clmns.append((n, int(r.group(1))))

            docs = copy.deepcopy(documents)
            for d in docs:
                assert "_id" not in d
                assert "id" in d
                for k, v in d.items():
                    if field_keyword(k):
                        if isinstance(v, list):
                            d[k] = "###".join(v)
                        else:
                            d[k] = v
                    elif re.search(r"_feas$", k):
                        d[k] = json.dumps(v)
                    elif k == 'kb_id':
                        if isinstance(d[k], list):
                            d[k] = d[k][0]
                    elif k == "position_int":
                        assert isinstance(v, list)
                        arr = [num for row in v for num in row]
                        d[k] = "_".join(f"{num:08x}" for num in arr)
                    elif k in ["page_num_int", "top_int"]:
                        assert isinstance(v, list)
                        d[k] = "_".join(f"{num:08x}" for num in v)
                    else:
                        d[k] = v

                    for n, vs in embedding_clmns:
                        if n in d:
                            continue
                        d[n] = [0] * vs
            ids = [d["id"] for d in docs]
            with vb_conn.cursor() as cur:
                cur.execute(sql.SQL("DELETE FROM {} WHERE id IN %s").format(
                    sql.Identifier(table_name)
                ), (tuple(ids),))
                column_names = list(docs[0].keys())
                values = [tuple(doc.get(col, None) for col in column_names) for doc in docs]
                insert_sql = sql.SQL("INSERT INTO {table_name} ({column_names}) VALUES %s").format(
                    table_name=sql.Identifier(table_name),
                    column_names=sql.SQL(', ').join([sql.Identifier(col) for col in column_names])
                )
                execute_values(cur, insert_sql, values)
                vb_conn.commit()
            logger.debug(f"VASTBASE inserted into {table_name} {ids}.")
            return []

    def update(
            self, condition: dict, new_value: dict, index_name: str, dataset_id: str
    ) -> bool:
        with self.get_conn() as vb_conn:
            table_name = f"{index_name}_{dataset_id}"
            table_instance = get_table_instance(vb_conn, table_name)

            clmns = {}
            if table_instance:
                for n, ty, de, _ in table_instance:
                    clmns[n] = (ty, de)
            filter = equivalent_condition_to_str(condition, table_instance)
            removeValue = {}
            for k, v in list(new_value.items()):
                if field_keyword(k):
                    if isinstance(v, list):
                        new_value[k] = "###".join(v)
                    else:
                        new_value[k] = v
                elif re.search(r"_feas$", k):
                    new_value[k] = json.dumps(v)
                elif k == 'kb_id':
                    if isinstance(new_value[k], list):
                        new_value[k] = new_value[k][0]
                elif k == "position_int":
                    assert isinstance(v, list)
                    arr = [num for row in v for num in row]
                    new_value[k] = "_".join(f"{num:08x}" for num in arr)
                elif k in ["page_num_int", "top_int"]:
                    assert isinstance(v, list)
                    new_value[k] = "_".join(f"{num:08x}" for num in v)
                elif k == "remove":
                    if isinstance(v, str):
                        assert v in clmns, f"'{v}' should be in '{clmns}'."
                        ty, de = clmns[v]
                        if ty.lower().find("cha"):
                            if not de:
                                de = ""
                        new_value[v] = de
                    else:
                        for kk, vv in v.items():
                            removeValue[kk] = vv
                        del new_value[k]
                else:
                    new_value[k] = v

            remove_opt = {}
            with vb_conn.cursor() as cur:
                if removeValue:
                    col_to_remove = list(removeValue.keys())
                    col_to_remove.append('id')
                    cur.execute(sql.SQL("SELECT {columns} FROM {table_name} WHERE {filter_clause}").format(
                        columns=sql.SQL(', ').join([sql.Identifier(col) for col in col_to_remove]),
                        table_name=sql.Identifier(table_name),
                        filter_clause=sql.SQL(filter)
                    ))
                    column_names = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    row_to_opt = pd.DataFrame(rows, columns=column_names)
                    logger.debug(f"VASTBASE search table {str(table_name)}, filter {filter}, result: {str(row_to_opt[0])}")
                    row_to_opt = self.get_fields(row_to_opt, col_to_remove)
                    for id, old_v in row_to_opt.items():
                        for k, remove_v in removeValue.items():
                            if remove_v in old_v[k]:
                                new_v = old_v[k].copy()
                                new_v.remove(remove_v)
                                kv_key = json.dumps([k, new_v])
                                if kv_key not in remove_opt:
                                    remove_opt[kv_key] = [id]
                                else:
                                    remove_opt[kv_key].append(id)

                logger.debug(f"VASTBASE update table {table_name}, filter {filter}, newValue {new_value}.")
                for update_kv, ids in remove_opt.items():
                    k, v = json.loads(update_kv)
                    cur.execute(sql.SQL("UPDATE {table_name} SET {k}=%s WHERE {filter_clause} AND id in %s").format(
                        table_name=sql.Identifier(table_name),
                        k=sql.Identifier(k),
                        filter_clause=sql.SQL(filter)
                    ), ("###".join(v), tuple(ids)))

                cur.execute(sql.SQL("UPDATE {table_name} SET {set_clause} WHERE {filter_clause}").format(
                    table_name=sql.Identifier(table_name),
                    set_clause=sql.SQL(', ').join([sql.SQL("{k}={v}").format(
                        k=sql.Identifier(k),
                        v=sql.Literal(v)
                    ) for k, v in new_value.items()]),
                    filter_clause=sql.SQL(filter)
                ))
                vb_conn.commit()
            return True

    def delete(self, condition: dict, index_name: str, dataset_id: str) -> int:
        with self.get_conn() as vb_conn:
            table_name = f"{index_name}_{dataset_id}"
            table_instance = get_table_instance(vb_conn, table_name)
            if not table_instance:
                logger.warning(f"Skipped deleting from table {table_name} since the table doesn't exist.")
                return 0
            filter = equivalent_condition_to_str(condition, table_instance)
            logger.debug(f"VASTBASE delete table {table_name}, filter {filter}.")
            with vb_conn.cursor() as cur:
                cur.execute(sql.SQL("DELETE FROM {table_name} WHERE {filter_clause}").format(
                    table_name=sql.Identifier(table_name),
                    filter_clause=sql.SQL(filter)
                ))
                deleted_rows = cur.rowcount
                vb_conn.commit()
            return deleted_rows

    """
    Helper functions for search result
    """

    def get_total(self, res: tuple[pd.DataFrame, int] | pd.DataFrame) -> int:
        if isinstance(res, tuple):
            return res[1]
        return len(res)

    def get_scores(self, res: tuple[pd.DataFrame, int] | pd.DataFrame) -> dict[str, float]:
        """
        Map chunk id to its similarity score from a Vastbase search result.
        The score is stored in the SIMILARITY column when a MatchDenseExpr
        is used (e.g. by _knn_scores).
        """
        if isinstance(res, tuple):
            res = res[0]
        if res.empty:
            return {}
        if "SIMILARITY" in res.columns:
            return dict(zip(res["id"], res["SIMILARITY"].fillna(0.0).astype(float)))
        return {row["id"]: 0.0 for _, row in res.iterrows()}

    def get_doc_ids(self, res: tuple[pd.DataFrame, int] | pd.DataFrame) -> list[str]:
        if isinstance(res, tuple):
            res = res[0]
        return list(res["id"])

    def get_fields(self, res: tuple[pd.DataFrame, int] | pd.DataFrame, fields: list[str]) -> dict[str, dict]:
        if isinstance(res, tuple):
            res = res[0]
        if not fields:
            return {}
        fieldsAll = fields.copy()
        fieldsAll.append('id')
        column_map = {col.lower(): col for col in res.columns}
        # Map _score to the actual score column in Vastbase results
        score_aliases = {"_score": ["score", "similarity"]}
        for alias_field, candidates in score_aliases.items():
            if alias_field in fieldsAll and alias_field not in column_map:
                for c in candidates:
                    if c in column_map:
                        column_map[alias_field] = column_map[c]
                        break
        matched_columns = {column_map[col.lower()]: col for col in set(fieldsAll) if col.lower() in column_map}
        none_columns = [col for col in set(fieldsAll) if col.lower() not in column_map]

        res2 = res[matched_columns.keys()]
        res2 = res2.rename(columns=matched_columns)
        res2.drop_duplicates(subset=['id'], inplace=True)

        for column in res2.columns:
            if res2[column] is None:
                res2[column] = ""
                continue
            k = column.lower()
            if field_keyword(k):
                res2[column] = res2[column].apply(lambda v: [kwd for kwd in (v or '').split("###") if kwd])
            elif k == "position_int":
                def to_position_int(v):
                    if v:
                        arr = [int(hex_val, 16) for hex_val in v.split('_')]
                        v = [arr[i:i + 5] for i in range(0, len(arr), 5)]
                    else:
                        v = []
                    return v
                res2[column] = res2[column].apply(to_position_int)
            elif k in ["page_num_int", "top_int"]:
                res2[column] = res2[column].apply(lambda v: [int(hex_val, 16) for hex_val in v.split('_')] if v else [])
            else:
                pass
        for column in none_columns:
            res2[column] = None

        return res2.set_index("id").to_dict(orient="index")

    def get_highlight(self, res: tuple[pd.DataFrame, int] | pd.DataFrame, keywords: list[str], fieldnm: str):
        if isinstance(res, tuple):
            res = res[0]
        ans = {}
        num_rows = len(res)
        column_id = res["id"]
        if fieldnm not in res:
            return {}
        for i in range(num_rows):
            id = column_id[i]
            txt = res[fieldnm][i]
            txt = re.sub(r"[\r\n]", " ", txt, flags=re.IGNORECASE | re.MULTILINE)
            txts = []
            for t in re.split(r"[.?!;\n]", txt):
                for w in keywords:
                    t = re.sub(
                        r"(^|[ .?/'\"\(\)!,:;-])(%s)([ .?/'\"\(\)!,:;-])"
                        % re.escape(w),
                        r"\1<em>\2</em>\3",
                        t,
                        flags=re.IGNORECASE | re.MULTILINE,
                    )
                if not re.search(
                        r"<em>[^<>]+</em>", t, flags=re.IGNORECASE | re.MULTILINE
                ):
                    continue
                txts.append(t)
            ans[id] = "...".join(txts)
        return ans

    def get_aggregation(self, res: tuple[pd.DataFrame, int] | pd.DataFrame, fieldnm: str):
        """
        TODO: Vastbase doesn't provide aggregation
        """
        return list()

    """
    SQL
    """

    def sql(self, sql: str, fetch_size: int, format: str):
        raise NotImplementedError("Not implemented")
