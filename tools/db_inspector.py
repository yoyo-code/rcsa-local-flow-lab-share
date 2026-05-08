from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg import sql


TABLES = [
    "business_processes",
    "subprocess_runs",
    "processing_tasks",
    "task_inputs",
    "documents",
    "document_units",
    "integration_events",
    "interview_job_runs",
    "interview_job_events",
    "interview_job_results",
    "interview_plan_runs",
    "interview_plan_tracks",
    "interview_plan_steps",
    "active_interview_plans",
    "interview_launches",
    "voice_agent_workflow_runs",
    "voice_agent_step_runs",
    "voice_agent_sessions",
    "voice_agent_transcripts",
    "voice_agent_audio_files",
]

MATCH_COLUMNS = [
    "id",
    "code",
    "process_id",
    "document_id",
    "subprocess_run_id",
    "subprocess_code",
    "preprocessor_run_id",
    "correlation_id",
    "job_id",
    "source_job_id",
    "plan_run_id",
    "result_plan_run_id",
    "process_code",
    "launch_token",
    "workflow_run_id",
    "session_id",
]

ORDER_COLUMNS = [
    "updated_at",
    "created_at",
    "occurred_at",
    "completed_at",
    "started_at",
    "id",
]

def main() -> None:
    db_url = os.getenv("FLOW_LAB_DATABASE_URL")
    if not db_url:
        print(json.dumps({"available": False, "error": "FLOW_LAB_DATABASE_URL no configurado"}))
        return

    payload = json.loads(sys.stdin.read() or "{}")
    identifiers = normalize_identifiers(payload)

    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        summary = {
            "available": True,
            "database": redact_db_url(db_url),
            "identifiers": identifiers,
            "tables": [],
        }
        with connection.cursor() as cursor:
            existing = existing_tables(cursor)
            columns_by_table = {table: table_columns(cursor, table) for table in existing}
            identifiers = derive_related_identifiers(cursor, existing, columns_by_table, identifiers)
            summary["identifiers"] = identifiers
            for table in TABLES:
                if table not in existing:
                    summary["tables"].append({"table": table, "exists": False})
                    continue
                columns = columns_by_table[table]
                where_sql, params = build_where(table, columns, identifiers)
                total_count = count_rows(cursor, table, None, {})
                matched_count = count_rows(cursor, table, where_sql, params)
                rows = fetch_rows(cursor, table, columns, where_sql, params)
                summary["tables"].append(
                    {
                        "table": table,
                        "exists": True,
                        "totalCount": total_count,
                        "matchedCount": matched_count,
                        "matchedBy": [column for column in MATCH_COLUMNS if column in columns],
                        "rows": rows,
                    }
                )
            summary["artifacts"] = fetch_readable_artifacts(cursor, existing, columns_by_table, identifiers)
        summary["hasExistingData"] = any(
            table.get("exists")
            and table.get("matchedCount", 0) > 0
            and table.get("table")
            in {
                "subprocess_runs",
                "documents",
                "document_units",
                "interview_job_runs",
                "interview_plan_runs",
                "interview_launches",
            }
            for table in summary["tables"]
        )
    print(json.dumps(summary, default=to_json, ensure_ascii=True))


def fetch_readable_artifacts(cursor, existing: set[str], columns_by_table: dict[str, set[str]], identifiers: dict) -> dict:
    return {
        "documents": fetch_document_markdown_artifacts(cursor, existing, columns_by_table, identifiers),
        "planner": fetch_planner_artifacts(cursor, existing, columns_by_table, identifiers),
    }


def fetch_document_markdown_artifacts(
    cursor,
    existing: set[str],
    columns_by_table: dict[str, set[str]],
    identifiers: dict,
) -> list[dict]:
    if "documents" not in existing:
        return []
    columns = columns_by_table.get("documents", set())
    primary_run_ids = identifier_values(identifiers, "preprocessorRunId")
    run_ids = primary_run_ids or identifier_values(identifiers, "preprocessorRunIds")
    criteria = [("subprocess_run_id", run_ids)]
    if not unique_values(run_ids):
        criteria.append(("process_id", identifier_values(identifiers, "processId") + identifier_values(identifiers, "processIds")))
    where_sql, params = build_simple_where(columns, criteria)
    if where_sql is None:
        return []

    cursor.execute(
        sql.SQL(
            """
            SELECT
              id::text,
              subprocess_run_id::text,
              file_name,
              document_role,
              processing_status,
              parse_strategy,
              page_count,
              unit_count,
              length(coalesce(markdown_combined, '')) AS markdown_chars,
              coalesce(markdown_combined, '') AS markdown_excerpt,
              created_at,
              updated_at
            FROM public.documents
            WHERE 
            """
        )
        + where_sql
        + sql.SQL(" ORDER BY document_role NULLS LAST, file_name NULLS LAST"),
        params,
    )
    documents = []
    for row in cursor.fetchall():
        document = dict(row)
        document["units"] = fetch_document_unit_artifacts(cursor, document["id"])
        documents.append(document)
    return documents


def fetch_document_unit_artifacts(cursor, document_id: str) -> list[dict]:
    cursor.execute(
        sql.SQL(
            """
            SELECT
              unit_number,
              unit_label,
              page_number,
              has_images,
              has_tables,
              llm_used,
              length(coalesce(markdown_text, '')) AS markdown_chars,
              coalesce(markdown_text, '') AS markdown_excerpt
            FROM public.document_units
            WHERE document_id = %(document_id)s
            ORDER BY unit_number NULLS LAST, page_number NULLS LAST
            """
        ),
        {"document_id": document_id},
    )
    return [dict(row) for row in cursor.fetchall()]


def fetch_planner_artifacts(
    cursor,
    existing: set[str],
    columns_by_table: dict[str, set[str]],
    identifiers: dict,
) -> dict:
    job_ids = unique_values(identifier_values(identifiers, "plannerJobId") or identifier_values(identifiers, "plannerJobIds"))
    plan_run_ids = unique_values(
        identifier_values(identifiers, "plannerPlanRunId") or identifier_values(identifiers, "plannerPlanRunIds")
    )
    return {
        "jobs": fetch_planner_job_artifacts(cursor, existing, columns_by_table, job_ids),
        "plans": fetch_plan_run_artifacts(cursor, existing, columns_by_table, plan_run_ids),
    }


def fetch_planner_job_artifacts(
    cursor,
    existing: set[str],
    columns_by_table: dict[str, set[str]],
    job_ids: list[str],
) -> list[dict]:
    if "interview_job_results" not in existing or not job_ids:
        return []
    columns = columns_by_table.get("interview_job_results", set())
    if "job_id" not in columns:
        return []

    params = {}
    placeholders = []
    for index, value in enumerate(unique_values(job_ids)):
        name = f"planner_job_id_{index}"
        params[name] = value
        placeholders.append(sql.Placeholder(name))
    if not placeholders:
        return []
    if len(placeholders) == 1:
        where_sql = sql.SQL("r.job_id::text = {}").format(placeholders[0])
    else:
        where_sql = sql.SQL("r.job_id::text IN ({})").format(sql.SQL(", ").join(placeholders))

    if "interview_job_runs" in existing:
        cursor.execute(
            sql.SQL(
                """
                SELECT
                  r.job_id,
                  r.result_ref,
                  r.result_json,
                  r.guides_count,
                  r.created_at,
                  r.updated_at,
                  j.progress_json,
                  j.request_payload,
                  j.preprocessor_run_id
                FROM public.interview_job_results r
                LEFT JOIN public.interview_job_runs j ON j.job_id::text = r.job_id::text
                WHERE 
                """
            )
            + where_sql
            + sql.SQL(" ORDER BY r.updated_at DESC NULLS LAST"),
            params,
        )
    else:
        cursor.execute(
            sql.SQL(
                """
                SELECT
                  r.job_id,
                  r.result_ref,
                  r.result_json,
                  r.guides_count,
                  r.created_at,
                  r.updated_at,
                  NULL::jsonb AS progress_json,
                  NULL::jsonb AS request_payload,
                  NULL::text AS preprocessor_run_id
                FROM public.interview_job_results r
                WHERE 
                """
            )
            + where_sql
            + sql.SQL(" ORDER BY r.updated_at DESC NULLS LAST"),
            params,
        )
    jobs = []
    for row in cursor.fetchall():
        result_json = row.get("result_json") or {}
        guides = extract_guides(result_json)
        jobs.append(
            {
                "job_id": row.get("job_id"),
                "result_ref": row.get("result_ref"),
                "guides_count": row.get("guides_count") or len(guides),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "progress_json": row.get("progress_json") or {},
                "request_payload": row.get("request_payload") or {},
                "preprocessor_run_id": row.get("preprocessor_run_id"),
                "guides": guides,
            }
        )
    return jobs


def fetch_plan_run_artifacts(
    cursor,
    existing: set[str],
    columns_by_table: dict[str, set[str]],
    plan_run_ids: list[str],
) -> list[dict]:
    if "interview_plan_runs" not in existing or not plan_run_ids:
        return []
    plan_columns = columns_by_table.get("interview_plan_runs", set())
    where_sql, params = build_simple_where(plan_columns, [("plan_run_id", plan_run_ids)])
    if where_sql is None:
        return []

    cursor.execute(
        sql.SQL(
            """
            SELECT
              plan_run_id::text,
              source_job_id,
              process_code,
              status,
              plan_version,
              objective,
              created_at,
              updated_at
            FROM public.interview_plan_runs
            WHERE 
            """
        )
        + where_sql
        + sql.SQL(" ORDER BY created_at DESC NULLS LAST"),
        params,
    )
    plans = [dict(row) for row in cursor.fetchall()]
    for plan in plans:
        plan["tracks"] = fetch_plan_tracks(cursor, existing, columns_by_table, plan["plan_run_id"])
    return plans


def fetch_plan_tracks(cursor, existing: set[str], columns_by_table: dict[str, set[str]], plan_run_id: str) -> list[dict]:
    if "interview_plan_tracks" not in existing:
        return []
    cursor.execute(
        """
        SELECT
          plan_track_id::text,
          track_key,
          display_name,
          description,
          mode,
          selection_mode,
          order_index
        FROM public.interview_plan_tracks
        WHERE plan_run_id = %(plan_run_id)s
        ORDER BY order_index NULLS LAST, display_name NULLS LAST
        """,
        {"plan_run_id": plan_run_id},
    )
    tracks = [dict(row) for row in cursor.fetchall()]
    for track in tracks:
        track["steps"] = fetch_plan_steps(cursor, existing, columns_by_table, plan_run_id, track["plan_track_id"])
    return tracks


def fetch_plan_steps(
    cursor,
    existing: set[str],
    columns_by_table: dict[str, set[str]],
    plan_run_id: str,
    plan_track_id: str,
) -> list[dict]:
    if "interview_plan_steps" not in existing:
        return []
    cursor.execute(
        sql.SQL(
            """
            SELECT
              plan_step_id::text,
              step_key,
              order_index,
              title,
              short_label,
              description,
              mode,
              taxonomy_json,
              coalesce(script_text, '') AS script_text,
              analysis_goal,
              review_policy,
              termination_rule,
              max_questions_hint
            FROM public.interview_plan_steps
            WHERE plan_run_id = %(plan_run_id)s
              AND plan_track_id = %(plan_track_id)s
            ORDER BY order_index NULLS LAST, title NULLS LAST
            """
        ),
        {"plan_run_id": plan_run_id, "plan_track_id": plan_track_id},
    )
    return [dict(row) for row in cursor.fetchall()]


def extract_guides(result_json: dict) -> list[dict]:
    if not isinstance(result_json, dict):
        return []
    container = result_json.get("result") if isinstance(result_json.get("result"), dict) else result_json
    guides = container.get("guides") if isinstance(container, dict) else []
    if not isinstance(guides, list):
        return []
    normalized = []
    for guide in guides:
        if not isinstance(guide, dict):
            continue
        normalized.append(
            {
                "title": guide.get("title"),
                "description": guide.get("description"),
                "script": str(guide.get("script") or ""),
                "taxonomy": guide.get("taxonomy"),
            }
        )
    return normalized


def build_simple_where(columns: set[str], criteria: list[tuple[str, list[str]]]) -> tuple[sql.SQL | None, dict]:
    clauses = []
    params = {}
    for column, raw_values in criteria:
        if column not in columns:
            continue
        values = unique_values(raw_values)
        if not values:
            continue
        placeholders = []
        for index, value in enumerate(values):
            name = f"artifact_{column}_{len(params)}_{index}"
            params[name] = value
            placeholders.append(sql.Placeholder(name))
        if len(placeholders) == 1:
            clauses.append(sql.SQL("{}::text = {}").format(sql.Identifier(column), placeholders[0]))
        else:
            clauses.append(
                sql.SQL("{}::text IN ({})").format(
                    sql.Identifier(column),
                    sql.SQL(", ").join(placeholders),
                )
            )
    if not clauses:
        return None, {}
    return sql.SQL(" OR ").join(clauses), params


def unique_values(values: list[str]) -> list[str]:
    result = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def normalize_identifiers(payload: dict) -> dict:
    keys = [
        "flowId",
        "subprocessCode",
        "correlationId",
        "preprocessorRunId",
        "plannerJobId",
        "plannerPlanRunId",
        "launchToken",
        "workflowRunId",
        "sessionId",
    ]
    identifiers = {key: str(payload.get(key) or "").strip() for key in keys if str(payload.get(key) or "").strip()}
    requested_scope = str(payload.get("scope") or "").strip().lower()
    if requested_scope in {"flow", "subprocess"}:
        identifiers["scope"] = requested_scope
    else:
        exact_keys = {
            "flowId",
            "preprocessorRunId",
            "plannerJobId",
            "plannerPlanRunId",
            "launchToken",
            "workflowRunId",
            "sessionId",
        }
        identifiers["scope"] = "flow" if exact_keys.intersection(identifiers) else "subprocess"
    return identifiers


def derive_related_identifiers(cursor, existing: set[str], columns_by_table: dict[str, set[str]], identifiers: dict) -> dict:
    subprocess_code = identifiers.get("subprocessCode")
    if not subprocess_code:
        return identifiers
    if identifiers.get("scope") == "flow":
        return derive_flow_identifiers(cursor, existing, columns_by_table, identifiers)

    process_ids = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "business_processes",
        "id",
        [("code", [subprocess_code])],
    )
    extend_identifier_list(identifiers, "processIds", process_ids)

    run_ids = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "subprocess_runs",
        "id",
        [("subprocess_code", [subprocess_code])],
    )
    extend_identifier_list(identifiers, "preprocessorRunIds", run_ids)
    if run_ids and not identifiers.get("preprocessorRunId"):
        identifiers["preprocessorRunId"] = run_ids[0]

    document_ids = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "documents",
        "id",
        [
            ("subprocess_run_id", run_ids),
            ("process_id", process_ids),
        ],
    )
    extend_identifier_list(identifiers, "documentIds", document_ids)

    job_ids = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "interview_job_runs",
        "job_id",
        [
            ("process_code", [subprocess_code]),
            ("preprocessor_run_id", run_ids),
        ],
    )
    extend_identifier_list(identifiers, "plannerJobIds", job_ids)
    if job_ids and not identifiers.get("plannerJobId"):
        identifiers["plannerJobId"] = job_ids[0]

    plan_run_ids = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "interview_plan_runs",
        "plan_run_id",
        [
            ("process_code", [subprocess_code]),
            ("preprocessor_run_id", run_ids),
            ("source_job_id", job_ids),
        ],
    )
    extend_identifier_list(identifiers, "plannerPlanRunIds", plan_run_ids)
    if plan_run_ids and not identifiers.get("plannerPlanRunId"):
        identifiers["plannerPlanRunId"] = plan_run_ids[0]

    launch_tokens = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "interview_launches",
        "launch_token",
        [
            ("subprocess_code", [subprocess_code]),
            ("subprocess_run_id", run_ids),
            ("job_id", job_ids),
            ("plan_run_id", plan_run_ids),
        ],
    )
    extend_identifier_list(identifiers, "launchTokens", launch_tokens)
    if launch_tokens and not identifiers.get("launchToken"):
        identifiers["launchToken"] = launch_tokens[0]
    return identifiers


def derive_flow_identifiers(cursor, existing: set[str], columns_by_table: dict[str, set[str]], identifiers: dict) -> dict:
    subprocess_code = identifiers.get("subprocessCode")
    process_ids = fetch_values_by_or(
        cursor,
        existing,
        columns_by_table,
        "business_processes",
        "id",
        [("code", [subprocess_code])],
    )
    extend_identifier_list(identifiers, "processIds", process_ids)

    launch_tokens = identifier_values(identifiers, "launchToken") or identifier_values(identifiers, "launchTokens")
    if launch_tokens:
        extend_identifier_list(
            identifiers,
            "preprocessorRunIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_launches",
                "subprocess_run_id",
                [("launch_token", launch_tokens)],
            ),
        )
        extend_identifier_list(
            identifiers,
            "plannerJobIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_launches",
                "job_id",
                [("launch_token", launch_tokens)],
            ),
        )
        extend_identifier_list(
            identifiers,
            "plannerPlanRunIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_launches",
                "plan_run_id",
                [("launch_token", launch_tokens)],
            ),
        )

    run_ids = unique_values(identifier_values(identifiers, "preprocessorRunId") + identifier_values(identifiers, "preprocessorRunIds"))
    job_ids = unique_values(identifier_values(identifiers, "plannerJobId") + identifier_values(identifiers, "plannerJobIds"))
    plan_run_ids = unique_values(identifier_values(identifiers, "plannerPlanRunId") + identifier_values(identifiers, "plannerPlanRunIds"))

    if job_ids:
        extend_identifier_list(
            identifiers,
            "preprocessorRunIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_job_runs",
                "preprocessor_run_id",
                [("job_id", job_ids)],
            ),
        )
        extend_identifier_list(
            identifiers,
            "plannerPlanRunIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_job_runs",
                "result_plan_run_id",
                [("job_id", job_ids)],
            ),
        )

    plan_run_ids = unique_values(identifier_values(identifiers, "plannerPlanRunId") + identifier_values(identifiers, "plannerPlanRunIds"))
    if plan_run_ids:
        extend_identifier_list(
            identifiers,
            "plannerJobIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_plan_runs",
                "source_job_id",
                [("plan_run_id", plan_run_ids)],
            ),
        )
        extend_identifier_list(
            identifiers,
            "preprocessorRunIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_plan_runs",
                "preprocessor_run_id",
                [("plan_run_id", plan_run_ids)],
            ),
        )

    run_ids = unique_values(identifier_values(identifiers, "preprocessorRunId") + identifier_values(identifiers, "preprocessorRunIds"))
    if run_ids:
        extend_identifier_list(
            identifiers,
            "documentIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "documents",
                "id",
                [("subprocess_run_id", run_ids)],
            ),
        )
        if not identifier_values(identifiers, "plannerJobId"):
            extend_identifier_list(
                identifiers,
                "plannerJobIds",
                fetch_values_by_or(
                    cursor,
                    existing,
                    columns_by_table,
                    "interview_job_runs",
                    "job_id",
                    [("preprocessor_run_id", run_ids)],
                ),
            )

    job_ids = unique_values(identifier_values(identifiers, "plannerJobId") + identifier_values(identifiers, "plannerJobIds"))
    if job_ids and not identifier_values(identifiers, "plannerPlanRunId"):
        extend_identifier_list(
            identifiers,
            "plannerPlanRunIds",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_plan_runs",
                "plan_run_id",
                [("source_job_id", job_ids)],
            ),
        )

    run_ids = unique_values(identifier_values(identifiers, "preprocessorRunId") + identifier_values(identifiers, "preprocessorRunIds"))
    job_ids = unique_values(identifier_values(identifiers, "plannerJobId") + identifier_values(identifiers, "plannerJobIds"))
    plan_run_ids = unique_values(identifier_values(identifiers, "plannerPlanRunId") + identifier_values(identifiers, "plannerPlanRunIds"))
    launch_tokens = unique_values(identifier_values(identifiers, "launchToken") + identifier_values(identifiers, "launchTokens"))
    if run_ids or job_ids or plan_run_ids:
        extend_identifier_list(
            identifiers,
            "launchTokens",
            fetch_values_by_or(
                cursor,
                existing,
                columns_by_table,
                "interview_launches",
                "launch_token",
                [
                    ("subprocess_run_id", run_ids),
                    ("job_id", job_ids),
                    ("plan_run_id", plan_run_ids),
                ],
            ),
        )
    if launch_tokens and not identifiers.get("launchToken"):
        identifiers["launchToken"] = launch_tokens[0]
    return identifiers


def extend_identifier_list(identifiers: dict, key: str, values: list[str], limit: int = 50) -> None:
    current = identifier_values(identifiers, key)
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in current:
            current.append(normalized)
    if current:
        identifiers[key] = current[:limit]


def identifier_values(identifiers: dict, key: str) -> list[str]:
    value = identifiers.get(key)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if value:
        return [str(value).strip()]
    return []


def fetch_values_by_or(
    cursor,
    existing: set[str],
    columns_by_table: dict[str, set[str]],
    table: str,
    select_column: str,
    criteria: list[tuple[str, list[str]]],
    limit: int = 50,
) -> list[str]:
    if table not in existing:
        return []
    columns = columns_by_table.get(table, set())
    if select_column not in columns:
        return []

    clauses = []
    params = {}
    for column, raw_values in criteria:
        if column not in columns:
            continue
        values = [str(value).strip() for value in raw_values if str(value or "").strip()]
        if not values:
            continue
        placeholders = []
        for index, value in enumerate(values):
            name = f"{column}_{len(params)}_{index}"
            params[name] = value
            placeholders.append(sql.Placeholder(name))
        if len(placeholders) == 1:
            clauses.append(
                sql.SQL("upper({}::text) = upper({})").format(sql.Identifier(column), placeholders[0])
            )
        else:
            clauses.append(
                sql.SQL("upper({}::text) IN ({})").format(
                    sql.Identifier(column),
                    sql.SQL(", ").join(sql.SQL("upper({})").format(item) for item in placeholders),
                )
            )
    if not clauses:
        return []

    order_column = next((column for column in ORDER_COLUMNS if column in columns), None)
    query = (
        sql.SQL("SELECT {}::text AS value FROM public.{} WHERE ").format(
            sql.Identifier(select_column),
            sql.Identifier(table),
        )
        + sql.SQL(" OR ").join(clauses)
    )
    if order_column:
        query += sql.SQL(" ORDER BY {} DESC NULLS LAST").format(sql.Identifier(order_column))
    query += sql.SQL(" LIMIT {}").format(sql.Literal(limit))
    cursor.execute(query, params)
    values = []
    for row in cursor.fetchall():
        value = str(row["value"] or "").strip()
        if value and value not in values:
            values.append(value)
    return values


def existing_tables(cursor) -> set[str]:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
    )
    return {row["table_name"] for row in cursor.fetchall()}


def table_columns(cursor, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %(table)s
        """,
        {"table": table},
    )
    return {row["column_name"] for row in cursor.fetchall()}


def build_where(table: str, columns: set[str], identifiers: dict) -> tuple[sql.SQL | None, dict]:
    clauses = []
    params = {}
    mapping = table_match_mapping(table, identifiers)
    for column, keys in mapping.items():
        if column not in columns:
            continue
        for key in keys:
            for value in identifier_values(identifiers, key):
                param_name = f"{column}_{key}_{len(params)}"
                clauses.append(
                    sql.SQL("upper({}::text) = upper({})").format(
                        sql.Identifier(column),
                        sql.Placeholder(param_name),
                    )
                )
                params[param_name] = value
    if not clauses:
        return None, {}
    return sql.SQL(" OR ").join(clauses), params


def table_match_mapping(table: str, identifiers: dict) -> dict[str, list[str]]:
    base_mapping = {
        "id": ["processId", "processIds", "preprocessorRunId", "preprocessorRunIds", "plannerPlanRunId", "plannerPlanRunIds"],
        "code": ["subprocessCode"],
        "process_id": ["processId", "processIds"],
        "document_id": ["documentId", "documentIds"],
        "subprocess_run_id": ["preprocessorRunId", "preprocessorRunIds"],
        "subprocess_code": ["subprocessCode"],
        "preprocessor_run_id": ["preprocessorRunId", "preprocessorRunIds"],
        "correlation_id": ["correlationId"],
        "job_id": ["plannerJobId", "plannerJobIds"],
        "source_job_id": ["plannerJobId", "plannerJobIds"],
        "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        "result_plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        "process_code": ["subprocessCode"],
        "launch_token": ["launchToken", "launchTokens"],
        "workflow_run_id": ["workflowRunId"],
        "session_id": ["sessionId"],
    }
    if identifiers.get("scope") != "flow":
        return base_mapping

    flow_mappings: dict[str, dict[str, list[str]]] = {
        "business_processes": {
            "id": ["processId", "processIds"],
            "code": ["subprocessCode"],
        },
        "subprocess_runs": {
            "id": ["preprocessorRunId", "preprocessorRunIds"],
            "correlation_id": ["correlationId"],
        },
        "processing_tasks": {
            "subprocess_run_id": ["preprocessorRunId", "preprocessorRunIds"],
            "correlation_id": ["correlationId"],
        },
        "task_inputs": {
            "subprocess_run_id": ["preprocessorRunId", "preprocessorRunIds"],
        },
        "documents": {
            "id": ["documentId", "documentIds"],
            "subprocess_run_id": ["preprocessorRunId", "preprocessorRunIds"],
        },
        "document_units": {
            "document_id": ["documentId", "documentIds"],
        },
        "integration_events": {
            "correlation_id": ["correlationId"],
            "subprocess_run_id": ["preprocessorRunId", "preprocessorRunIds"],
            "job_id": ["plannerJobId", "plannerJobIds"],
            "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        },
        "interview_job_runs": {
            "job_id": ["plannerJobId", "plannerJobIds"],
            "preprocessor_run_id": ["preprocessorRunId", "preprocessorRunIds"],
            "result_plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
            "correlation_id": ["correlationId"],
        },
        "interview_job_events": {
            "job_id": ["plannerJobId", "plannerJobIds"],
        },
        "interview_job_results": {
            "job_id": ["plannerJobId", "plannerJobIds"],
        },
        "interview_plan_runs": {
            "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
            "source_job_id": ["plannerJobId", "plannerJobIds"],
            "preprocessor_run_id": ["preprocessorRunId", "preprocessorRunIds"],
        },
        "interview_plan_tracks": {
            "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        },
        "interview_plan_steps": {
            "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        },
        "active_interview_plans": {
            "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        },
        "interview_launches": {
            "launch_token": ["launchToken", "launchTokens"],
            "subprocess_run_id": ["preprocessorRunId", "preprocessorRunIds"],
            "job_id": ["plannerJobId", "plannerJobIds"],
            "plan_run_id": ["plannerPlanRunId", "plannerPlanRunIds"],
        },
        "voice_agent_workflow_runs": {
            "workflow_run_id": ["workflowRunId"],
        },
        "voice_agent_step_runs": {
            "workflow_run_id": ["workflowRunId"],
        },
        "voice_agent_sessions": {
            "session_id": ["sessionId"],
        },
        "voice_agent_transcripts": {
            "session_id": ["sessionId"],
        },
        "voice_agent_audio_files": {
            "workflow_run_id": ["workflowRunId"],
            "session_id": ["sessionId"],
        },
    }
    return flow_mappings.get(table, base_mapping)


def count_rows(cursor, table: str, where_sql: sql.SQL | None, params: dict) -> int:
    query = sql.SQL("SELECT count(*) AS count FROM public.{}").format(sql.Identifier(table))
    if where_sql is not None:
        query += sql.SQL(" WHERE ") + where_sql
    cursor.execute(query, params)
    return int(cursor.fetchone()["count"])


def fetch_rows(cursor, table: str, columns: set[str], where_sql: sql.SQL | None, params: dict) -> list[dict]:
    order_column = next((column for column in ORDER_COLUMNS if column in columns), None)
    query = sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(table))
    if where_sql is not None:
        query += sql.SQL(" WHERE ") + where_sql
    if order_column:
        query += sql.SQL(" ORDER BY {} DESC NULLS LAST").format(sql.Identifier(order_column))
    query += sql.SQL(" LIMIT 8")
    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [compact_row(row) for row in rows]


def compact_row(row: dict) -> dict:
    preferred = [
        "id",
        "code",
        "name",
        "process_kind",
        "document_id",
        "task_id",
        "event_id",
        "event_type",
        "status",
        "stage",
        "documents_status",
        "planner_status",
        "subprocess_code",
        "subprocess_run_id",
        "preprocessor_run_id",
        "correlation_id",
        "job_id",
        "source_job_id",
        "plan_run_id",
        "result_plan_run_id",
        "process_code",
        "process_id",
        "launch_token",
        "plan_version",
        "source_uri",
        "file_name",
        "document_role",
        "processing_status",
        "page_count",
        "unit_count",
        "workflow_run_id",
        "session_id",
        "progress_percent",
        "created_at",
        "updated_at",
        "completed_at",
        "error_message",
        "error",
    ]
    compact = {key: row.get(key) for key in preferred if key in row}
    if not compact:
        for key in list(row.keys())[:10]:
            compact[key] = row[key]
    return compact


def redact_db_url(value: str) -> str:
    if "@" not in value:
        return value
    prefix, suffix = value.rsplit("@", 1)
    scheme = prefix.split("://", 1)[0] if "://" in prefix else "postgresql"
    return f"{scheme}://***@{suffix}"


def to_json(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


if __name__ == "__main__":
    main()
