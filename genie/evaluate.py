"""Ask the Genie space the 10 benchmark questions and check its answers (P6.2).

For every question Genie writes SQL; its result is compared with the result of our
reference SQL (genie/space.py BENCHMARKS). A question passes when every value of the
reference's first row appears in Genie's result:
- numbers within 1 %, also accepted in another unit (x1000, /1000, x100 for shares),
- dates by year-month-day (or year-month for a month), text case-insensitively.

    python -m genie.evaluate --catalog gridpulse_dev --title-prefix "[dev] "
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from genie import space
from genie.deploy_space import find_space, warehouse_id

SCALES = (1, 1000, 0.001, 100, 0.01, 1_000_000, 0.000_001)


def as_number(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def matches(expected, cells: list) -> bool:
    """Does one expected value appear among Genie's result cells?"""
    if expected is None:
        return True
    num = as_number(expected)
    if num is not None:
        for c in cells:
            got = as_number(c)
            if got is None:
                continue
            for s in SCALES:
                target = num * s
                if abs(got - target) <= max(abs(target) * 0.01, 0.011 * s):
                    return True
        return False
    text = str(expected).strip().lower()
    if len(text) >= 10 and text[4] == "-":  # a date or timestamp
        day = text[:10]
        month_only = day.endswith("-01")
        for c in cells:
            c = str(c).strip().lower()
            if c[:10] == day or (month_only and c[:7] == day[:7]):
                return True
        return False
    return any(text in str(c).strip().lower() for c in cells)


def run_sql(w: WorkspaceClient, wh: str, sql: str) -> list[list]:
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="50s")
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{r.status.state}: {r.status.error}")
    return (r.result.data_array if r.result else None) or []


def ask(w: WorkspaceClient, space_id: str, question: str) -> tuple[str | None, list[list], str]:
    """Genie's SQL, its result rows, and any text answer."""
    msg = w.genie.start_conversation_and_wait(space_id, question)
    sql, rows, text = None, [], ""
    for a in msg.attachments or []:
        if a.text and a.text.content:
            text += a.text.content
        if a.query and a.query.query:
            sql = a.query.query
            res = w.genie.get_message_attachment_query_result(
                space_id, msg.conversation_id, msg.message_id, a.attachment_id
            )
            sr = res.statement_response
            rows = (sr.result.data_array if sr and sr.result else None) or []
    return sql, rows, text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--title-prefix", default="")
    args = parser.parse_args(argv)

    w = WorkspaceClient()
    title = args.title_prefix + space.TITLE
    space_id = find_space(w, title)
    if not space_id:
        raise SystemExit(f"Genie space {title!r} not found; run genie.deploy_space first")
    wh = warehouse_id(w)

    passed = 0
    report = [f"# Genie benchmark ({dt.date.today()}, {args.catalog})", ""]
    for i, (question, ref_sql) in enumerate(space.BENCHMARKS, 1):
        expected = run_sql(w, wh, ref_sql.format(c=args.catalog))
        expected_row = expected[0] if expected else []
        try:
            sql, rows, text = ask(w, space_id, question)
        except Exception as exc:  # noqa: BLE001 - a failed question is a failed test
            sql, rows, text = None, [], f"error: {exc}"
        cells = [c for r in rows[:20] for c in r]
        ok = bool(expected_row) and all(matches(v, cells) for v in expected_row)
        passed += ok
        mark = "PASS" if ok else "FAIL"
        print(f"{i:2}. {mark}  {question}")
        print(f"      expected {expected_row}   genie {rows[:3]}")
        report += [
            f"## {i}. {mark}: {question}",
            f"- expected: `{expected_row}`",
            f"- Genie result (first rows): `{rows[:3]}`",
        ]
        if text:
            report.append(f"- Genie text: {text.strip()[:300]}")
        if sql:
            report += ["", "```sql", sql.strip(), "```"]
        report.append("")

    summary = f"{passed}/{len(space.BENCHMARKS)} questions answered correctly"
    print(f"\n{summary}")
    report.insert(1, summary)
    with open("genie_benchmark.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(report))
    return 0 if passed == len(space.BENCHMARKS) else 1


if __name__ == "__main__":
    sys.exit(main())
