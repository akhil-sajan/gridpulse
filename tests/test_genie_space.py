"""P6.2: the Genie space definition follows the API's validation rules."""

import json
import re

from genie import evaluate, space

HEX32 = re.compile(r"^[0-9a-f]{32}$")


def ids(d: dict) -> list[list[str]]:
    ins = d["instructions"]
    return [
        [x["id"] for x in d["config"]["sample_questions"]],
        [x["id"] for x in ins["text_instructions"]],
        [x["id"] for x in ins["example_question_sqls"]],
        [x["id"] for x in ins["join_specs"]],
        [x["id"] for x in d["benchmarks"]["questions"]],
    ]


def test_ids_valid_sorted_and_unique():
    d = space.build("gridpulse_dev")
    for group in ids(d):
        assert all(HEX32.match(i) for i in group)
        assert group == sorted(group)
    flat = [i for g in ids(d) for i in g]
    assert len(flat) == len(set(flat))


def test_tables_and_columns_sorted_three_level():
    d = space.build("gridpulse_prod")
    names = [t["identifier"] for t in d["data_sources"]["tables"]]
    assert names == sorted(names)
    assert all(n.count(".") == 2 and n.startswith("gridpulse_prod.") for n in names)
    for t in d["data_sources"]["tables"]:
        cols = [c["column_name"] for c in t.get("column_configs", [])]
        assert cols == sorted(cols)


def test_limits_and_shape():
    d = space.build("gridpulse_dev")
    assert d["version"] == 2
    assert len(d["instructions"]["text_instructions"]) == 1
    assert len(d["benchmarks"]["questions"]) == 10
    for q in d["benchmarks"]["questions"]:
        assert len(q["answer"]) == 1 and q["answer"][0]["format"] == "SQL"
    for j in d["instructions"]["join_specs"]:
        assert len(j["sql"]) == 2 and j["sql"][1].startswith("--rt=FROM_RELATIONSHIP_TYPE_")
    assert all(len(s) <= 25_000 for s in json.dumps(d).split('"'))


def test_sql_uses_only_space_tables_and_target_catalog():
    d = space.build("gridpulse_dev")
    known = {t["identifier"] for t in d["data_sources"]["tables"]}
    sqls = ["".join(e["sql"]) for e in d["instructions"]["example_question_sqls"]] + [
        "".join(b["answer"][0]["content"]) for b in d["benchmarks"]["questions"]
    ]
    for sql in sqls:
        used = set(re.findall(r"gridpulse_\w+\.gold\.\w+", sql))
        assert used and used <= known, used - known


def test_examples_and_benchmarks_differ():
    assert not {q for q, _ in space.EXAMPLES} & {q for q, _ in space.BENCHMARKS}


def test_answer_matching():
    m = evaluate.matches
    assert m("41.3", ["41.27"])  # within 1 %
    assert m("0.413", ["41.3"])  # share vs percent
    assert m("59.12", ["59120.4"])  # GW vs MW
    assert not m("59.12", ["70"])
    assert m("2025-06-01T00:00:00.000Z", ["2025-06"])  # month
    assert m("2025-04-21", ["2025-04-21T00:00:00Z"])
    assert m("Niedersachsen", ["NIEDERSACHSEN"])
    assert not m("Bayern", ["Hessen"])
