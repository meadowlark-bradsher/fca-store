# fca-store

Standalone FCA Store repo for Work Order 2 (Layer A).

## What this provides

- Versioned incidence store (`objects`, `attributes`, present/absent edges).
- Optional environment + overlay inheritance (`base_version_id`).
- Validity filtering (`valid_from` / `valid_to`) for time-aware queries.
- Concept lattice build using NextClosure (Ganter) and persisted concept artifacts.
- Query API:
  - `put_incidence(...)`
  - `build_lattice(...)`
  - `get_extent(...)`
  - `get_intent(...)`
  - `suggest_split_attributes(...)`
  - `diff_versions(...)`
- Optional HTTP service via FastAPI.

## Storage choice (v0)

Selected: **SQLite**.

Tradeoffs:
- Pros:
  - Minimal ops and setup for v0.
  - Deterministic local development and CI.
  - File-based artifact makes early integration simple.
- Cons:
  - Not ideal for high-concurrency writes.
  - Fewer scaling options than PostgreSQL.
  - Less ergonomic for some analytics workloads.

The code is organized so a PostgreSQL backend can be added behind the same interface later.

## Repo layout

```text
fca-store/
  src/fca_store/
    interface.py
    lattice.py
    sqlite_store.py
    http.py
  tests/
```

## Install

```bash
pip install -e .
```

For HTTP service:

```bash
pip install -e ".[http]"
```

For tests:

```bash
pip install -e ".[test]"
```

## Python usage

```python
from fca_store.sqlite_store import SQLiteFCAStore

store = SQLiteFCAStore("fca_store.db")

store.put_incidence(
    version_id="v1",
    objects=["cat", "dog"],
    attributes=["mammal", "has_fur"],
    edges_present=[("cat", "mammal"), ("cat", "has_fur"), ("dog", "mammal"), ("dog", "has_fur")],
    meta={"environment": "dev"},
)

store.build_lattice("v1", method="nextclosure")
extent = store.get_extent("v1", ["mammal"])
intent = store.get_intent("v1", ["cat"])
```

Overlay version example:

```python
store.put_incidence(
    version_id="v2",
    objects=["bat"],
    attributes=["can_fly"],
    edges_present=[("bat", "can_fly"), ("bat", "mammal")],
    edges_absent=[("dog", "has_fur")],
    meta={"base_version_id": "v1", "overlay": True, "environment": "dev"},
)
```

## Layer C integration contract

`put_incidence(...)` accepts Layer C materializer output as present edges:

- `edges_present`: list of `(object_id, attribute_id)`
- `edges_absent`: optional list of `(object_id, attribute_id)`

Absent/unknown edges are stored separately and used for non-FCA reasoning and diffs.

## Optional HTTP API

```python
from fca_store.http import create_app

app = create_app(db_path="fca_store.db")
```

Endpoints:
- `GET /versions`
- `POST /versions/{id}/extent`
- `POST /versions/{id}/intent`
- `GET /versions/{id}/lattice/status`
- `GET /versions/{id}/diff/{other}`

## Tests

Includes:
- Toy animals domain (known concepts and closure checks)
- Overlay + version diff checks
- Validity filtering checks
- 1k objects × 200 attributes sparse performance smoke

Run:

```bash
pytest
```

## GitHub Pages

This repo includes a Pages workflow at `.github/workflows/pages.yml` that deploys `docs/` on:
- pushes to `main`
- manual `workflow_dispatch`

One-time repo setting:
- In GitHub: `Settings -> Pages -> Build and deployment -> Source`
- Select `GitHub Actions`

After the first successful run, the site URL will be available in the workflow deployment output.
