from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .sqlite_store import SQLiteFCAStore


def create_app(store: SQLiteFCAStore | None = None, db_path: str | Path = "fca_store.db"):
    try:
        from fastapi import FastAPI, HTTPException
        from pydantic import BaseModel, Field
    except ImportError as exc:  # pragma: no cover - optional dependency path
        raise ImportError(
            "FastAPI is not installed. Install fca-store with the 'http' extra."
        ) from exc

    app = FastAPI(title="FCA Store v0", version="0.1.0")
    service = store or SQLiteFCAStore(db_path=db_path)

    class ExtentRequest(BaseModel):
        attribute_ids: list[str] = Field(default_factory=list)
        as_of: str | None = None

    class IntentRequest(BaseModel):
        object_ids: list[str] = Field(default_factory=list)
        as_of: str | None = None

    @app.get("/versions")
    def list_versions(environment: str | None = None) -> list[dict[str, Any]]:
        return service.list_versions(environment=environment)

    @app.post("/versions/{version_id}/extent")
    def extent(version_id: str, payload: ExtentRequest) -> dict[str, Any]:
        try:
            object_ids = service.get_extent(
                version_id=version_id,
                attribute_ids=payload.attribute_ids,
                as_of=payload.as_of,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"version_id": version_id, "object_ids": object_ids}

    @app.post("/versions/{version_id}/intent")
    def intent(version_id: str, payload: IntentRequest) -> dict[str, Any]:
        try:
            attribute_ids = service.get_intent(
                version_id=version_id,
                object_ids=payload.object_ids,
                as_of=payload.as_of,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"version_id": version_id, "attribute_ids": attribute_ids}

    @app.get("/versions/{version_id}/lattice/status")
    def lattice_status(version_id: str) -> dict[str, Any]:
        return service.get_lattice_status(version_id)

    @app.get("/versions/{version_id}/diff/{other_version_id}")
    def diff(version_id: str, other_version_id: str, as_of: str | None = None) -> dict[str, Any]:
        try:
            return service.diff_versions(version_id, other_version_id, as_of=as_of)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

    return app
