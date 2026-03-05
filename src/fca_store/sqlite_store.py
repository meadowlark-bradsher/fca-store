from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .interface import FCAStore
from .lattice import (
    FormalConcept,
    build_concepts_nextclosure,
    build_context,
    extent_from_attributes,
    intent_from_objects,
)


class SQLiteFCAStore(FCAStore):
    def __init__(self, db_path: str | Path = "fca_store.db") -> None:
        self.db_path = str(db_path)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._ensure_schema()

    def close(self) -> None:
        self._conn.close()

    def _ensure_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS versions (
                version_id TEXT PRIMARY KEY,
                environment TEXT,
                base_version_id TEXT,
                overlay INTEGER NOT NULL DEFAULT 0,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(base_version_id) REFERENCES versions(version_id)
            );

            CREATE TABLE IF NOT EXISTS version_objects (
                version_id TEXT NOT NULL,
                object_id TEXT NOT NULL,
                PRIMARY KEY (version_id, object_id),
                FOREIGN KEY(version_id) REFERENCES versions(version_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS version_attributes (
                version_id TEXT NOT NULL,
                attribute_id TEXT NOT NULL,
                PRIMARY KEY (version_id, attribute_id),
                FOREIGN KEY(version_id) REFERENCES versions(version_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS version_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id TEXT NOT NULL,
                object_id TEXT NOT NULL,
                attribute_id TEXT NOT NULL,
                is_present INTEGER NOT NULL CHECK (is_present IN (0, 1)),
                valid_from TEXT,
                valid_to TEXT,
                edge_meta_json TEXT,
                FOREIGN KEY(version_id) REFERENCES versions(version_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_version_edges_vid
                ON version_edges(version_id, id);
            CREATE INDEX IF NOT EXISTS idx_version_edges_lookup
                ON version_edges(version_id, object_id, attribute_id);

            CREATE TABLE IF NOT EXISTS lattice_meta (
                version_id TEXT PRIMARY KEY,
                method TEXT NOT NULL,
                concept_count INTEGER NOT NULL,
                built_at TEXT NOT NULL,
                FOREIGN KEY(version_id) REFERENCES versions(version_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS lattice_concepts (
                version_id TEXT NOT NULL,
                concept_idx INTEGER NOT NULL,
                intent_json TEXT NOT NULL,
                extent_json TEXT NOT NULL,
                intent_key TEXT NOT NULL,
                extent_key TEXT NOT NULL,
                PRIMARY KEY (version_id, concept_idx),
                FOREIGN KEY(version_id) REFERENCES versions(version_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_lattice_intent_key
                ON lattice_concepts(version_id, intent_key);
            CREATE INDEX IF NOT EXISTS idx_lattice_extent_key
                ON lattice_concepts(version_id, extent_key);
            """
        )
        self._conn.commit()

    @staticmethod
    def _coerce_as_of(as_of: datetime | str | None) -> str | None:
        if as_of is None:
            return None
        if isinstance(as_of, datetime):
            if as_of.tzinfo is None:
                return as_of.replace(microsecond=0).isoformat()
            return as_of.astimezone().replace(microsecond=0).isoformat()
        return as_of

    def put_incidence(
        self,
        version_id: str,
        objects: Sequence[str],
        attributes: Sequence[str],
        edges_present: Sequence[tuple[str, str]],
        edges_absent: Sequence[tuple[str, str]] | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        payload = dict(meta or {})
        edges_absent = list(edges_absent or [])
        edges_present = list(edges_present)
        now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        object_set = set(objects)
        attribute_set = set(attributes)
        for object_id, attribute_id in edges_present + edges_absent:
            object_set.add(object_id)
            attribute_set.add(attribute_id)

        base_version_id = payload.get("base_version_id")
        overlay = int(bool(payload.get("overlay", bool(base_version_id))))
        environment = payload.get("environment")
        valid_from = payload.get("valid_from")
        valid_to = payload.get("valid_to")

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO versions(version_id, environment, base_version_id, overlay, meta_json, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(version_id) DO UPDATE SET
                    environment = excluded.environment,
                    base_version_id = excluded.base_version_id,
                    overlay = excluded.overlay,
                    meta_json = excluded.meta_json,
                    updated_at = excluded.updated_at
                """,
                (
                    version_id,
                    environment,
                    base_version_id,
                    overlay,
                    json.dumps(payload, sort_keys=True),
                    now,
                    now,
                ),
            )

            self._conn.execute("DELETE FROM version_objects WHERE version_id = ?", (version_id,))
            self._conn.execute("DELETE FROM version_attributes WHERE version_id = ?", (version_id,))
            self._conn.execute("DELETE FROM version_edges WHERE version_id = ?", (version_id,))
            self._conn.execute("DELETE FROM lattice_concepts WHERE version_id = ?", (version_id,))
            self._conn.execute("DELETE FROM lattice_meta WHERE version_id = ?", (version_id,))

            self._conn.executemany(
                "INSERT INTO version_objects(version_id, object_id) VALUES(?, ?)",
                [(version_id, object_id) for object_id in sorted(object_set)],
            )
            self._conn.executemany(
                "INSERT INTO version_attributes(version_id, attribute_id) VALUES(?, ?)",
                [(version_id, attribute_id) for attribute_id in sorted(attribute_set)],
            )

            self._conn.executemany(
                """
                INSERT INTO version_edges(version_id, object_id, attribute_id, is_present, valid_from, valid_to, edge_meta_json)
                VALUES(?, ?, ?, 1, ?, ?, NULL)
                """,
                [
                    (version_id, object_id, attribute_id, valid_from, valid_to)
                    for object_id, attribute_id in edges_present
                ],
            )
            self._conn.executemany(
                """
                INSERT INTO version_edges(version_id, object_id, attribute_id, is_present, valid_from, valid_to, edge_meta_json)
                VALUES(?, ?, ?, 0, ?, ?, NULL)
                """,
                [
                    (version_id, object_id, attribute_id, valid_from, valid_to)
                    for object_id, attribute_id in edges_absent
                ],
            )

    def _get_version_chain(self, version_id: str) -> list[str]:
        chain: list[str] = []
        seen: set[str] = set()
        current = version_id
        while current is not None:
            if current in seen:
                raise ValueError(f"Version inheritance cycle detected at version '{current}'")
            seen.add(current)
            row = self._conn.execute(
                "SELECT version_id, base_version_id FROM versions WHERE version_id = ?",
                (current,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown version_id: {current}")
            chain.append(row["version_id"])
            current = row["base_version_id"]
        chain.reverse()
        return chain

    def _effective_state(
        self,
        version_id: str,
        as_of: datetime | str | None = None,
    ) -> tuple[list[str], list[str], set[tuple[str, str]], set[tuple[str, str]]]:
        chain = self._get_version_chain(version_id)
        as_of_value = self._coerce_as_of(as_of)

        objects: set[str] = set()
        attributes: set[str] = set()
        edge_state: dict[tuple[str, str], bool] = {}

        for chain_version in chain:
            object_rows = self._conn.execute(
                "SELECT object_id FROM version_objects WHERE version_id = ?",
                (chain_version,),
            ).fetchall()
            attr_rows = self._conn.execute(
                "SELECT attribute_id FROM version_attributes WHERE version_id = ?",
                (chain_version,),
            ).fetchall()
            objects.update(row["object_id"] for row in object_rows)
            attributes.update(row["attribute_id"] for row in attr_rows)

            if as_of_value is None:
                edge_rows = self._conn.execute(
                    """
                    SELECT object_id, attribute_id, is_present
                    FROM version_edges
                    WHERE version_id = ?
                    ORDER BY id ASC
                    """,
                    (chain_version,),
                ).fetchall()
            else:
                edge_rows = self._conn.execute(
                    """
                    SELECT object_id, attribute_id, is_present
                    FROM version_edges
                    WHERE version_id = ?
                      AND (valid_from IS NULL OR valid_from <= ?)
                      AND (valid_to IS NULL OR valid_to > ?)
                    ORDER BY id ASC
                    """,
                    (chain_version, as_of_value, as_of_value),
                ).fetchall()

            for row in edge_rows:
                object_id = row["object_id"]
                attribute_id = row["attribute_id"]
                objects.add(object_id)
                attributes.add(attribute_id)
                edge_state[(object_id, attribute_id)] = bool(row["is_present"])

        present_edges = {key for key, value in edge_state.items() if value}
        absent_edges = {key for key, value in edge_state.items() if not value}
        return sorted(objects), sorted(attributes), present_edges, absent_edges

    def _context_for_version(
        self,
        version_id: str,
        as_of: datetime | str | None = None,
    ):
        object_ids, attribute_ids, present_edges, _ = self._effective_state(version_id, as_of=as_of)
        return build_context(object_ids, attribute_ids, present_edges)

    def build_lattice(self, version_id: str, method: str = "nextclosure") -> dict[str, Any]:
        normalized_method = method.strip().lower()
        if normalized_method not in {"nextclosure", "ganter"}:
            raise ValueError(f"Unsupported lattice method '{method}'. Use 'nextclosure' or 'ganter'.")

        context = self._context_for_version(version_id)
        concepts = build_concepts_nextclosure(context)
        built_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        with self._conn:
            self._conn.execute("DELETE FROM lattice_concepts WHERE version_id = ?", (version_id,))
            self._conn.execute("DELETE FROM lattice_meta WHERE version_id = ?", (version_id,))

            self._conn.executemany(
                """
                INSERT INTO lattice_concepts(version_id, concept_idx, intent_json, extent_json, intent_key, extent_key)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        version_id,
                        idx,
                        json.dumps(concept.intent),
                        json.dumps(concept.extent),
                        "|".join(concept.intent),
                        "|".join(concept.extent),
                    )
                    for idx, concept in enumerate(concepts)
                ],
            )
            self._conn.execute(
                """
                INSERT INTO lattice_meta(version_id, method, concept_count, built_at)
                VALUES(?, ?, ?, ?)
                """,
                (version_id, normalized_method, len(concepts), built_at),
            )

        return {
            "version_id": version_id,
            "method": normalized_method,
            "concept_count": len(concepts),
            "built_at": built_at,
        }

    def get_extent(
        self,
        version_id: str,
        attribute_ids: Sequence[str],
        as_of: datetime | str | None = None,
    ) -> list[str]:
        context = self._context_for_version(version_id, as_of=as_of)
        return list(extent_from_attributes(context, list(attribute_ids)))

    def get_intent(
        self,
        version_id: str,
        object_ids: Sequence[str],
        as_of: datetime | str | None = None,
    ) -> list[str]:
        context = self._context_for_version(version_id, as_of=as_of)
        return list(intent_from_objects(context, list(object_ids)))

    def suggest_split_attributes(
        self,
        version_id: str,
        current_attribute_ids: Sequence[str],
        remaining_object_ids: Sequence[str] | None = None,
        as_of: datetime | str | None = None,
    ) -> list[str]:
        context = self._context_for_version(version_id, as_of=as_of)
        selected_objects = (
            list(remaining_object_ids)
            if remaining_object_ids is not None
            else self.get_extent(version_id, current_attribute_ids, as_of=as_of)
        )

        if not selected_objects:
            return []

        object_lookup = {obj_id: idx for idx, obj_id in enumerate(context.object_ids)}
        selected_indices = [object_lookup[obj_id] for obj_id in selected_objects if obj_id in object_lookup]
        if not selected_indices:
            return []

        selected_count = len(selected_indices)
        selected_set = set(current_attribute_ids)
        ranked: list[tuple[float, str]] = []
        fallback: list[tuple[int, str]] = []

        for attr_idx, attr_id in enumerate(context.attribute_ids):
            if attr_id in selected_set:
                continue

            support = 0
            bit = 1 << attr_idx
            for obj_idx in selected_indices:
                if context.object_attribute_masks[obj_idx] & bit:
                    support += 1

            if 0 < support < selected_count:
                # Smaller distance from 50/50 split ranks higher.
                split_score = abs((support / selected_count) - 0.5)
                ranked.append((split_score, attr_id))
            elif support > 0:
                fallback.append((-support, attr_id))

        ranked.sort(key=lambda item: (item[0], item[1]))
        fallback.sort()
        return [attr_id for _, attr_id in ranked] + [attr_id for _, attr_id in fallback]

    def diff_versions(
        self,
        v1: str,
        v2: str,
        as_of: datetime | str | None = None,
    ) -> dict[str, Any]:
        objects_1, attrs_1, edges_1, _ = self._effective_state(v1, as_of=as_of)
        objects_2, attrs_2, edges_2, _ = self._effective_state(v2, as_of=as_of)

        set_objects_1 = set(objects_1)
        set_objects_2 = set(objects_2)
        set_attrs_1 = set(attrs_1)
        set_attrs_2 = set(attrs_2)

        added_edges = sorted(edges_2 - edges_1)
        removed_edges = sorted(edges_1 - edges_2)

        return {
            "v1": v1,
            "v2": v2,
            "added_objects": sorted(set_objects_2 - set_objects_1),
            "removed_objects": sorted(set_objects_1 - set_objects_2),
            "added_attributes": sorted(set_attrs_2 - set_attrs_1),
            "removed_attributes": sorted(set_attrs_1 - set_attrs_2),
            "added_edges": [
                {"object_id": object_id, "attribute_id": attribute_id}
                for object_id, attribute_id in added_edges
            ],
            "removed_edges": [
                {"object_id": object_id, "attribute_id": attribute_id}
                for object_id, attribute_id in removed_edges
            ],
        }

    def list_versions(self, environment: str | None = None) -> list[dict[str, Any]]:
        if environment is None:
            rows = self._conn.execute(
                """
                SELECT v.version_id, v.environment, v.base_version_id, v.overlay, v.created_at, v.updated_at,
                       lm.method AS lattice_method, lm.concept_count, lm.built_at
                FROM versions v
                LEFT JOIN lattice_meta lm ON lm.version_id = v.version_id
                ORDER BY v.version_id ASC
                """
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT v.version_id, v.environment, v.base_version_id, v.overlay, v.created_at, v.updated_at,
                       lm.method AS lattice_method, lm.concept_count, lm.built_at
                FROM versions v
                LEFT JOIN lattice_meta lm ON lm.version_id = v.version_id
                WHERE v.environment = ?
                ORDER BY v.version_id ASC
                """,
                (environment,),
            ).fetchall()

        return [
            {
                "version_id": row["version_id"],
                "environment": row["environment"],
                "base_version_id": row["base_version_id"],
                "overlay": bool(row["overlay"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "lattice": {
                    "built": row["built_at"] is not None,
                    "method": row["lattice_method"],
                    "concept_count": row["concept_count"],
                    "built_at": row["built_at"],
                },
            }
            for row in rows
        ]

    def get_lattice_status(self, version_id: str) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT method, concept_count, built_at FROM lattice_meta WHERE version_id = ?",
            (version_id,),
        ).fetchone()
        if row is None:
            return {
                "version_id": version_id,
                "built": False,
                "method": None,
                "concept_count": 0,
                "built_at": None,
            }
        return {
            "version_id": version_id,
            "built": True,
            "method": row["method"],
            "concept_count": row["concept_count"],
            "built_at": row["built_at"],
        }

    def get_lattice_concepts(self, version_id: str) -> list[FormalConcept]:
        rows = self._conn.execute(
            """
            SELECT intent_json, extent_json
            FROM lattice_concepts
            WHERE version_id = ?
            ORDER BY concept_idx ASC
            """,
            (version_id,),
        ).fetchall()
        concepts: list[FormalConcept] = []
        for row in rows:
            concepts.append(
                FormalConcept(
                    intent=tuple(json.loads(row["intent_json"])),
                    extent=tuple(json.loads(row["extent_json"])),
                )
            )
        return concepts
