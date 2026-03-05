from __future__ import annotations

import time

from fca_store.sqlite_store import SQLiteFCAStore


def _animals_fixture() -> tuple[list[str], list[str], list[tuple[str, str]]]:
    objects = ["cat", "dog", "eagle", "shark"]
    attributes = ["mammal", "has_fur", "can_fly", "can_swim", "predator"]
    edges_present = [
        ("cat", "mammal"),
        ("cat", "has_fur"),
        ("cat", "predator"),
        ("dog", "mammal"),
        ("dog", "has_fur"),
        ("dog", "predator"),
        ("eagle", "can_fly"),
        ("eagle", "predator"),
        ("shark", "can_swim"),
        ("shark", "predator"),
    ]
    return objects, attributes, edges_present


def test_animals_extent_and_intent_queries(tmp_path):
    store = SQLiteFCAStore(tmp_path / "fca.db")
    objects, attributes, edges_present = _animals_fixture()
    store.put_incidence("animals_v1", objects, attributes, edges_present)

    assert store.get_extent("animals_v1", ["mammal"]) == ["cat", "dog"]
    assert store.get_extent("animals_v1", ["predator"]) == ["cat", "dog", "eagle", "shark"]
    assert store.get_intent("animals_v1", ["cat", "dog"]) == ["has_fur", "mammal", "predator"]
    assert store.get_extent("animals_v1", []) == ["cat", "dog", "eagle", "shark"]
    assert store.get_intent("animals_v1", []) == [
        "can_fly",
        "can_swim",
        "has_fur",
        "mammal",
        "predator",
    ]


def test_lattice_build_and_closure_properties(tmp_path):
    store = SQLiteFCAStore(tmp_path / "fca.db")
    objects, attributes, edges_present = _animals_fixture()
    store.put_incidence("animals_v1", objects, attributes, edges_present)

    result = store.build_lattice("animals_v1", method="nextclosure")
    assert result["concept_count"] > 0

    concepts = store.get_lattice_concepts("animals_v1")
    assert ("has_fur", "mammal", "predator") in {concept.intent for concept in concepts}

    for concept in concepts:
        assert set(store.get_extent("animals_v1", list(concept.intent))) == set(concept.extent)
        assert set(store.get_intent("animals_v1", list(concept.extent))) == set(concept.intent)


def test_overlay_diff_and_absent_edge(tmp_path):
    store = SQLiteFCAStore(tmp_path / "fca.db")
    objects, attributes, edges_present = _animals_fixture()
    store.put_incidence("animals_v1", objects, attributes, edges_present)

    store.put_incidence(
        "animals_v2",
        objects=["bat"],
        attributes=[],
        edges_present=[("bat", "mammal"), ("bat", "can_fly"), ("bat", "predator")],
        edges_absent=[("dog", "has_fur")],
        meta={"base_version_id": "animals_v1", "overlay": True, "environment": "dev"},
    )

    assert store.get_extent("animals_v2", ["has_fur"]) == ["cat"]
    diff = store.diff_versions("animals_v1", "animals_v2")

    assert diff["added_objects"] == ["bat"]
    assert {"object_id": "dog", "attribute_id": "has_fur"} in diff["removed_edges"]
    assert {"object_id": "bat", "attribute_id": "mammal"} in diff["added_edges"]


def test_validity_filtering(tmp_path):
    store = SQLiteFCAStore(tmp_path / "fca.db")
    store.put_incidence(
        "v1",
        objects=["o1"],
        attributes=["a1"],
        edges_present=[("o1", "a1")],
    )
    store.put_incidence(
        "v2",
        objects=[],
        attributes=[],
        edges_present=[],
        edges_absent=[("o1", "a1")],
        meta={
            "base_version_id": "v1",
            "overlay": True,
            "valid_from": "2026-01-01T00:00:00Z",
        },
    )

    assert store.get_extent("v2", ["a1"], as_of="2025-12-31T00:00:00Z") == ["o1"]
    assert store.get_extent("v2", ["a1"], as_of="2026-01-02T00:00:00Z") == []


def test_performance_smoke_1k_by_200_sparse(tmp_path):
    store = SQLiteFCAStore(tmp_path / "fca.db")

    object_count = 1_000
    attribute_count = 200
    objects = [f"obj_{i:04d}" for i in range(object_count)]
    attributes = [f"attr_{i:03d}" for i in range(attribute_count)]

    edges_present: list[tuple[str, str]] = []
    for idx, object_id in enumerate(objects):
        prefix_size = idx % 20
        for attr_idx in range(prefix_size):
            edges_present.append((object_id, attributes[attr_idx]))

    store.put_incidence("perf_v1", objects, attributes, edges_present)

    started = time.monotonic()
    lattice = store.build_lattice("perf_v1")
    elapsed = time.monotonic() - started

    assert lattice["concept_count"] <= 300
    assert elapsed < 30
    assert len(store.get_extent("perf_v1", [attributes[0]])) > 0
