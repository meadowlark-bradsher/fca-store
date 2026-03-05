from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Sequence


class FCAStore(ABC):
    @abstractmethod
    def put_incidence(
        self,
        version_id: str,
        objects: Sequence[str],
        attributes: Sequence[str],
        edges_present: Sequence[tuple[str, str]],
        edges_absent: Sequence[tuple[str, str]] | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def build_lattice(self, version_id: str, method: str = "nextclosure") -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_extent(
        self,
        version_id: str,
        attribute_ids: Sequence[str],
        as_of: datetime | str | None = None,
    ) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_intent(
        self,
        version_id: str,
        object_ids: Sequence[str],
        as_of: datetime | str | None = None,
    ) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def suggest_split_attributes(
        self,
        version_id: str,
        current_attribute_ids: Sequence[str],
        remaining_object_ids: Sequence[str] | None = None,
        as_of: datetime | str | None = None,
    ) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def diff_versions(self, v1: str, v2: str, as_of: datetime | str | None = None) -> dict[str, Any]:
        raise NotImplementedError
