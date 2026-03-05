from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FormalConcept:
    intent: tuple[str, ...]
    extent: tuple[str, ...]


@dataclass(frozen=True)
class FCAContext:
    object_ids: list[str]
    attribute_ids: list[str]
    object_attribute_masks: list[int]
    attribute_object_masks: list[int]

    @property
    def all_objects_mask(self) -> int:
        return (1 << len(self.object_ids)) - 1

    @property
    def all_attributes_mask(self) -> int:
        return (1 << len(self.attribute_ids)) - 1


def build_context(
    object_ids: list[str],
    attribute_ids: list[str],
    present_edges: set[tuple[str, str]],
) -> FCAContext:
    obj_ids = sorted(object_ids)
    attr_ids = sorted(attribute_ids)

    obj_index = {obj_id: idx for idx, obj_id in enumerate(obj_ids)}
    attr_index = {attr_id: idx for idx, attr_id in enumerate(attr_ids)}

    object_attribute_masks = [0] * len(obj_ids)
    attribute_object_masks = [0] * len(attr_ids)
    for obj_id, attr_id in present_edges:
        obj_idx = obj_index.get(obj_id)
        attr_idx = attr_index.get(attr_id)
        if obj_idx is None or attr_idx is None:
            continue
        object_attribute_masks[obj_idx] |= 1 << attr_idx
        attribute_object_masks[attr_idx] |= 1 << obj_idx

    return FCAContext(
        object_ids=obj_ids,
        attribute_ids=attr_ids,
        object_attribute_masks=object_attribute_masks,
        attribute_object_masks=attribute_object_masks,
    )


def mask_to_ids(mask: int, ids: list[str]) -> tuple[str, ...]:
    values: list[str] = []
    bitset = mask
    while bitset:
        low_bit = bitset & -bitset
        idx = low_bit.bit_length() - 1
        values.append(ids[idx])
        bitset ^= low_bit
    return tuple(values)


def closure(context: FCAContext, attribute_mask: int) -> tuple[int, int]:
    extent_mask = context.all_objects_mask
    attrs = attribute_mask
    while attrs:
        low_bit = attrs & -attrs
        idx = low_bit.bit_length() - 1
        extent_mask &= context.attribute_object_masks[idx]
        attrs ^= low_bit

    if extent_mask == 0:
        return context.all_attributes_mask, extent_mask

    intent_mask = context.all_attributes_mask
    objs = extent_mask
    while objs:
        low_bit = objs & -objs
        idx = low_bit.bit_length() - 1
        intent_mask &= context.object_attribute_masks[idx]
        objs ^= low_bit

    return intent_mask, extent_mask


def extent_from_attributes(context: FCAContext, attribute_ids: list[str]) -> tuple[str, ...]:
    if not attribute_ids:
        return tuple(context.object_ids)

    attr_index = {attr_id: idx for idx, attr_id in enumerate(context.attribute_ids)}
    attribute_mask = 0
    for attr_id in attribute_ids:
        idx = attr_index.get(attr_id)
        if idx is None:
            return tuple()
        attribute_mask |= 1 << idx

    extent_mask = context.all_objects_mask
    attrs = attribute_mask
    while attrs:
        low_bit = attrs & -attrs
        idx = low_bit.bit_length() - 1
        extent_mask &= context.attribute_object_masks[idx]
        attrs ^= low_bit

    return mask_to_ids(extent_mask, context.object_ids)


def intent_from_objects(context: FCAContext, object_ids: list[str]) -> tuple[str, ...]:
    if not object_ids:
        return tuple(context.attribute_ids)

    obj_index = {obj_id: idx for idx, obj_id in enumerate(context.object_ids)}
    intent_mask = context.all_attributes_mask
    for obj_id in object_ids:
        idx = obj_index.get(obj_id)
        if idx is None:
            return tuple()
        intent_mask &= context.object_attribute_masks[idx]

    return mask_to_ids(intent_mask, context.attribute_ids)


def build_concepts_nextclosure(
    context: FCAContext,
    max_concepts: int = 100_000,
) -> list[FormalConcept]:
    concepts: list[FormalConcept] = []
    seen: set[int] = set()

    current_intent, current_extent = closure(context, 0)
    while True:
        if current_intent not in seen:
            seen.add(current_intent)
            concepts.append(
                FormalConcept(
                    intent=mask_to_ids(current_intent, context.attribute_ids),
                    extent=mask_to_ids(current_extent, context.object_ids),
                )
            )
            if len(concepts) > max_concepts:
                raise RuntimeError(
                    f"Concept count exceeded max_concepts={max_concepts}. "
                    "Try reducing attribute count or increasing the limit."
                )

        found_next = False
        for idx in range(len(context.attribute_ids) - 1, -1, -1):
            bit = 1 << idx
            if current_intent & bit:
                continue

            lower_prefix = current_intent & (bit - 1)
            candidate_intent, candidate_extent = closure(context, lower_prefix | bit)

            if (candidate_intent & (bit - 1)) == lower_prefix:
                current_intent, current_extent = candidate_intent, candidate_extent
                found_next = True
                break

        if not found_next:
            break

    return concepts
