"""Workflow config schema validation helpers."""

from __future__ import annotations

from typing import Any

from agent_orchestrator.exceptions import WorkflowConfigError

SUPPORTED_SCHEMA_KEYS = {
    "type",
    "enum",
    "required",
    "properties",
    "additionalProperties",
    "items",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
}


def validate_schema(node_id: str, schema_name: str, schema: Any) -> None:
    if not isinstance(schema, dict):
        raise WorkflowConfigError(f"node {node_id} {schema_name} must be a mapping")
    validate_schema_keys(node_id, schema_name, schema)

    properties = schema.get("properties", {})
    if properties is not None and not isinstance(properties, dict):
        raise WorkflowConfigError(f"node {node_id} {schema_name}.properties must be a mapping")
    properties = properties or {}

    required = schema.get("required", [])
    if required is not None and (
        not isinstance(required, list) or not all(isinstance(item, str) for item in required)
    ):
        raise WorkflowConfigError(f"node {node_id} {schema_name}.required must be a string list")

    additional_properties = schema.get("additionalProperties")
    if additional_properties is not None and not isinstance(additional_properties, bool | dict):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name}.additionalProperties must be a boolean or mapping"
        )
    if isinstance(additional_properties, dict):
        validate_schema_spec(node_id, schema_name, "additionalProperties", additional_properties)

    for field, spec in properties.items():
        if not isinstance(field, str) or not isinstance(spec, dict):
            raise WorkflowConfigError(
                f"node {node_id} {schema_name} property definitions must be mappings"
            )
        validate_schema_spec(node_id, schema_name, field, spec)

    items = schema.get("items")
    if items is not None:
        if not isinstance(items, dict):
            raise WorkflowConfigError(f"node {node_id} {schema_name}.items must be a mapping")
        validate_schema_spec(node_id, schema_name, "items", items)


def validate_schema_spec(
    node_id: str,
    schema_name: str,
    field: str,
    spec: dict[str, Any],
) -> None:
    validate_schema_keys(node_id, f"{schema_name} property {field}", spec)
    expected_type = spec.get("type")
    if expected_type is not None and expected_type not in {
        "string",
        "number",
        "integer",
        "boolean",
        "object",
        "array",
        "null",
    }:
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} has unsupported type"
        )
    enum = spec.get("enum")
    if enum is not None and not isinstance(enum, list):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} enum must be a list"
        )

    min_length = spec.get("minLength")
    if min_length is not None and (not isinstance(min_length, int) or min_length < 0):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} minLength must be >= 0"
        )
    max_length = spec.get("maxLength")
    if max_length is not None and (not isinstance(max_length, int) or max_length < 0):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} maxLength must be >= 0"
        )
    if min_length is not None and max_length is not None and min_length > max_length:
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} minLength must be <= maxLength"
        )

    minimum = spec.get("minimum")
    if minimum is not None and (not isinstance(minimum, int | float) or isinstance(minimum, bool)):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} minimum must be a number"
        )
    maximum = spec.get("maximum")
    if maximum is not None and (not isinstance(maximum, int | float) or isinstance(maximum, bool)):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} maximum must be a number"
        )
    if minimum is not None and maximum is not None and minimum > maximum:
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} minimum must be <= maximum"
        )

    additional_properties = spec.get("additionalProperties")
    if additional_properties is not None and not isinstance(additional_properties, bool | dict):
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} property {field} additionalProperties must be a boolean or mapping"
        )
    if isinstance(additional_properties, dict):
        validate_schema_spec(
            node_id,
            schema_name,
            f"{field}.additionalProperties",
            additional_properties,
        )


def validate_schema_keys(node_id: str, schema_name: str, schema: dict[str, Any]) -> None:
    unsupported = sorted(set(schema) - SUPPORTED_SCHEMA_KEYS)
    if unsupported:
        raise WorkflowConfigError(
            f"node {node_id} {schema_name} has unsupported schema keyword: {unsupported[0]}"
        )
