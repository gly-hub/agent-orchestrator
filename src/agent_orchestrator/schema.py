"""Small schema validation helpers used by workflow boundaries."""

from __future__ import annotations

from typing import Any

from agent_orchestrator.exceptions import WorkflowError

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


def validate_schema_value(value: Any, schema: dict[str, Any] | None, *, label: str) -> None:
    """Validate a value against a small JSON-schema-like subset."""

    if not schema:
        return
    if not isinstance(schema, dict):
        raise WorkflowError(f"{label} schema must be a mapping")
    _validate_schema_keys(schema, label)
    _validate_value(value, schema, label)


def _validate_value(value: Any, schema: dict[str, Any], path: str) -> None:
    _validate_schema_keys(schema, path)
    expected_type = schema.get("type")
    if expected_type and not _matches_type(value, expected_type):
        raise WorkflowError(f"{path} must be {expected_type}")

    enum = schema.get("enum")
    if enum is not None and value not in enum:
        raise WorkflowError(f"{path} must be one of: {', '.join(map(str, enum))}")

    if isinstance(value, str):
        _validate_string(value, schema, path)
    if isinstance(value, int | float) and not isinstance(value, bool):
        _validate_number(value, schema, path)
    if expected_type == "object" or isinstance(value, dict):
        _validate_object(value, schema, path)
    if expected_type == "array" or isinstance(value, list):
        _validate_array(value, schema, path)


def _validate_string(value: str, schema: dict[str, Any], path: str) -> None:
    min_length = schema.get("minLength")
    if min_length is not None and len(value) < int(min_length):
        raise WorkflowError(f"{path} length must be >= {min_length}")
    max_length = schema.get("maxLength")
    if max_length is not None and len(value) > int(max_length):
        raise WorkflowError(f"{path} length must be <= {max_length}")


def _validate_number(value: int | float, schema: dict[str, Any], path: str) -> None:
    minimum = schema.get("minimum")
    if minimum is not None and value < minimum:
        raise WorkflowError(f"{path} must be >= {minimum}")
    maximum = schema.get("maximum")
    if maximum is not None and value > maximum:
        raise WorkflowError(f"{path} must be <= {maximum}")


def _validate_object(value: Any, schema: dict[str, Any], path: str) -> None:
    if not isinstance(value, dict):
        return

    required = schema.get("required", [])
    for field in required:
        if field not in value:
            raise WorkflowError(f"{path} missing required field: {field}")

    properties = schema.get("properties", {})
    if properties is not None and not isinstance(properties, dict):
        raise WorkflowError(f"{path}.properties must be a mapping")
    properties = properties or {}

    for field, spec in properties.items():
        if field not in value:
            continue
        if not isinstance(spec, dict):
            raise WorkflowError(f"{path}.{field} schema must be a mapping")
        _validate_schema_keys(spec, f"{path}.{field}")
        _validate_value(value[field], spec, f"{path}.{field}")

    additional_properties = schema.get("additionalProperties", True)
    if additional_properties is False:
        extra_fields = sorted(set(value) - set(properties))
        if extra_fields:
            raise WorkflowError(f"{path} has unsupported field: {extra_fields[0]}")
    elif isinstance(additional_properties, dict):
        for field in sorted(set(value) - set(properties)):
            _validate_value(value[field], additional_properties, f"{path}.{field}")


def _validate_array(value: Any, schema: dict[str, Any], path: str) -> None:
    if not isinstance(value, list):
        return

    items = schema.get("items")
    if items is None:
        return
    if not isinstance(items, dict):
        raise WorkflowError(f"{path}.items must be a mapping")
    _validate_schema_keys(items, f"{path}.items")
    for index, item in enumerate(value):
        _validate_value(item, items, f"{path}.{index}")


def _validate_schema_keys(schema: dict[str, Any], label: str) -> None:
    unsupported = sorted(set(schema) - SUPPORTED_SCHEMA_KEYS)
    if unsupported:
        raise WorkflowError(f"{label} has unsupported schema keyword: {unsupported[0]}")


def _matches_type(value: object, expected_type: str) -> bool:
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "number":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array":
        return isinstance(value, list)
    if expected_type == "null":
        return value is None
    return True
