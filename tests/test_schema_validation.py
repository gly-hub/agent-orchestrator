"""Tests for schema.py — runtime schema validation."""

from __future__ import annotations

import pytest

from agent_orchestrator.exceptions import WorkflowError
from agent_orchestrator.schema import validate_schema_value


class TestTypeChecks:
    def test_string(self) -> None:
        validate_schema_value("hello", {"type": "string"}, label="v")

    def test_string_rejects_int(self) -> None:
        with pytest.raises(WorkflowError, match="v must be string"):
            validate_schema_value(42, {"type": "string"}, label="v")

    def test_number_accepts_int(self) -> None:
        validate_schema_value(42, {"type": "number"}, label="v")

    def test_number_accepts_float(self) -> None:
        validate_schema_value(3.14, {"type": "number"}, label="v")

    def test_number_rejects_bool(self) -> None:
        with pytest.raises(WorkflowError, match="v must be number"):
            validate_schema_value(True, {"type": "number"}, label="v")

    def test_integer(self) -> None:
        validate_schema_value(5, {"type": "integer"}, label="v")

    def test_integer_rejects_float(self) -> None:
        with pytest.raises(WorkflowError, match="v must be integer"):
            validate_schema_value(5.5, {"type": "integer"}, label="v")

    def test_integer_rejects_bool(self) -> None:
        with pytest.raises(WorkflowError, match="v must be integer"):
            validate_schema_value(True, {"type": "integer"}, label="v")

    def test_boolean(self) -> None:
        validate_schema_value(True, {"type": "boolean"}, label="v")
        validate_schema_value(False, {"type": "boolean"}, label="v")

    def test_boolean_rejects_int(self) -> None:
        with pytest.raises(WorkflowError, match="v must be boolean"):
            validate_schema_value(1, {"type": "boolean"}, label="v")

    def test_object(self) -> None:
        validate_schema_value({"a": 1}, {"type": "object"}, label="v")

    def test_object_rejects_list(self) -> None:
        with pytest.raises(WorkflowError, match="v must be object"):
            validate_schema_value([1, 2], {"type": "object"}, label="v")

    def test_array(self) -> None:
        validate_schema_value([1, 2], {"type": "array"}, label="v")

    def test_array_rejects_dict(self) -> None:
        with pytest.raises(WorkflowError, match="v must be array"):
            validate_schema_value({"a": 1}, {"type": "array"}, label="v")

    def test_null(self) -> None:
        validate_schema_value(None, {"type": "null"}, label="v")

    def test_null_rejects_string(self) -> None:
        with pytest.raises(WorkflowError, match="v must be null"):
            validate_schema_value("", {"type": "null"}, label="v")


class TestStringConstraints:
    def test_min_length(self) -> None:
        validate_schema_value("abc", {"type": "string", "minLength": 3}, label="v")

    def test_min_length_violation(self) -> None:
        with pytest.raises(WorkflowError, match="length must be >= 3"):
            validate_schema_value("ab", {"type": "string", "minLength": 3}, label="v")

    def test_max_length(self) -> None:
        validate_schema_value("ab", {"type": "string", "maxLength": 5}, label="v")

    def test_max_length_violation(self) -> None:
        with pytest.raises(WorkflowError, match="length must be <= 2"):
            validate_schema_value("abc", {"type": "string", "maxLength": 2}, label="v")


class TestNumberConstraints:
    def test_minimum(self) -> None:
        validate_schema_value(5, {"type": "number", "minimum": 5}, label="v")

    def test_minimum_violation(self) -> None:
        with pytest.raises(WorkflowError, match="must be >= 5"):
            validate_schema_value(4, {"type": "number", "minimum": 5}, label="v")

    def test_maximum(self) -> None:
        validate_schema_value(10, {"type": "number", "maximum": 10}, label="v")

    def test_maximum_violation(self) -> None:
        with pytest.raises(WorkflowError, match="must be <= 10"):
            validate_schema_value(11, {"type": "number", "maximum": 10}, label="v")


class TestEnumValidation:
    def test_valid_enum(self) -> None:
        validate_schema_value("a", {"enum": ["a", "b", "c"]}, label="v")

    def test_invalid_enum(self) -> None:
        with pytest.raises(WorkflowError, match="must be one of"):
            validate_schema_value("d", {"enum": ["a", "b", "c"]}, label="v")


class TestObjectValidation:
    def test_required_fields(self) -> None:
        schema = {"type": "object", "required": ["name"]}
        validate_schema_value({"name": "Alice"}, schema, label="v")

    def test_missing_required_field(self) -> None:
        schema = {"type": "object", "required": ["name"]}
        with pytest.raises(WorkflowError, match="missing required field: name"):
            validate_schema_value({}, schema, label="v")

    def test_additional_properties_false(self) -> None:
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "additionalProperties": False,
        }
        with pytest.raises(WorkflowError, match="unsupported field"):
            validate_schema_value({"name": "ok", "extra": 1}, schema, label="v")

    def test_additional_properties_true(self) -> None:
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "additionalProperties": True,
        }
        validate_schema_value({"name": "ok", "extra": 1}, schema, label="v")

    def test_additional_properties_schema(self) -> None:
        schema = {
            "type": "object",
            "properties": {},
            "additionalProperties": {"type": "integer"},
        }
        validate_schema_value({"count": 5}, schema, label="v")
        with pytest.raises(WorkflowError, match="must be integer"):
            validate_schema_value({"count": "five"}, schema, label="v")

    def test_nested_property_validation(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "age": {"type": "integer", "minimum": 0},
            },
        }
        validate_schema_value({"age": 25}, schema, label="v")
        with pytest.raises(WorkflowError, match="must be >= 0"):
            validate_schema_value({"age": -1}, schema, label="v")

    def test_properties_not_dict_raises(self) -> None:
        schema = {"type": "object", "properties": "bad"}
        with pytest.raises(WorkflowError, match="properties must be a mapping"):
            validate_schema_value({"x": 1}, schema, label="v")

    def test_property_spec_not_dict_raises(self) -> None:
        schema = {"type": "object", "properties": {"x": "bad"}}
        with pytest.raises(WorkflowError, match="schema must be a mapping"):
            validate_schema_value({"x": 1}, schema, label="v")


class TestArrayValidation:
    def test_items_schema(self) -> None:
        schema = {"type": "array", "items": {"type": "integer"}}
        validate_schema_value([1, 2, 3], schema, label="v")

    def test_items_schema_violation(self) -> None:
        schema = {"type": "array", "items": {"type": "integer"}}
        with pytest.raises(WorkflowError, match="must be integer"):
            validate_schema_value([1, "two", 3], schema, label="v")

    def test_no_items_schema(self) -> None:
        schema = {"type": "array"}
        validate_schema_value([1, "two", None], schema, label="v")

    def test_items_not_dict_raises(self) -> None:
        schema = {"type": "array", "items": "bad"}
        with pytest.raises(WorkflowError, match="items must be a mapping"):
            validate_schema_value([1], schema, label="v")


class TestEdgeCases:
    def test_none_schema_skips_validation(self) -> None:
        validate_schema_value("anything", None, label="v")

    def test_empty_schema_skips_validation(self) -> None:
        validate_schema_value("anything", {}, label="v")

    def test_schema_not_dict_raises(self) -> None:
        with pytest.raises(WorkflowError, match="schema must be a mapping"):
            validate_schema_value("x", "not_a_dict", label="v")  # type: ignore[arg-type]

    def test_unsupported_schema_key(self) -> None:
        with pytest.raises(WorkflowError, match="unsupported schema keyword"):
            validate_schema_value("x", {"type": "string", "pattern": ".*"}, label="v")

    def test_unknown_type_passes(self) -> None:
        validate_schema_value("anything", {"type": "custom_type"}, label="v")

    def test_deeply_nested(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "users": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["name"],
                        "properties": {
                            "name": {"type": "string", "minLength": 1},
                        },
                    },
                },
            },
        }
        validate_schema_value({"users": [{"name": "Alice"}]}, schema, label="v")
        with pytest.raises(WorkflowError, match="length must be >= 1"):
            validate_schema_value({"users": [{"name": ""}]}, schema, label="v")
