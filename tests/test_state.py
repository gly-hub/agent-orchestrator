"""Tests for state.py — path resolution, template rendering, condition evaluation."""

from __future__ import annotations

import pytest

from agent_orchestrator.exceptions import StateResolutionError
from agent_orchestrator.state import (
    evaluate_when,
    get_path,
    render_template,
    set_path,
    validate_when_syntax,
)


class TestGetPath:
    def test_simple_key(self) -> None:
        assert get_path({"a": 1}, "a") == 1

    def test_nested_key(self) -> None:
        assert get_path({"a": {"b": {"c": 3}}}, "a.b.c") == 3

    def test_list_index(self) -> None:
        assert get_path({"items": [10, 20, 30]}, "items.1") == 20

    def test_list_index_out_of_range(self) -> None:
        with pytest.raises(StateResolutionError, match="list index out of range"):
            get_path({"items": [1]}, "items.5")

    def test_missing_key(self) -> None:
        with pytest.raises(StateResolutionError, match="path not found"):
            get_path({"a": 1}, "b")

    def test_empty_segment(self) -> None:
        with pytest.raises(StateResolutionError, match="invalid empty path segment"):
            get_path({"a": 1}, "a..b")

    def test_nested_list_and_dict(self) -> None:
        data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
        assert get_path(data, "users.0.name") == "Alice"
        assert get_path(data, "users.1.name") == "Bob"

    def test_returns_complex_value(self) -> None:
        data = {"a": {"b": [1, 2]}}
        assert get_path(data, "a.b") == [1, 2]


class TestSetPath:
    def test_simple_set(self) -> None:
        data: dict = {}
        set_path(data, "a", 1)
        assert data == {"a": 1}

    def test_nested_set_creates_intermediates(self) -> None:
        data: dict = {}
        set_path(data, "a.b.c", 42)
        assert data == {"a": {"b": {"c": 42}}}

    def test_overwrites_non_dict(self) -> None:
        data: dict = {"a": "old"}
        set_path(data, "a.b", "new")
        assert data == {"a": {"b": "new"}}

    def test_empty_path_raises(self) -> None:
        with pytest.raises(StateResolutionError, match="cannot set empty path"):
            set_path({}, "", 1)


class TestRenderTemplate:
    def test_whole_string_template(self) -> None:
        result = render_template("{{x}}", {"x": 42})
        assert result == 42

    def test_whole_string_preserves_type(self) -> None:
        result = render_template("{{items}}", {"items": [1, 2, 3]})
        assert result == [1, 2, 3]

    def test_inline_template(self) -> None:
        result = render_template("hello {{name}}!", {"name": "world"})
        assert result == "hello world!"

    def test_multiple_inline(self) -> None:
        result = render_template("{{a}}-{{b}}", {"a": "x", "b": "y"})
        assert result == "x-y"

    def test_nested_dict(self) -> None:
        template = {"key": "{{val}}"}
        result = render_template(template, {"val": 10})
        assert result == {"key": 10}

    def test_nested_list(self) -> None:
        template = ["{{a}}", "{{b}}"]
        result = render_template(template, {"a": 1, "b": 2})
        assert result == [1, 2]

    def test_non_string_passthrough(self) -> None:
        assert render_template(42, {}) == 42
        assert render_template(None, {}) is None
        assert render_template(True, {}) is True

    def test_default_filter(self) -> None:
        result = render_template("{{missing | default('fallback')}}", {})
        assert result == "fallback"

    def test_default_filter_not_used_when_present(self) -> None:
        result = render_template("{{x | default('fb')}}", {"x": "real"})
        assert result == "real"

    def test_missing_path_raises(self) -> None:
        with pytest.raises(StateResolutionError, match="path not found"):
            render_template("{{no.such.path}}", {})

    def test_inline_none_becomes_empty(self) -> None:
        result = render_template("val={{x}}", {"x": None})
        assert result == "val="

    def test_deep_copy(self) -> None:
        original = [1, 2]
        result = render_template("{{data}}", {"data": original})
        result.append(3)
        assert original == [1, 2]


class TestEvaluateWhen:
    def test_none_is_true(self) -> None:
        assert evaluate_when(None, {}) is True

    def test_empty_is_true(self) -> None:
        assert evaluate_when("", {}) is True

    def test_equals(self) -> None:
        state = {"status": "done"}
        assert evaluate_when("{{status}} == 'done'", state) is True
        assert evaluate_when("{{status}} == 'pending'", state) is False

    def test_not_equals(self) -> None:
        state = {"x": 1}
        assert evaluate_when("{{x}} != 2", state) is True
        assert evaluate_when("{{x}} != 1", state) is False

    def test_greater_than(self) -> None:
        state = {"score": 10}
        assert evaluate_when("{{score}} > 5", state) is True
        assert evaluate_when("{{score}} > 10", state) is False

    def test_greater_equal(self) -> None:
        state = {"score": 10}
        assert evaluate_when("{{score}} >= 10", state) is True
        assert evaluate_when("{{score}} >= 11", state) is False

    def test_less_than(self) -> None:
        state = {"score": 3}
        assert evaluate_when("{{score}} < 5", state) is True
        assert evaluate_when("{{score}} < 3", state) is False

    def test_less_equal(self) -> None:
        state = {"score": 3}
        assert evaluate_when("{{score}} <= 3", state) is True
        assert evaluate_when("{{score}} <= 2", state) is False

    def test_in_list(self) -> None:
        state = {"role": "admin"}
        assert evaluate_when("{{role}} in ['admin', 'superuser']", state) is True
        assert evaluate_when("{{role}} in ['user', 'guest']", state) is False

    def test_not_in_list(self) -> None:
        state = {"role": "guest"}
        assert evaluate_when("{{role}} not in ['admin', 'superuser']", state) is True
        assert evaluate_when("{{role}} not in ['guest', 'user']", state) is False

    def test_and(self) -> None:
        state = {"a": True, "b": True, "c": False}
        assert evaluate_when("{{a}} and {{b}}", state) is True
        assert evaluate_when("{{a}} and {{c}}", state) is False

    def test_or(self) -> None:
        state = {"a": False, "b": True, "c": False}
        assert evaluate_when("{{a}} or {{b}}", state) is True
        assert evaluate_when("{{a}} or {{c}}", state) is False

    def test_parenthesized(self) -> None:
        state = {"x": 5}
        assert evaluate_when("({{x}} > 3)", state) is True
        assert evaluate_when("({{x}} < 3)", state) is False

    def test_nested_parentheses_with_combinators(self) -> None:
        state = {"a": 1, "b": 2, "c": 3}
        assert evaluate_when("({{a}} == 1) and ({{b}} == 2 or {{c}} == 0)", state) is True

    def test_truthiness_check(self) -> None:
        assert evaluate_when("{{x}}", {"x": "yes"}) is True
        assert evaluate_when("{{x}}", {"x": ""}) is False
        assert evaluate_when("{{x}}", {"x": 0}) is False
        assert evaluate_when("{{x}}", {"x": 1}) is True

    def test_comparison_type_error(self) -> None:
        state = {"x": "text"}
        with pytest.raises(StateResolutionError, match="cannot compare values"):
            evaluate_when("{{x}} > 5", state)

    def test_boolean_literal_comparison(self) -> None:
        state = {"flag": True}
        assert evaluate_when("{{flag}} == true", state) is True

    def test_null_literal_comparison(self) -> None:
        state = {"val": None}
        assert evaluate_when("{{val}} == null", state) is True

    def test_numeric_literal(self) -> None:
        state = {"n": 3.14}
        assert evaluate_when("{{n}} > 3.0", state) is True

    def test_empty_list_literal(self) -> None:
        state = {"role": "admin"}
        assert evaluate_when("{{role}} in []", state) is False


class TestValidateWhenSyntax:
    def test_none_ok(self) -> None:
        validate_when_syntax(None)

    def test_empty_ok(self) -> None:
        validate_when_syntax("")
        validate_when_syntax("  ")

    def test_valid_comparison(self) -> None:
        validate_when_syntax("{{x}} == 'done'")

    def test_valid_truthiness(self) -> None:
        validate_when_syntax("{{x}}")

    def test_bare_string_rejected(self) -> None:
        with pytest.raises(StateResolutionError, match="truthiness checks must be whole templates"):
            validate_when_syntax("not_a_template")

    def test_empty_around_and(self) -> None:
        with pytest.raises(StateResolutionError, match="empty condition around and"):
            validate_when_syntax("and ")

    def test_empty_around_or(self) -> None:
        with pytest.raises(StateResolutionError, match="empty condition around or"):
            validate_when_syntax("or ")

    def test_unsupported_literal(self) -> None:
        with pytest.raises(StateResolutionError, match="unsupported condition literal"):
            validate_when_syntax("{{x}} == not_a_number_or_quoted")

    def test_empty_operand(self) -> None:
        with pytest.raises(StateResolutionError):
            validate_when_syntax("{{x}} == ")

    def test_valid_complex(self) -> None:
        validate_when_syntax("({{a}} == 1) and ({{b}} in ['x', 'y'])")
