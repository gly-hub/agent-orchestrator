"""Shared state path and template helpers."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from agent_orchestrator.exceptions import StateResolutionError

_EXPR_RE = re.compile(r"^\s*\{\{\s*([^{}]+?)\s*\}\}\s*$")
_INLINE_EXPR_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")


def get_path(data: dict[str, Any], path: str) -> Any:
    """Resolve a dotted path from a mapping."""

    current: Any = data
    for part in path.split("."):
        part = part.strip()
        if not part:
            raise StateResolutionError(f"invalid empty path segment in {path!r}")
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            idx = int(part)
            try:
                current = current[idx]
            except IndexError as exc:
                raise StateResolutionError(f"list index out of range: {path!r}") from exc
            continue
        raise StateResolutionError(f"path not found: {path!r}")
    return current


def set_path(data: dict[str, Any], path: str, value: Any) -> None:
    """Set a dotted path into a mapping, creating intermediate dictionaries."""

    parts = [part.strip() for part in path.split(".") if part.strip()]
    if not parts:
        raise StateResolutionError("cannot set empty path")

    current = data
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            child = {}
            current[part] = child
        current = child
    current[parts[-1]] = value


def render_template(value: Any, state: dict[str, Any]) -> Any:
    """Render {{path.to.value}} templates inside nested data structures."""

    if isinstance(value, dict):
        return {key: render_template(item, state) for key, item in value.items()}
    if isinstance(value, list):
        return [render_template(item, state) for item in value]
    if not isinstance(value, str):
        return deepcopy(value)

    whole_match = _EXPR_RE.match(value)
    if whole_match:
        return deepcopy(_resolve_expression(state, whole_match.group(1).strip()))

    def replace(match: re.Match[str]) -> str:
        resolved = _resolve_expression(state, match.group(1).strip())
        return "" if resolved is None else str(resolved)

    return _INLINE_EXPR_RE.sub(replace, value)


def _resolve_expression(state: dict[str, Any], expression: str) -> Any:
    path, default = _split_default(expression)
    try:
        return get_path(state, path)
    except StateResolutionError:
        if default is _MISSING:
            raise
        return default


def evaluate_when(expression: str | None, state: dict[str, Any]) -> bool:
    """Evaluate a small, explicit condition expression.

    Supported forms:
    - absent/empty expression: true
    - {{path}} == literal
    - {{path}} != literal
    - {{path}} >, >=, <, <= literal
    - {{path}} in [literal, ...]
    - {{path}} not in [literal, ...]
    - expression and expression
    - expression or expression
    - (expression) for grouping
    - {{path}} as a truthiness check
    """

    if not expression:
        return True

    expr = expression.strip()

    paren_inner = _extract_parenthesized(expr)
    if paren_inner is not None:
        return evaluate_when(paren_inner, state)

    or_parts = _split_keyword(expr, "or")
    if len(or_parts) > 1:
        return any(evaluate_when(part, state) for part in or_parts)

    and_parts = _split_keyword(expr, "and")
    if len(and_parts) > 1:
        return all(evaluate_when(part, state) for part in and_parts)

    for operator in ("not in", "in", ">=", "<=", "==", "!=", ">", "<"):
        split = _split_operator(expr, operator)
        if split:
            left, right = split
            left_value = _parse_operand(left.strip(), state)
            right_value = _parse_operand(right.strip(), state)
            return _compare_values(left_value, right_value, operator)

    return bool(render_template(expr, state))


def validate_when_syntax(expression: str | None) -> None:
    """Validate the small condition grammar accepted by ``evaluate_when``."""

    if expression is None:
        return
    expr = expression.strip()
    if not expr:
        return

    _validate_when_clause(expr)


def _validate_when_clause(expression: str) -> None:
    for keyword in ("or", "and"):
        if (
            expression == keyword
            or expression.startswith(f"{keyword} ")
            or expression.endswith(f" {keyword}")
        ):
            raise StateResolutionError(f"empty condition around {keyword}")

    paren_inner = _extract_parenthesized(expression)
    if paren_inner is not None:
        _validate_when_clause(paren_inner)
        return

    or_parts = _split_keyword(expression, "or")
    if len(or_parts) > 1:
        for part in or_parts:
            _validate_non_empty_clause(part, "or")
            _validate_when_clause(part)
        return

    and_parts = _split_keyword(expression, "and")
    if len(and_parts) > 1:
        for part in and_parts:
            _validate_non_empty_clause(part, "and")
            _validate_when_clause(part)
        return

    for operator in ("not in", "in", ">=", "<=", "==", "!=", ">", "<"):
        split = _split_operator(expression, operator)
        if split:
            left, right = split
            _validate_operand(left.strip(), allow_template=True)
            _validate_operand(right.strip(), allow_template=True)
            return

    if not _is_whole_template(expression):
        raise StateResolutionError("truthiness checks must be whole templates like {{path.to.value}}")


def _validate_non_empty_clause(part: str, operator: str) -> None:
    if not part.strip():
        raise StateResolutionError(f"empty condition around {operator}")


def _validate_operand(value: str, *, allow_template: bool) -> None:
    if not value:
        raise StateResolutionError("empty condition operand")
    if allow_template and _is_whole_template(value):
        return
    if "{{" in value or "}}" in value:
        raise StateResolutionError("templates in conditions must occupy the whole operand")
    _parse_literal_strict(value)


def _is_whole_template(value: str) -> bool:
    return _EXPR_RE.match(value) is not None


def _parse_operand(value: str, state: dict[str, Any]) -> Any:
    if _INLINE_EXPR_RE.search(value):
        return render_template(value, state)
    return _parse_literal(value)


def _compare_values(left: Any, right: Any, operator: str) -> bool:
    if operator == "==":
        return left == right
    if operator == "!=":
        return left != right
    if operator == "in":
        return left in right
    if operator == "not in":
        return left not in right
    if operator in {">", ">=", "<", "<="}:
        try:
            if operator == ">":
                return left > right
            if operator == ">=":
                return left >= right
            if operator == "<":
                return left < right
            if operator == "<=":
                return left <= right
        except TypeError as exc:
            raise StateResolutionError(
                f"cannot compare values with {operator}: {left!r}, {right!r}"
            ) from exc
    raise StateResolutionError(f"unsupported operator: {operator}")


def _parse_literal(value: str) -> Any:
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_literal(part.strip()) for part in _split_csv(inner)]

    quoted = (
        (value.startswith("'") and value.endswith("'"))
        or (value.startswith('"') and value.endswith('"'))
    )
    if quoted:
        return value[1:-1]
    if value == "true":
        return True
    if value == "false":
        return False
    if value == "null":
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _parse_literal_strict(value: str) -> Any:
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        for part in _split_csv(inner):
            _parse_literal_strict(part.strip())
        return []

    quoted = (
        (value.startswith("'") and value.endswith("'"))
        or (value.startswith('"') and value.endswith('"'))
    )
    if quoted:
        return value[1:-1]
    if value in {"true", "false", "null"}:
        return _parse_literal(value)
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError as exc:
            raise StateResolutionError(f"unsupported condition literal: {value!r}") from exc


def _split_operator(expression: str, operator: str) -> tuple[str, str] | None:
    token = operator if operator in {">=", "<=", "==", "!=", ">", "<"} else f" {operator} "
    index = _find_top_level_token(expression, token, require_word_boundary=False)
    if index < 0:
        return None
    return expression[:index], expression[index + len(token):]


def _split_keyword(expression: str, keyword: str) -> list[str]:
    token = f" {keyword} "
    parts: list[str] = []
    start = 0
    while True:
        index = _find_top_level_token(expression, token, start=start, require_word_boundary=False)
        if index < 0:
            break
        parts.append(expression[start:index].strip())
        start = index + len(token)
    if not parts:
        return [expression]
    parts.append(expression[start:].strip())
    return parts


def _split_csv(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    quote: str | None = None
    bracket_depth = 0
    for index, char in enumerate(value):
        if quote:
            if char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            continue
        if char == "[":
            bracket_depth += 1
            continue
        if char == "]":
            bracket_depth -= 1
            continue
        if char == "," and bracket_depth == 0:
            parts.append(value[start:index])
            start = index + 1
    parts.append(value[start:])
    return parts


def _find_top_level_token(
    expression: str,
    token: str,
    *,
    start: int = 0,
    require_word_boundary: bool = False,
) -> int:
    quote: str | None = None
    bracket_depth = 0
    brace_depth = 0
    paren_depth = 0
    index = start
    while index <= len(expression) - len(token):
        char = expression[index]
        if quote:
            if char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            index += 1
            continue
        if expression.startswith("{{", index):
            brace_depth += 1
            index += 2
            continue
        if expression.startswith("}}", index) and brace_depth > 0:
            brace_depth -= 1
            index += 2
            continue
        if char == "(":
            paren_depth += 1
            index += 1
            continue
        if char == ")":
            paren_depth -= 1
            index += 1
            continue
        if char == "[":
            bracket_depth += 1
            index += 1
            continue
        if char == "]":
            bracket_depth -= 1
            index += 1
            continue
        if (
            bracket_depth == 0
            and brace_depth == 0
            and paren_depth == 0
            and expression.startswith(token, index)
            and (
                not require_word_boundary
                or _has_word_boundaries(expression, index, index + len(token))
            )
        ):
            return index
        index += 1
    return -1


def _extract_parenthesized(expression: str) -> str | None:
    """If the entire expression is wrapped in matching parentheses, return the inner content."""

    expr = expression.strip()
    if not expr.startswith("(") or not expr.endswith(")"):
        return None
    depth = 0
    for index, char in enumerate(expr):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if depth == 0 and index < len(expr) - 1:
            return None
    if depth != 0:
        return None
    return expr[1:-1].strip()


def _has_word_boundaries(expression: str, start: int, end: int) -> bool:
    left_ok = start == 0 or not expression[start - 1].isalnum()
    right_ok = end >= len(expression) or not expression[end].isalnum()
    return left_ok and right_ok


_MISSING = object()


def _split_default(expression: str) -> tuple[str, Any]:
    marker = "| default("
    if marker not in expression:
        return expression, _MISSING
    path, remainder = expression.split(marker, 1)
    if not remainder.endswith(")"):
        return expression, _MISSING
    return path.strip(), _parse_literal(remainder[:-1].strip())
