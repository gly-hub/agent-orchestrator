"""Retry policy helpers for workflow nodes."""

from __future__ import annotations


def should_retry(exc: Exception, retry_on: tuple[str, ...]) -> bool:
    """Return whether an exception matches the configured retry filter."""

    if not retry_on:
        return True
    error_type = type(exc).__name__
    return error_type in retry_on or f"{type(exc).__module__}.{error_type}" in retry_on


def retry_delay_ms(
    *,
    base_delay_ms: int,
    max_delay_ms: int | None,
    backoff_multiplier: float,
    attempt: int,
) -> int:
    """Calculate the delay before the next retry attempt."""

    delay = int(base_delay_ms * (backoff_multiplier ** max(0, attempt - 1)))
    if max_delay_ms is not None:
        delay = min(delay, int(max_delay_ms))
    return max(0, delay)
