from agent_orchestrator.retry import retry_delay_ms, should_retry


async def test_retry_helpers_match_error_type_and_calculate_backoff():
    error = RuntimeError("temporary")

    assert should_retry(error, ())
    assert should_retry(error, ("RuntimeError",))
    assert should_retry(error, ("builtins.RuntimeError",))
    assert not should_retry(error, ("ValueError",))
    assert retry_delay_ms(
        base_delay_ms=100,
        max_delay_ms=250,
        backoff_multiplier=2,
        attempt=3,
    ) == 250
