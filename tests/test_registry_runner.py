import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import AgentRegistry, RunState, ToolRegistry, WorkflowEvent
from agent_orchestrator.exceptions import RegistryError
from agent_orchestrator.runners import ClaudeAgentRunner, ClaudeAgentRunnerConfig


async def planner_agent(agent_input, run_state):
    yield WorkflowEvent(
        type="agent.output",
        run_id=run_state.run_id,
        node_id="planner",
        data={},
    )


async def query_profile(args, run_state):
    return {"profile": {"user_id": args["user_id"], "level": "vip"}}


class FakeTextBlock:
    def __init__(self, text):
        self.text = text


class FakeToolUseBlock:
    def __init__(self, name, input, id="toolu_1"):
        self.name = name
        self.input = input
        self.id = id


class FakeToolResultBlock:
    def __init__(self, tool_use_id, content, is_error=False):
        self.tool_use_id = tool_use_id
        self.content = content
        self.is_error = is_error


class FakeAssistantMessage:
    def __init__(self, content):
        self.content = content


class FakeResultMessage:
    def __init__(self, result=None):
        self.result = result


class FakeStreamEvent:
    def __init__(self, event):
        self.event = event


class FakeClaudeAgentOptions:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class FakeClaudeSDKClient:
    last_client = None

    def __init__(self, options):
        self.options = options
        self.connected = False
        self.disconnected = False
        self.queried_prompt = None
        FakeClaudeSDKClient.last_client = self

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnected = True

    async def query(self, prompt):
        self.queried_prompt = prompt

    async def receive_response(self):
        yield FakeAssistantMessage(
            [
                FakeTextBlock("hello"),
                FakeToolUseBlock("lookup", {"q": "x"}),
                FakeToolResultBlock("toolu_1", "ok"),
                FakeTextBlock(" world"),
            ]
        )
        yield FakeResultMessage()


FAKE_CLAUDE_SDK = SimpleNamespace(
    ClaudeAgentOptions=FakeClaudeAgentOptions,
    ClaudeSDKClient=FakeClaudeSDKClient,
    AssistantMessage=FakeAssistantMessage,
    ResultMessage=FakeResultMessage,
    StreamEvent=FakeStreamEvent,
    TextBlock=FakeTextBlock,
    ToolUseBlock=FakeToolUseBlock,
    ToolResultBlock=FakeToolResultBlock,
)


class RegistryRunnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_agent_registry_rejects_invalid_registration(self):
        agents = AgentRegistry()

        with self.assertRaisesRegex(RegistryError, "agent name is required"):
            agents.register("", planner_agent)
        with self.assertRaisesRegex(RegistryError, "agent handler must be callable"):
            agents.register("planner", None)

    async def test_tool_registry_rejects_invalid_registration_metadata(self):
        tools = ToolRegistry()

        with self.assertRaisesRegex(RegistryError, "tool name is required"):
            tools.register("", query_profile)
        with self.assertRaisesRegex(RegistryError, "tool handler must be callable"):
            tools.register("query_profile", None)
        with self.assertRaisesRegex(RegistryError, "tool permissions must be a string list"):
            tools.register("query_profile", query_profile, permissions=["read", 123])
        with self.assertRaisesRegex(RegistryError, "tool risk_level must be one of"):
            tools.register("query_profile", query_profile, risk_level="critical")
        with self.assertRaisesRegex(RegistryError, "tool confirmation_policy must be one of"):
            tools.register("query_profile", query_profile, confirmation_policy="sometimes")

    async def test_claude_agent_runner_maps_sdk_messages_to_workflow_events(self):
        runner = ClaudeAgentRunner(
            ClaudeAgentRunnerConfig(
                options={"model": "fake-model"},
                prompt_template="Prompt: {message}",
            ),
            sdk_module=FAKE_CLAUDE_SDK,
        )
        run_state = RunState(
            run_id="run_test",
            workflow_id="wf",
            workflow_version=1,
            status="running",
            state={},
            current_node_id="claude",
        )

        events = [event async for event in runner({"message": "hi"}, run_state)]

        self.assertEqual(
            [event.type for event in events],
            [
                "agent.delta",
                "agent.tool_use",
                "agent.tool_result",
                "agent.delta",
                "agent.output",
            ],
        )
        self.assertEqual(events[0].data["text"], "hello")
        self.assertEqual(events[1].data["tool_name"], "lookup")
        self.assertEqual(events[2].data["content"], "ok")
        self.assertEqual(events[-1].data["text"], "hello world")
        self.assertEqual(FakeClaudeSDKClient.last_client.queried_prompt, "Prompt: hi")
        self.assertTrue(FakeClaudeSDKClient.last_client.disconnected)

    async def test_claude_agent_runner_streams_partial_message_deltas(self):
        class PartialClaudeSDKClient(FakeClaudeSDKClient):
            async def receive_response(self):
                yield FakeStreamEvent(
                    {
                        "type": "content_block_delta",
                        "delta": {"type": "text_delta", "text": "hel"},
                    }
                )
                yield FakeStreamEvent(
                    {
                        "type": "content_block_delta",
                        "delta": {"type": "text_delta", "text": "lo"},
                    }
                )
                yield FakeAssistantMessage([FakeTextBlock("hello")])
                yield FakeResultMessage()

        fake_sdk = SimpleNamespace(
            **{
                **FAKE_CLAUDE_SDK.__dict__,
                "ClaudeSDKClient": PartialClaudeSDKClient,
            }
        )
        runner = ClaudeAgentRunner(
            ClaudeAgentRunnerConfig(options={"include_partial_messages": True}),
            sdk_module=fake_sdk,
        )
        run_state = RunState(
            run_id="run_partial",
            workflow_id="wf",
            workflow_version=1,
            status="running",
            state={},
            current_node_id="claude",
        )

        events = [event async for event in runner({"message": "hi"}, run_state)]

        self.assertEqual(
            [event.type for event in events],
            ["agent.delta", "agent.delta", "agent.output"],
        )
        self.assertEqual(
            [event.data["text"] for event in events if event.type == "agent.delta"],
            ["hel", "lo"],
        )
        self.assertEqual(events[-1].data["text"], "hello")
