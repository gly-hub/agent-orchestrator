import sys
import unittest
from pathlib import Path

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from agent_orchestrator import (
    AgentRegistry,
    InMemoryArtifactStore,
    PersistencePluginRegistry,
    StartRunRequest,
    ToolRegistry,
    WorkflowConfig,
    WorkflowEngine,
    WorkflowEvent,
    create_artifact_store,
    resolve_artifacts,
    validate_schema_value,
)
from agent_orchestrator.exceptions import WorkflowConfigError, WorkflowError
from agent_orchestrator.state import evaluate_when


class ArtifactsSchemaTest(unittest.IsolatedAsyncioTestCase):
    async def test_condition_expression_supports_comparison_membership_and_boolean_logic(self):
        state = {
            "context": {
                "score": 91,
                "level": "vip",
                "tags": ["deploy", "prod"],
                "phrase": "rock and roll",
            }
        }

        self.assertTrue(evaluate_when("{{context.score}} >= 90", state))
        self.assertTrue(evaluate_when("{{context.score}} < 100 and {{context.level}} in ['vip', 'svip']", state))
        self.assertTrue(evaluate_when("'prod' in {{context.tags}}", state))
        self.assertTrue(evaluate_when("{{context.level}} not in ['normal', 'guest']", state))
        self.assertTrue(evaluate_when("{{context.phrase}} == 'rock and roll' or {{context.score}} < 10", state))
        self.assertFalse(evaluate_when("{{context.score}} <= 80 or {{context.level}} == 'guest'", state))

    async def test_human_response_schema_validates_resume_payload(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        workflow = WorkflowConfig.from_dict(
            {
                "id": "human-schema",
                "version": 1,
                "nodes": [
                    {
                        "id": "collect",
                        "type": "human",
                        "title": "补充参数",
                        "fields": [
                            {"id": "env", "type": "select", "options": ["staging", "prod"]},
                            {"id": "version", "type": "text"},
                        ],
                        "response_schema": {
                            "required": ["decision", "env", "version"],
                            "properties": {
                                "decision": {"type": "string", "enum": ["submit", "cancel"]},
                                "env": {"type": "string", "enum": ["staging", "prod"]},
                                "version": {"type": "string"},
                            },
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        first_events = [event async for event in engine.start(StartRunRequest(message="deploy"))]
        required = [event for event in first_events if event.type == "human.required"][0]
        pending_action_id = required.data["pending_action_id"]
        self.assertEqual(required.data["request"]["fields"][0]["id"], "env")

        with self.assertRaisesRegex(WorkflowError, "missing required field: version"):
            [
                event
                async for event in engine.resume(
                    pending_action_id=pending_action_id,
                    decision={"decision": "submit", "env": "staging"},
                )
            ]

    async def test_tool_input_schema_rejects_invalid_args(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register(
            "deploy",
            deploy_tool,
            input_schema={
                "type": "object",
                "required": ["env"],
                "properties": {"env": {"type": "string", "enum": ["staging", "prod"]}},
            },
        )
        workflow = WorkflowConfig.from_dict(
            {
                "id": "tool-input-schema",
                "version": 1,
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "args": {"env": "dev"},
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="deploy"))]

        self.assertEqual(calls, [])
        self.assertEqual(events[-1].type, "run.failed")
        self.assertIn("tool deploy input.env", events[-1].data["error"])

    async def test_tool_output_schema_rejects_invalid_output(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def bad_tool(args, run_state):
            return {"ok": "yes"}

        tools.register(
            "bad",
            bad_tool,
            output_schema={
                "type": "object",
                "required": ["ok"],
                "properties": {"ok": {"type": "boolean"}},
            },
        )
        workflow = WorkflowConfig.from_dict(
            {
                "id": "tool-output-schema",
                "version": 1,
                "nodes": [{"id": "bad", "type": "tool", "tool": "bad"}],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="run"))]

        self.assertEqual(events[-1].type, "run.failed")
        self.assertIn("tool bad output.ok must be boolean", events[-1].data["error"])

    async def test_tool_input_schema_rejects_string_length_and_number_range(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        calls = []

        async def deploy_tool(args, run_state):
            calls.append(args)
            return {"ok": True}

        tools.register(
            "deploy",
            deploy_tool,
            input_schema={
                "type": "object",
                "required": ["env", "version", "replicas"],
                "properties": {
                    "env": {"type": "string", "minLength": 3, "maxLength": 8},
                    "version": {"type": "string", "minLength": 5},
                    "replicas": {"type": "integer", "minimum": 1, "maximum": 5},
                },
                "additionalProperties": False,
            },
        )
        workflow = WorkflowConfig.from_dict(
            {
                "id": "schema-ranges",
                "version": 1,
                "nodes": [
                    {
                        "id": "deploy",
                        "type": "tool",
                        "tool": "deploy",
                        "args": {
                            "env": "prod",
                            "version": "v1",
                            "replicas": 10,
                            "extra": True,
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="deploy"))]

        self.assertEqual(calls, [])
        self.assertEqual(events[-1].type, "run.failed")
        self.assertIn("tool deploy input.version length must be >= 5", events[-1].data["error"])

    async def test_schema_additional_properties_false_rejects_extra_fields(self):
        with self.assertRaisesRegex(WorkflowError, "payload has unsupported field: extra"):
            validate_schema_value(
                {"name": "deploy", "extra": True},
                {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "additionalProperties": False,
                },
                label="payload",
            )

    async def test_schema_additional_properties_schema_validates_extra_fields(self):
        validate_schema_value(
            {"known": "ok", "dynamic": 2},
            {
                "type": "object",
                "properties": {"known": {"type": "string"}},
                "additionalProperties": {"type": "integer", "minimum": 1},
            },
            label="payload",
        )
        with self.assertRaisesRegex(WorkflowError, "payload.dynamic must be >= 1"):
            validate_schema_value(
                {"known": "ok", "dynamic": 0},
                {
                    "type": "object",
                    "properties": {"known": {"type": "string"}},
                    "additionalProperties": {"type": "integer", "minimum": 1},
                },
                label="payload",
            )

    async def test_schema_rejects_unknown_keywords_at_runtime(self):
        with self.assertRaisesRegex(WorkflowError, "unsupported schema keyword: oneOf"):
            validate_schema_value(
                {"name": "deploy"},
                {
                    "type": "object",
                    "oneOf": [{"required": ["name"]}],
                },
                label="payload",
            )

    async def test_workflow_config_rejects_invalid_schema_constraints(self):
        with self.assertRaisesRegex(WorkflowConfigError, "minLength must be <= maxLength"):
            WorkflowConfig.from_dict(
                {
                    "id": "bad-schema",
                    "version": 1,
                    "nodes": [
                        {
                            "id": "bad",
                            "type": "tool",
                            "tool": "bad",
                            "input_schema": {
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "minLength": 5,
                                        "maxLength": 2,
                                    }
                                }
                            },
                        }
                    ],
                }
            )

    async def test_workflow_config_rejects_unknown_schema_keywords(self):
        with self.assertRaisesRegex(WorkflowConfigError, "unsupported schema keyword: pattern"):
            WorkflowConfig.from_dict(
                {
                    "id": "bad-schema-keyword",
                    "version": 1,
                    "nodes": [
                        {
                            "id": "bad",
                            "type": "tool",
                            "tool": "bad",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string", "pattern": "^[a-z]+$"},
                                },
                            },
                        }
                    ],
                }
            )

    async def test_agent_output_schema_rejects_invalid_output(self):
        agents = AgentRegistry()
        tools = ToolRegistry()

        async def bad_agent(agent_input, run_state):
            yield WorkflowEvent(
                type="agent.output",
                run_id=run_state.run_id,
                node_id="planner",
                data={"requires_confirmation": "yes"},
            )

        agents.register("planner", bad_agent)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "agent-output-schema",
                "version": 1,
                "nodes": [
                    {
                        "id": "planner",
                        "type": "agent",
                        "agent": "planner",
                        "output_schema": {
                            "type": "object",
                            "required": ["requires_confirmation"],
                            "properties": {"requires_confirmation": {"type": "boolean"}},
                        },
                    }
                ],
            }
        )
        engine = WorkflowEngine(workflow, agents=agents, tools=tools)

        events = [event async for event in engine.start(StartRunRequest(message="plan"))]

        self.assertEqual(events[-1].type, "run.failed")
        self.assertIn("agent planner output.requires_confirmation must be boolean", events[-1].data["error"])

    async def test_node_output_can_be_stored_as_artifact(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        artifacts = InMemoryArtifactStore()

        async def large_tool(args, run_state):
            return {"text": "x" * 100}

        tools.register("large", large_tool)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "artifact-output",
                "version": 1,
                "nodes": [
                    {
                        "id": "large",
                        "type": "tool",
                        "tool": "large",
                        "output_artifact": True,
                    }
                ],
            }
        )
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            artifact_store=artifacts,
        )

        events = [event async for event in engine.start(StartRunRequest(message="run"))]

        finished = [event for event in events if event.type == "node.finished"][0]
        ref = finished.data["output"]["artifact_ref"]
        value = await artifacts.get(ref)
        self.assertEqual(ref["node_id"], "large")
        self.assertEqual(value, {"text": "x" * 100})

    async def test_resolve_artifacts_replaces_nested_artifact_refs(self):
        artifacts = InMemoryArtifactStore()
        ref = await artifacts.put(
            run_id="run_art",
            node_id="producer",
            name="output",
            value={"text": "hello"},
        )

        resolved = await resolve_artifacts(
            {"items": [{"artifact_ref": ref}], "keep": "plain"},
            artifacts,
        )

        self.assertEqual(resolved, {"items": [{"text": "hello"}], "keep": "plain"})

    async def test_node_input_can_resolve_artifact_refs(self):
        agents = AgentRegistry()
        tools = ToolRegistry()
        artifacts = InMemoryArtifactStore()
        seen = []

        async def producer(args, run_state):
            return {"payload": {"text": "large result"}}

        async def consumer(args, run_state):
            seen.append(args)
            return {"received": args["document"]["payload"]["text"]}

        tools.register("producer", producer)
        tools.register("consumer", consumer)
        workflow = WorkflowConfig.from_dict(
            {
                "id": "artifact-input",
                "version": 1,
                "nodes": [
                    {
                        "id": "producer",
                        "type": "tool",
                        "tool": "producer",
                        "output_artifact": True,
                    },
                    {
                        "id": "consumer",
                        "type": "tool",
                        "tool": "consumer",
                        "resolve_input_artifacts": True,
                        "args": {
                            "document": "{{nodes.producer.output}}",
                        },
                    },
                ],
            }
        )
        engine = WorkflowEngine(
            workflow,
            agents=agents,
            tools=tools,
            artifact_store=artifacts,
        )

        events = [event async for event in engine.start(StartRunRequest(message="run"))]

        consumer_finished = [
            event for event in events if event.node_id == "consumer" and event.type == "node.finished"
        ][0]
        self.assertEqual(seen, [{"document": {"payload": {"text": "large result"}}}])
        self.assertEqual(consumer_finished.data["output"], {"received": "large result"})

    async def test_artifact_store_provider_can_be_registered(self):
        registry = PersistencePluginRegistry()
        registry.artifacts.register("custom-artifacts", lambda config: InMemoryArtifactStore())

        store = create_artifact_store("custom-artifacts", registry=registry)

        self.assertIsInstance(store, InMemoryArtifactStore)
