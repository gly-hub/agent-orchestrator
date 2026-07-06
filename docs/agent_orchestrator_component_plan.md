# Agent 编排组件开发规划

## 1. 目标

构建一个可配置、可恢复、可观测的 Agent Workflow Engine。它把一次用户对话抽象为一个 `orchestration_run`，在内部按配置执行多个节点：

- Agent 节点：调用 Claude Agent / LLM Agent。
- Tool 节点：调用后端注册工具或用户配置工具。
- Transform 节点：组装、清洗、裁剪上下文。
- Condition 节点：根据状态决定分支。
- Human 节点：暂停等待用户确认，再恢复执行。

前端仍然通过一轮对话体验接收结果。遇到确认、人审、补参数等暂停点时，后端通过两阶段或多阶段 SSE 让多次流式请求归并到同一条助手消息。

## 2. 与现有确认方案的关系

`tool_confirmation_sse_continuity.md` 是编排系统里的一个窄场景：

```text
Tool Confirmation = Human Node + Pending Action + Resume SSE
```

当前文档描述的是通用层：

```text
Workflow Run
  -> Node
  -> Pending Action / Checkpoint
  -> Resume
```

已有工具确认方案可以作为第一批 Human Node 的实现基础。后续把 `ConfirmRecord` 泛化为 `PendingActionRecord`，把 `confirm_id` 泛化为 `pending_action_id` 或 `checkpoint_id`。

## 3. 核心概念

### 3.1 Orchestration Run

一次用户请求对应一个 `orchestration_run`。

它负责记录：

- 原始用户输入。
- 当前 workflow 配置版本。
- 所有节点执行状态。
- 共享状态 `state`。
- SSE 消息归并 ID。
- 当前是否处于等待用户状态。

### 3.2 Node

节点是编排组件的最小执行单元。

```text
agent       调用 Agent SDK / LLM
tool        调用工具注册表里的工具
function    调用后端业务方法
transform   数据映射与组装
condition   条件判断
human       暂停，等待用户输入
parallel    并行执行多个子节点
```

第一版建议先实现：

```text
agent
tool
transform
condition edge
human
```

### 3.3 Shared State

所有节点读写同一个 run state。

```json
{
  "input": {
    "message": "用户原始输入"
  },
  "messages": {
    "user_message_id": "msg_user_001",
    "assistant_message_id": "msg_asst_001",
    "bubble_id": "bubble_001"
  },
  "nodes": {
    "planner": {
      "status": "success",
      "output": {
        "skills": [],
        "tools": []
      }
    },
    "query_profile": {
      "status": "success",
      "output": {
        "profile": {}
      }
    }
  }
}
```

后续节点通过路径表达式引用前序结果：

```text
{{input.message}}
{{nodes.planner.output.tools}}
{{nodes.query_profile.output.profile}}
```

## 4. 推荐模块划分

```text
orchestrator/
  engine.py              # workflow 执行循环
  models.py              # Run / Node / Event / Checkpoint 数据结构
  config.py              # workflow 配置加载与校验
  state.py               # shared state 读写与路径解析
  events.py              # 内部事件与 SSE 事件转换
  registry.py            # agent/tool/function registry
  policy.py              # 权限、确认、风险策略
  checkpoint.py          # 暂停点持久化与恢复
  nodes/
    agent.py
    tool.py
    transform.py
    human.py
    condition.py
```

当前仓库还没有服务端目录。第一阶段可以先在 `orchestrator/` 下做纯 Python 核心库，不和具体 Web 框架绑定；SSE 层通过 async iterator 消费事件。

## 5. Workflow 配置草案

```yaml
id: default_agent_flow
version: 1

nodes:
  - id: planner
    type: agent
    agent: planner
    input:
      message: "{{input.message}}"
    output: plan

  - id: query_profile
    type: tool
    tool: queryUserProfile
    args:
      user_id: "{{context.user_id}}"
      fields: "{{nodes.planner.output.required_user_fields}}"
    output: profile

  - id: confirm_apply
    type: human
    title: "确认执行"
    message: "Agent 准备执行高风险操作，请确认是否继续。"
    options:
      - id: approve
        label: "确认"
      - id: reject
        label: "取消"
    timeout: 10m
    on_timeout: reject

  - id: executor
    type: agent
    agent: executor
    skills: "{{nodes.planner.output.skills}}"
    tools: "{{nodes.planner.output.tools}}"
    input:
      message: "{{input.message}}"
      profile: "{{nodes.query_profile.output.profile}}"

edges:
  - from: planner
    to: query_profile

  - from: query_profile
    to: confirm_apply
    when: "{{nodes.planner.output.requires_confirmation}} == true"

  - from: query_profile
    to: executor
    when: "{{nodes.planner.output.requires_confirmation}} != true"

  - from: confirm_apply
    to: executor
    when: "{{nodes.confirm_apply.output.decision}} == 'approve'"

  - from: confirm_apply
    to: finalizer
    when: "{{nodes.confirm_apply.output.decision}} == 'reject'"
```

## 6. SSE 事件协议

内部事件统一，再映射成 SSE。

```text
run.started
node.started
agent.delta
tool.started
tool.finished
human.required
run.waiting
run.resumed
node.finished
run.finished
run.failed
```

消息连续性继续复用已有方案的三个原则：

- 多阶段共用 `user_message_id`。
- 多阶段共用 `assistant_message_id`。
- 等待用户时不发送最终 `FINISH`，恢复后继续 append。

示例：

```json
{
  "event": "human.required",
  "run_id": "run_001",
  "node_id": "confirm_apply",
  "pending_action_id": "pa_001",
  "up_message_id": "msg_user_001",
  "down_message_id": "msg_asst_001",
  "request": {
    "title": "确认执行",
    "options": [
      {"id": "approve", "label": "确认"},
      {"id": "reject", "label": "取消"}
    ]
  }
}
```

## 7. Human Node 恢复机制

Human Node 不在内存里无限等待。它执行到暂停点时：

1. 保存 checkpoint。
2. 写入 pending action。
3. 发出 `human.required`。
4. 标记 run 为 `waiting_for_user`。
5. 结束当前 SSE。

用户确认后：

1. 前端提交 `pending_action_id` 和 decision。
2. 后端校验 action 状态、TTL、幂等锁。
3. 把 decision 写入对应节点 output。
4. 重新打开 resume SSE。
5. 从下一个节点继续执行。

## 8. 权限与安全

Agent 可以建议工具和参数，但不能直接决定执行权限。

必须经过：

- Tool Registry：工具是否存在，输入输出 schema。
- Policy Gate：当前用户、当前 flow、当前节点是否允许调用。
- Risk Policy：是否需要 Human Node。
- Idempotency Lock：恢复后防止重复执行。
- Canonical Args：确认前后的工具名与参数必须严格一致。

## 9. MVP 开发拆分

### 阶段 1：核心执行器

- 定义 `WorkflowConfig`、`NodeConfig`、`RunState`、`WorkflowEvent`。
- 实现顺序执行 `agent/tool/transform` 节点。
- 实现 shared state 路径读取。
- 实现 async event stream。
- 用 fake agent 和 fake tool 写单元测试。

### 阶段 2：Human Node 与恢复

- 实现 `PendingActionRecord`。
- 实现 checkpoint save/load。
- 实现 `human.required`、`run.waiting`、`run.resumed`。
- 实现 confirm/resume API 所需的核心方法。
- 复用 `tool_confirmation_sse_continuity.md` 的消息 ID 归并原则。

### 阶段 3：条件分支与策略

- 实现 edge `when` 条件。
- 实现工具 allowlist、risk policy、schema validation。
- 支持拒绝后进入替代节点。
- 支持 timeout 默认决策。

### 阶段 4：接入 Claude Agent SDK

- 封装 `ClaudeAgentRunner`。
- 按节点动态配置 system prompt、skills、allowed_tools、mcp_servers。
- 把 SDK 的 text/tool/result 事件转换为统一 `WorkflowEvent`。
- 将 `chat.py` 改造成一个 demo flow。

### 阶段 5：服务端与前端协议

- 提供 `/chat/stream`。
- 提供 `/runs/{run_id}/actions/{pending_action_id}/confirm`。
- 提供 `/runs/{run_id}/resume/stream`。
- 前端按 `down_message_id` 合并多阶段流。
- 增加历史回放：从 `stream_events` 重建 UI。

## 10. 第一版建议接口

```python
class WorkflowEngine:
    async def start(self, request: StartRunRequest) -> AsyncIterator[WorkflowEvent]:
        ...

    async def resume(self, request: ResumeRunRequest) -> AsyncIterator[WorkflowEvent]:
        ...


class ToolRegistry:
    def register(self, tool: ToolDefinition) -> None:
        ...

    async def call(self, name: str, args: dict, ctx: ToolContext) -> dict:
        ...


class CheckpointStore:
    async def save_waiting(self, run_state: RunState, action: PendingActionRecord) -> None:
        ...

    async def approve(self, pending_action_id: str, decision: dict) -> RunState:
        ...
```

当前 MVP 已落地为 `agent_orchestrator/` 独立库目录，并额外提供 SSE 适配器：

```python
from agent_orchestrator import stream_sse, to_message_event

payload = to_message_event(workflow_event)

async for frame in stream_sse(engine.start(request)):
    yield frame
```

`to_message_event` 会把 `WorkflowEvent` 转换为包含 `up_message_id`、`down_message_id`、`bubble_id` 的消息协议事件；`stream_sse` 会进一步编码为标准 SSE frame。

服务层示例已放在 `examples/aiohttp_orchestrator_server.py`，暴露：

```text
POST /chat/stream
POST /runs/{run_id}/actions/{pending_action_id}/confirm
POST /runs/{run_id}/resume/stream
GET  /runs/{run_id}/resume/stream
```

## 11. 主要风险

- 节点配置过于灵活，导致调试困难。第一版应限制节点类型和表达式能力。
- Agent 动态选择工具时越权。所有工具必须经过后端 registry 与 policy。
- SSE 阶段切换导致前端重复消息。必须强制复用 `down_message_id`。
- Human Node 恢复后重复执行副作用工具。必须加 pending action 幂等锁。
- Run state 太大。需要限制节点 output 大小，必要时把大对象写 artifact store。

## 12. 推荐落地顺序

先做一个纯后端 demo：

```text
planner(fake agent)
  -> query_profile(fake tool)
  -> confirm_apply(human node)
  -> executor(fake agent)
  -> finalizer(fake agent)
```

跑通后再接 Claude Agent SDK。这样能先验证编排、暂停、恢复、SSE 连续性，不会被模型流式事件和工具权限细节拖住。

## 13. 当前实现进展

已实现：

- 独立 `src/agent_orchestrator` 包。
- `WorkflowEngine.start(...)` / `WorkflowEngine.resume(...)`。
- `agent`、`tool`、`transform`、`human` 节点。
- `InMemoryCheckpointStore`。
- `FileCheckpointStore`，支持 JSON 持久化、TTL 过期判断、重复 resume 锁。
- workflow 配置校验，支持重复节点、未知 edge、缺字段、无效节点类型、环路检查。
- tool policy / permission gate，支持工具权限、风险等级、确认策略、缺权限拒绝。
- Claude Agent SDK runner，可将 SDK 的 TextBlock / ToolUseBlock / ToolResultBlock 转成统一 WorkflowEvent。
- `to_message_event` / `stream_sse` SSE 适配。
- aiohttp 示例服务。

后续仍建议补：

- Redis/DB checkpoint store。
- message event store 与历史回放。
