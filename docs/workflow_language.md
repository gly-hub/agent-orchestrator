# Workflow Language

This project intentionally implements a small workflow language instead of a
general expression engine or full JSON Schema validator.

## Templates

Templates resolve dotted state paths:

```json
{"user_id": "{{context.user_id}}"}
```

A whole-string template returns the resolved value with its original type. An
inline template is converted to text:

```json
{"count": "{{nodes.lookup.output.count}}"}
{"message": "count={{nodes.lookup.output.count}}"}
```

Missing paths fail unless a default is provided:

```json
"{{context.locale | default('en-US')}}"
```

Default values support strings, numbers, booleans, `null`, and simple arrays.

## Conditions

`when` expressions support only these forms:

- empty or absent expression
- `{{path}}` truthiness
- `{{path}} == literal`
- `{{path}} != literal`
- `{{path}} > literal`, `>=`, `<`, `<=`
- `{{path}} in [literal, ...]`
- `{{path}} not in [literal, ...]`
- `expression and expression`
- `expression or expression`
- `(expression)` for grouping precedence

Parentheses can be used to override the default `and`/`or` evaluation order:

```text
({{context.score}} >= 90 or {{context.level}} == 'vip') and {{context.active}} == true
```

Function calls, arithmetic, object literals, and arbitrary Python or JavaScript
expressions are not supported.

Condition routing does not create a nested execution scope. A condition node
writes its selected value to state, and outgoing edges use `when` expressions to
activate only the selected paths. Edges whose `when` expression evaluates false
are skipped and do not block downstream joins.

## DAG Scheduling

The runtime executes workflows as explicit DAGs. Node declaration order is only
a stable listing order; it does not imply execution order. If one node depends
on another, the workflow must include an edge.

Nodes with no incoming edges are entry nodes and may run concurrently. A node
with multiple incoming edges is an implicit join. By default, it waits for all
active incoming paths. Paths skipped by conditions do not block the join.

`human` nodes pause only their own DAG path. Other ready nodes continue running.
When no ready or running nodes remain and one or more human actions are pending,
the run emits `run.waiting`. `run.waiting` includes `pending_action_ids`, and
also includes the legacy `pending_action_id` field when there is exactly one
pending action.

Internal scheduler state (edge activation status, ready/running queues, waiting
actions) is stored under `state["_internal"]` and is not part of the user-facing
state contract. Application code should not read or modify `_internal` entries.

Supported join policies:

- `all_active`: default; wait for all active incoming paths
- `all_success`: wait for all active incoming paths and require successful predecessors
- `any`: run after the first active incoming path completes

## Nested Scopes

`subflow` workflows and `loop` bodies create nested execution scopes. A
`parallel` node remains available as a compact branch-output compatibility
primitive, but general concurrency is provided by DAG scheduling.

`subflow` workflows support `human` nodes. When a child workflow reaches a
human checkpoint, the subflow node pauses the parent run. On resume, the child
workflow continues from where it left off.

`loop` bodies cannot contain `human` nodes. The config validator rejects any
`human` node inside a loop body because resumable checkpoints conflict with
iteration semantics.

## Schemas

Input, output, and human response schemas use a JSON-Schema-like subset:

- `type`
- `enum`
- `required`
- object `properties`
- `additionalProperties`
- array `items`
- string `minLength` and `maxLength`
- number `minimum` and `maximum`

Unsupported JSON Schema keywords are rejected during config validation and by
the runtime schema helper.

## Built-in Stores

The file and SQLite stores are built-in, dependency-free persistence options.
Their public methods are async and run blocking disk/database work in a worker
thread so they can be embedded in async services. They are still intended for
local services, tests, and small deployments; high-throughput production systems
should use the Redis provider or provide a dedicated Postgres/etc. store through
the plugin interfaces.
