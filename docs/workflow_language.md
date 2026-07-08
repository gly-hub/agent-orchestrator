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

Condition routing does not create a nested execution scope. `human` nodes may be
used normally before or after condition nodes, and on paths selected by edge
`when` rules.

## Nested Scopes

`parallel` branches, `subflow` workflows, and `loop` bodies create nested
execution scopes.

`parallel` branches that contain `human` nodes automatically fall back to
sequential execution. When a sequential branch hits a human checkpoint, the
parallel node pauses the entire run. On resume, the branch completes and
remaining branches execute in order.

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
