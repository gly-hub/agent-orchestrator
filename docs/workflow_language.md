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

Parentheses, function calls, arithmetic, object literals, and arbitrary Python
or JavaScript expressions are not supported.

Condition routing does not create a nested execution scope. `human` nodes may be
used normally before or after condition nodes, and on paths selected by edge
`when` rules.

## Nested Scopes

`parallel` branches and `subflow` workflows create nested execution scopes.
They intentionally cannot contain `human` nodes. Human checkpoints are supported
in the parent workflow, including ordinary condition/edge branches, but not
inside these nested scopes.

The config validator rejects:

- direct `human` nodes inside `parallel.branches`
- `human` nodes inside workflow-style parallel branches
- `human` nodes inside `subflow.workflow`

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
