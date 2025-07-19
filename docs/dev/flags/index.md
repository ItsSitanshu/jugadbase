---
title: Flags
parent: 
nav_order: 1
---

---
title: Flags
---

# `__fl_<x>` Internal Flag System

The `__fl_<x>` system (short for *force logic flags*) is a set of internal, engine-level flags designed to override or bypass certain parts of the standard query execution pipeline in trusted, non-user-facing contexts such as migrations, recovery, or system-level operations.

These flags are never exposed to external users or SQL interfaces. Instead, they are embedded within the `JQLCommand` structure and used selectively by the engine's core logic.

## Purpose

The purpose of `__fl_<x>` flags is to:

* Bypass constraint checks, triggers, or other enforcement mechanisms.
* Perform fast, low-level operations in controlled internal contexts.
* Allow the system to manipulate its own tables safely (e.g., during WAL replay, system bootstrapping, etc.).

## Rules and Guidelines

* `__fl_` flags must not be used in general-purpose query execution.
* These flags are unsafe unless used in specific, audited control paths.
* Any code path that relies on a `__fl_` flag must have clear checks and comments.
* Avoid introducing multiple flags unless their responsibilities are distinct and well-scoped.

## Current Flags

### `__fl_a`: Bypass All Constraints

| Name     | Value Type | Effect                                              |
| -------- | ---------- | --------------------------------------------------- |
| `__fl_a` | `bool`     | Disables all constraint-related logic including:    |
|          |            | - Foreign key `ON UPDATE` / `ON DELETE` enforcement |
|          |            | - Type casting and validation in FK comparisons     |
|          |            | - Collection and propagation of FK tuples           |
|          |            | - Trigger logic (if implemented)                    |

**Intended Use:**

* System table updates
* Bulk inserts during migrations
* WAL or snapshot replay
* Controlled testing and debugging scenarios

**Not for:**

* User-facing query execution
* Regular DML statements

## Example: Bypassing FK Logic using `__fl_a`

In `execute_update()`:

```c
if (!cmd->__fl_a) {
  referencing_fks = get_fk_constr_ref_table(...);
  // enforce FK constraints
}

// Still collect update set regardless of flag
collect_fk_tuples_update(
  db,
  schema,
  cmd,
  cmd->__fl_a ? NULL : referencing_fks,
  ...
);
```

## Future Plans

| Flag     | Purpose (Planned)                    |
| -------- | ------------------------------------ |
| `__fl_b` | Disable WAL (write-ahead logging)    |
| `__fl_c` | Temporarily ignore CHECK constraints |
| `__fl_x` | Execute without index updates        |

## Best Practices

* Wrap any use of `__fl_` flags with comments indicating the reason and risk.
* Document all flags in this file when added.
