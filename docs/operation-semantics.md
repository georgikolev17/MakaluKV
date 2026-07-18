# MakaluKV Operation Semantics

## Get

Returns the value stored under a key.

Edge cases:

- If the key does not exist, return `found = false`.
- If the key exists with an empty value, return `found = true` and an empty value.
- Empty keys are invalid.
- Keys larger than the configured limit are invalid.

## Put

Creates a key or replaces its existing value.

Edge cases:

- If the key already exists, overwrite its value.
- If the key does not exist, create it.
- Empty values are allowed.
- Empty keys are invalid.
- Oversized keys or values are invalid.
- Success is returned only after the value is persisted locally.

## Delete

Removes a key and its value.

Edge cases:

- If the key exists, delete it.
- If the key does not exist, still return success.
- Empty keys are invalid.
- Oversized keys are invalid.
- Success is returned only after the deletion is persisted locally.

## CompareAndSwap

Replaces a value only when the current value matches the expected value.

Possible results:

- `SWAPPED` — the value matched and was replaced.
- `KEY_NOT_FOUND` — the key does not exist.
- `VALUE_MISMATCH` — the key exists, but the value is different.

Edge cases:

- An existing empty value is different from a missing key.
- Empty expected and new values are allowed.
- Empty keys are invalid.
- Oversized keys or values are invalid.
- The comparison and replacement must happen atomically.
- A successful replacement must be persisted before returning.