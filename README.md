An adapted version of the scan-query contrib extension, with some changes:

- Standalone packaging.
- Adds virtualColumns.
- Always uses ObjectColumnSelectors rather than a mix of various selector types.
- Returns `__time` as `__time` rather than `timestamp`.
- Query type changed to `scan_` so it can potentially coexist with the community extension.
