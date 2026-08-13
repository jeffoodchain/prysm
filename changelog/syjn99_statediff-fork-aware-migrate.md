### Fixed

- Resolve cold-state migration boundaries using finalized canonical block roots, avoiding failures or orphan-state writes when multiple blocks share a slot.
- Track migration progress independently from the cached finalized state so cache misses do not repeat completed work or create inconsistent finalized metadata.
