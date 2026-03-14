---
name: improve-coverage
description: Use when the user asks to "improve coverage", "increase test coverage", "cover more lines", or "add pragma no cover". Runs coverage, picks uncovered sections, and either marks them or writes new tests.
disable-model-invocation: true
---

# Improve Coverage

## Current Coverage State
- Missing lines: !`uv run pytest tests/ --cov=src --cov-report=term-missing:skip-covered -q 2>&1 | grep -E "^\S.*\s+[0-9]+\s+[0-9]+\s+[0-9]+\s+[0-9]+%\s" | head -30`

## Workflow

1. **Run coverage** to get the current list of missing lines per file:
   ```
   uv run pytest tests/ --cov=src --cov-report=term-missing:skip-covered -q
   ```

2. **Randomly pick 2–4 uncovered sections** from the output — vary which files and line ranges you look at so you don't always improve the same file.

3. **For each selected section**, read the relevant source lines and classify:

   ### Case A — Inside a false/sync branch of `ASYNC_TRUTH_MARKER`
   The codebase has a pattern like:
   ```python
   if ASYNC_TRUTH_MARKER:
       # async path — executed at runtime
   else:
       # sync path — dead code at runtime, never executed
   ```
   The `else` branch and any surrounding sync-only guard blocks (e.g. `if not ASYNC_TRUTH_MARKER:`) are unreachable in the async-patched runtime. Add `# pragma: no cover` to the **`else:` line** (or the `if not ASYNC_TRUTH_MARKER:` line) so coverage.py skips the whole block:
   ```python
   else:  # pragma: no cover
       ...
   ```
   Do **not** add pragma to lines that run in both paths.

   ### Case B — Reachable but untested code
   Write **simple, focused** tests in the most appropriate existing test file:
   - `tests/test_compiler_coverage.py` — for `compiler.py` branches
   - `tests/test_overrides.py` — for queryset/model ORM-level tests
   - `tests/test_coverage.py` — for other src coverage

   Keep tests small — one scenario per test, `@pytest.mark.asyncio` + `@pytest.mark.django_db` for async DB tests. Use `sync_to_async(Model.objects.create)(...)` for setup. Only add tests you're confident will pass.

4. **Verify**: Run the specific test(s) you added to confirm they pass:
   ```
   uv run pytest tests/<file>.py::test_name -v
   ```

5. **Report** what you changed: which lines got `# pragma: no cover`, which tests were added, and the new coverage % for the affected files.

## Constraints
- Keep tests simple — no elaborate fixtures, no mocking
- Only add `# pragma: no cover` to genuinely unreachable sync branches, not to hide real gaps
- Do not modify `ASYNC_TRUTH_MARKER` itself or any async path code
- If a section is complex or risky, skip it and pick a different one
