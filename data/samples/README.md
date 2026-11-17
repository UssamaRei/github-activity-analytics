# Sample events

`github_events.jsonl` contains **six fictional, simplified events**, not real GitHub activity.
The IDs and users are deliberately prefixed with `demo-`. Nested payloads are shortened.
The fixture contains 3 push events, 2 issue events, 1 pull request event, 2 repositories,
and 3 actors. It provides a deterministic first-run smoke test.

Real records should be acquired with `python -m ingestion.download`. Never commit
downloaded archives, tokens, or private repository data.
