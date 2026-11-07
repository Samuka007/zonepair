# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues on `Samuka007/zonepair`. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --json number,title,body,labels,comments`.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Blocking**: GitHub's native issue dependencies. Add with `gh issue edit <child> --add-blocked-by <blocker>` (`gh` 2.94+); older `gh`: `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, using the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`). Fall back to a `Blocked by: #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Comment**: `gh issue comment <number> --body "..."`
- **Labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: post the explanation with `gh issue comment`, then `gh issue close <number>`.

`gh` infers the repo from `git remote -v` when run inside the clone. File issues only into `Samuka007/zonepair`.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` only if this repo treats external PRs as feature requests.)_

## When a skill says "publish to the issue tracker"

Create a GitHub issue and apply the triage label named in `docs/agents/triage-labels.md`.

## When a skill says "fetch the relevant ticket"

`gh issue view <number> --json number,title,body,labels,comments`.
