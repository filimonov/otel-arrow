# Series-parquet PR Series and Handover Issue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the series-parquet work into a set of PR branches that mirror the future upstream PRs. Every change that can start from origin/main starts there and merges on its own; the others depend only on the PRs they truly need. Add a handover issue that explains every branch, and open all of it in Altinity/otel-arrow for internal review.

**Architecture:** The content is fixed. It is the fork's core branch `series-parquet-upstream` (12 commits) plus the eight `sp/*` branches. This plan only regroups that content into PR units, bases each unit on origin/main or on its real dependencies, and proves that the regrouped branches merge back to the same tree. A PR that needs several others gets a generated `deps/*` integration branch as its base, so its diff shows only its own code.

**Tech Stack:** git (cherry-pick, merge, rebase), cargo workspace `rust/otap-dataflow`, `make chlog-validate`, the Python E2E suite, gh CLI.

**Spec:** the user's requirements of 2026-09-28, quoted in Global Constraints. Background:
- `docs/superpowers/reports/series-parquet-measurement/REPORT.md` (campaign report);
- `docs/superpowers/plans/2026-09-23-plan-4-backlog.md` (backlog);
- scratchpad `upstream-review/SPLIT-REPORT.md` (current branches) and `REVIEW4-FIX-BRIEF.md` (scope rule).

## Global Constraints

- The user's requirements (2026-09-28):
  - "Every PR contains a fully correct description that meets the upstream requirements, and passes the build and tests."
  - "Everything that can branch from upstream/main must branch from it."
  - "An issue describes all branches, why they were added, the problems they solve, and why they are needed."
  - "Every PR that branches from main contains tests and everything it needs to be merged on its own."
  - "A PR that depends on others depends only on those where there would otherwise be hard conflicts or obvious code defects (not necessarily on every PR in main)."
- Upstream base: origin/main `137215724`. Altinity/otel-arrow `main` is 3 commits behind it and is a fast-forward ancestor.
- The content source of truth is the fork refs at plan start, recorded in Task 0: the core `1ab1402ea` and the sp/* tips `4d61d0441` (otlp-framing), `64675dcce` (pdata-id-overflow), `5ebe4b63c` (graceful-shutdown-acks), `21b4e6227` (s3-unsigned-azure), `47cd1cd87` (jemalloc-background-thread), `fa2baf4cb` (receiver-refusal-extras), `c3bcc91e4` (durable-buffer-small) and `ea4d4ebca` (misc).
- No behaviour changes. Edits are allowed only where a unit must stand alone: a test helper, an import, a README sentence or link that names a later unit, a changelog entry, or a feature flag.
- PR descriptions follow `.github/pull_request_template.md`: Change summary, Related issue, Validation, User-facing changes. Chore PRs follow `.github/PULL_REQUEST_TEMPLATE/chore.md` and carry `chore` in the title.
- Every user-facing Rust change carries a `rust/otap-dataflow/.chloggen/*.yaml` entry:
  - the component is listed in `config.yaml`;
  - the note is at most 200 characters and the subtext at most 300;
  - a `breaking` entry has a `Migration:` subtext;
  - `issues:` is `[0]` until real numbers exist (Task 10 fills them).
- Commit trailers carry only `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Commit messages stay as they are now unless a unit's content changes.
- Branch names in Altinity/otel-arrow:
  - `series-parquet/pr-NN-<slug>` for PR heads;
  - `series-parquet/deps-<slug>` for generated integration bases.
  The same names are pushed to the fork `filimonov` first.
- cargo runs only under the host lease: `flock -w 3600 /tmp/series-parquet-host-measurement.lock`, or the wrapper `scratchpad/u1/cargo.sh`. Test binaries run under `timeout -s KILL 900`.
- Nothing is published to Altinity/otel-arrow without the user's explicit go (Task 10). No upstream (open-telemetry) PRs.

## Review Focus

- **A PR whose tests reference a helper added by another unit.** It compiles only in the stack. Task 2 builds and tests every unit on its own base.
- **A README, config comment or workflow path in a unit that names a file or feature from a later unit.** Examples: the exporter README linking `configs/series-parquet-*.yaml`, or E2E path filters naming `deploy/`. The per-unit lint in Task 2 includes lychee or markdownlint link checks and a grep for later-unit paths.
- **A changelog entry that lands in the wrong unit, or a unit with user-facing changes and no entry.** Task 7 checks every unit with `make chlog-validate` on its own branch.
- **A PR whose diff on GitHub also shows its dependencies' commits** because its base is wrong. Task 9 compares each PR's `git log base..head` with its unit list.
- **Merge drift.** All PR branches merged together must equal the reference tree (core plus every sp/* merged, as today). Task 8 checks the tree hash and runs the full E2E on that merge.

---

## Unit catalogue (input to Task 1)

Current commits, grouped into candidate PR units. The base column is the hypothesis Task 1 must confirm or correct.

| Unit | Commits (current fork refs) | Hypothesised base | Why |
|---|---|---|---|
| U01 object store wiring | 0126f656b | main | refactor in otap |
| U02 retryable receiver statuses | d95727c3b, 6fb95528d, aad3ba6a3 | main | receivers only |
| U03 byte-size deserializers | 4bce2c1ef | main | config helper |
| U04 durable_buffer oldest_pending.age | 24163c215 | main | quiver + buffer |
| U05 series-lake crate | 9dec6284f | main | new crate; check whether it needs U03 |
| U06 otap completion route | 5946d85fb | main | otap Context |
| U07 series_parquet exporter | c134a8f9d | deps: U01, U03?, U05, U06 | the exporter uses them |
| U08 reference configs + Alloy | 7f7d594d8 | U07 | configs name the exporter |
| U09 E2E suite + workflow | 3f6a68d0d | U08 | runs the configs |
| U10 deployment examples | 1ab1402ea | U09 (+U04 for alerts) | last; Altinity review only |
| U11 engine completion phase | 43c92c5dc | main | engine |
| U12 durable_buffer ack recording at shutdown | b3c71e1a0 | U11 | uses the completion phase |
| U13 engine completion-slot reservation | 6079bd7ad | main or U06 | check |
| U14 series_parquet write at shutdown | 5ebe4b63c | deps: U07, U11, U12?, U13 | adapter |
| U15 pdata framing check + prost views | f9097ce40, ee55a5ed1 | main | pdata |
| U16 exporters (file/otap/parquet) refuse broken framing | f94e19c22 | U15 | uses the check |
| U17 series_parquet refuses broken framing | 4d61d0441 | deps: U07, U15 | adapter |
| U18 pdata id overflow (u16 + delta) | d67df116d, 381768422 | main | pdata |
| U19 permanent refusal of conversion failures | d36b7cdd7 | U18 (try to remove the U16 dependency) | otap/parquet exporters + buffer convert branch |
| U20 series_parquet 65,536-record doc | 64675dcce | deps: U07, U18 | doc adapter; may fold into U19 or U08 |
| U21 S3 unsigned_payload | cf3208b91 | U01 | object store |
| U22 Azure endpoint | d3d024842 | U01 | object store |
| U23 series_parquet reports signing | 21b4e6227 | deps: U07, U21, U22 | adapter |
| U24 jemalloc background thread | cc49d917d | main | engine/df_engine |
| U25 deploy relies on the engine's jemalloc | 47cd1cd87 | deps: U10, U24 | adapter |
| U26 receiver concurrency shed | a6fd83777 | U02 | uses RetryInfo |
| U27 oversized INVALID_ARGUMENT, over-burst retryable | 2df326a84 | U02 (U26?) | receiver |
| U28 lowered-limit warning | 8b078760d | main or U26 | receiver |
| U29 series_parquet follows INVALID_ARGUMENT | fa2baf4cb | deps: U07, U27 | adapter |
| U30 durable_buffer conversion_failed | 6b8eaf48a | main | buffer |
| U31 durable_buffer docs | 8d3050f63 | main | docs (chore) |
| U32 durable_buffer log rate limits | 5bb7247cb | main | buffer |
| U33 alert on unreadable bundles | c3bcc91e4 | deps: U10, U30 | adapter |
| U34 pdata UTF-8 repair count | dd40aa280 | main | pdata |
| U35 series_parquet counts UTF-8 repairs | 12613cf46 | deps: U07, U34 | adapter |
| U36 node-panic characterization | ea4d4ebca | main | engine test + docs (chore) |

Rules for deciding a unit's base (Task 1):
1. **Base = origin/main** if the unit's commits cherry-pick cleanly onto origin/main and its crates build and pass tests there.
2. **Base = one other unit** if (1) fails only because of that unit: an API it uses, a conflict that would take real rewriting, or a test that needs its code.
3. **Base = `deps-<slug>`** (origin/main plus a merge of each dependency) if several units are needed.
4. **Remove false dependencies.** A dependency that exists only because the commit was written on top of another, such as context lines or a README sentence, is removed by a minimal edit, not accepted. Record each edit.
5. **Split or merge units.** A unit that mixes an upstream change with a series_parquet adaptation is split. Two units that cannot be reviewed apart are merged; record why.

---

### Task 0: Setup and reference tree

**Files:**
- Create: `scratchpad/handover/LEDGER.md`
- Create: `scratchpad/handover/refs-at-start.txt`

- [ ] **Step 1: Record the starting refs**

```bash
cd /home/mfilimonov/workspace/otel-arrow
git fetch filimonov
git ls-remote filimonov 'refs/heads/series-parquet-upstream' 'refs/heads/sp/*' > $S/handover/refs-at-start.txt
```

Expected: the tips listed in Global Constraints. If any differ, stop and report.

- [ ] **Step 2: Create a worktree from the core tip**

```bash
git worktree add $S/handover/wt filimonov/series-parquet-upstream
```

- [ ] **Step 3: Build the reference tree R (everything together)**

```bash
cd $S/handover/wt
git checkout -b ref-all filimonov/series-parquet-upstream
for b in otlp-framing pdata-id-overflow graceful-shutdown-acks s3-unsigned-azure jemalloc-background-thread receiver-refusal-extras durable-buffer-small misc; do
  git merge --no-edit filimonov/sp/$b || { echo "conflict in $b"; break; }
done
git rev-parse HEAD^{tree} > $S/handover/ref-tree.txt
```

Resolve any conflict by keeping both sides, as the sp/* rebases did. Record every resolution in LEDGER.md.

- [ ] **Step 4: Gate R**

Run under the lease:
- `cargo check --workspace --all-targets`;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`;
- the full Python E2E.

Expected: all green. R is the target tree for Task 8.

### Task 1: Dependency map

**Files:**
- Create: `scratchpad/handover/DEPENDENCY-MAP.md`

- [ ] **Step 1: Probe each unit alone on origin/main**

For each unit in the catalogue, on a throwaway branch from `137215724`:

```bash
git checkout -B probe 137215724
git cherry-pick <commits of the unit> && cargo check --workspace --all-targets
```

Then run the tests of the crates the unit touches. Record one of:
- clean;
- conflict: list the files;
- build error: list the missing items and the unit that defines them;
- test failure: name the tests.

- [ ] **Step 2: Decide each base with the rules above**

Write DEPENDENCY-MAP.md with one row per unit: unit, final base, depends on, and a one-line reason with evidence (conflict, missing API, failing test). List every false dependency to remove and its planned minimal edit. List every split or merge of units and why.

- [ ] **Step 3: Check the map for cycles and for over-deep stacks**

Keep stacks at most 3 deep, except the series_parquet chain U07 → U08 → U09 → U10. Otherwise record why.

- [ ] **Step 4: Stop for review**

Send the map to the controller before building. The dependency choices are the part of this plan most worth a second opinion.

### Task 2: Build the independent branches (base origin/main)

**Files:**
- Create: branches `series-parquet/pr-NN-<slug>`, one per unit whose base is main

- [ ] **Step 1: Create each branch**

```bash
git checkout -B series-parquet/pr-NN-<slug> 137215724
git cherry-pick <unit commits>
```

- [ ] **Step 2: Make it stand alone**

Apply only the edits recorded in the map (Global Constraints, "No behaviour changes"). If a unit's tests relied on another unit's helper, move a minimal copy of the helper into the unit and record it. Commit with `--fixup` onto the unit's own commit, then `git rebase -i --autosquash 137215724` with the sequence editor set to `:`.

- [ ] **Step 3: Gate the branch**

Under the lease:
- `cargo check --workspace --all-targets` with 0 warnings;
- tests of every crate the unit touches;
- clippy `-D warnings` on those crates with their features;
- `cargo fmt --check`;
- markdownlint on changed md;
- `make chlog-validate CHLOGGEN=~/go/bin/chloggen`;
- `python3 tools/sanitycheck.py`;
- a grep that no changed file names a path or feature of a later unit.

Expected: all green. Record the results in LEDGER.md.

### Task 3: Build the single-dependency branches

- [ ] **Step 1: Create each branch on its dependency's branch**

```bash
git checkout -B series-parquet/pr-NN-<slug> series-parquet/pr-MM-<dep>
git cherry-pick <unit commits>
```

- [ ] **Step 2: Stand alone against the dependency, then gate**

Stand-alone edits and gates as in Task 2, with the dependency in place of main.

### Task 4: Build the integration bases and the series_parquet chain

- [ ] **Step 1: Build `series-parquet/deps-exporter`**

Start from `137215724` and merge, with `--no-ff`, each unit the exporter needs according to the map. Gate: `cargo check --workspace --all-targets`.

- [ ] **Step 2: Build the exporter chain**

Build U07 on `deps-exporter`, U08 on U07, U09 on U08. U10 goes on U09, merged with U04 if the map says the alerts need it (through a `deps-deploy` base if needed).

- [ ] **Step 3: Gate the chain**

Gate each branch as in Task 2. On U09 and U10 also run the full Python E2E and `scripts/validate-configs.sh`. On U10 also run the deploy checks: `check.py`, `test_check.py`, promtool and kubeconform.

### Task 5: Build the adapter branches

- [ ] **Step 1: Build a `deps-<slug>` base and the adapter branch for each adapter**

Adapters are U14, U17, U20, U23, U25, U29, U33 and U35. The base merges the series_parquet branch the adapter needs with its feature unit. Then:

```bash
git checkout -B series-parquet/pr-NN-<slug> series-parquet/deps-<slug>
git cherry-pick <unit commits>
```

- [ ] **Step 2: Gate each adapter**

Gates as in Task 2. On U14 also run the full E2E, because it changes shutdown behaviour.

### Task 6: PR descriptions

**Files:**
- Create: `scratchpad/handover/prs/pr-NN-<slug>.md`, one per branch

- [ ] **Step 1: Write each description from the template**

Sections:
- **Change summary:** 2–5 sentences, taken from the unit's commit messages.
- **Related issue:** `Part of #<handover issue>` (placeholder `#TRACKING` until Task 10).
- **Depends on:** the PR list, or "none — independent of the rest of the series".
- **Validation:** the exact commands and results from the gate run in LEDGER.md.
- **User-facing changes:** each chloggen entry, or "None" together with a chore title.
- **Notes for reviewers:** what to look at, known limits, and backlog items that stay open.

Use the chore template for units that are chore commits (U09, U31, U36 and chore adapters).

- [ ] **Step 2: Check each description against its branch**

Every file, config key, metric or behaviour the description names must exist on that branch (grep it). No numbers without their source (LEDGER.md or REPORT.md).

### Task 7: Changelog audit per branch

- [ ] **Step 1: Run the changelog checks on every PR branch**

Run `make chlog-validate` and list `.chloggen/*.yaml` added by `base..head`. Every user-facing unit has exactly the entries describing its own change. No entry describes a change from another unit. Breaking entries have `Migration:`.

- [ ] **Step 2: Fix and re-gate**

Fix misplaced entries with fixups and re-run Task 2's gate for the branch.

### Task 8: Equivalence and full-system check

- [ ] **Step 1: Merge every PR branch in dependency order**

On a scratch branch from `137215724`, merge every `series-parquet/pr-*` branch in dependency order.

- [ ] **Step 2: Compare with R**

```bash
test "$(git rev-parse HEAD^{tree})" = "$(cat $S/handover/ref-tree.txt)" || git diff --stat $(cat $S/handover/ref-tree.txt) HEAD
```

Expected: equal trees. If they differ, every difference must be one of the recorded stand-alone edits, listed in LEDGER.md. Anything else is a bug; fix it at its unit.

- [ ] **Step 3: Gate the merge**

Run clippy with `--all-features`, the tests of all touched crates, and the full E2E on the merge.

### Task 9: Branch audit and push to the fork

- [ ] **Step 1: Check each PR's diff**

For every PR, `git log --oneline <base>..<head>` must list exactly the unit's commits. No dependency commits may leak into a PR.

- [ ] **Step 2: Push to the fork**

Push all `series-parquet/*` branches to the fork `filimonov`.

- [ ] **Step 3: Independent review**

Run a codex check (`codex exec -m gpt-5.6-sol`) of:
- DEPENDENCY-MAP.md against the branches;
- every PR description against its branch.

Findings are reported only when an item is false or missing. Fix them and re-push.

### Task 10: Handover issue draft

**Files:**
- Create: `scratchpad/handover/ISSUE.md`

- [ ] **Step 1: Write the issue**

In English, with these sections:
1. **Goal and context:**
   - the Alloy → OTLP receiver → durable_buffer → series_parquet deployment;
   - targets: 100k–1M records/s, bounded memory, at-least-once;
   - the Parquet lake layout.
2. **Architecture in brief:**
   - the series-lake format;
   - the ACK contract;
   - the memory model;
   - shutdown and restart behaviour;
   - the operator guide.
3. **What was done and how it was verified:**
   - the campaign stages;
   - the key numbers, each with its source in REPORT.md;
   - the E2E suite, including the SIGKILL tests;
   - the soak and fault runs.
4. **Review history:**
   - four multi-seat reviews and the codex rounds;
   - the scope rule;
   - why the work was split into a minimal core and optional units.
5. **Branch and PR map:** for every PR, a table row with
   - the problem it solves;
   - why it is needed (or why it is optional);
   - its base and dependencies;
   - how it was tested;
   - the chloggen type.
6. **Known limitations:**
   - the power-loss window;
   - the WAL tail written after a restart and a possible duplicate block;
   - the OTLP/HTTP 413 answer for over-burst requests under memory pressure;
   - the replace-rollout directory overlap;
   - flaky tests found along the way.
7. **Backlog:** a summary of plan-4 P0–P3 plus the review-4 and split leftovers, as a checklist.
8. **Questions for reviewers:**
   - should the completion phase go upstream;
   - should the framing check go upstream;
   - core-nodes or contrib-nodes;
   - the format freeze;
   - should the deploy examples stay in the repository.
9. **How to try it:** the reference configs, deploy, and the E2E commands.

- [ ] **Step 2: Fact-check the issue**

Every claim links to a file, a commit or a REPORT.md section. Run the same codex check as in Task 9 on ISSUE.md.

### Task 11: Publish to Altinity/otel-arrow (user go required)

- [ ] **Step 1: Ask the user for the decisions this task needs**

- whether to fast-forward Altinity `main` to `137215724`, or to use a separate base branch;
- whether to enable Issues on Altinity/otel-arrow, or where else the issue goes;
- whether the deploy PR (U10) is opened;
- whether the optional units are opened as ready or as draft;
- reviewers and labels.

- [ ] **Step 2: Push the branches, open the issue, then the PRs in dependency order**

```bash
git push altinity 137215724:refs/heads/main   # only if the user approved the fast-forward
git push altinity 'refs/heads/series-parquet/*:refs/heads/series-parquet/*'
gh issue create -R Altinity/otel-arrow --title "..." --body-file $S/handover/ISSUE.md
gh pr create -R Altinity/otel-arrow --base <base branch> --head series-parquet/pr-NN-<slug> --title "..." --body-file $S/handover/prs/pr-NN-<slug>.md
```

Open the PRs in the map's dependency order. Each PR's base is its dependency branch, or main.

- [ ] **Step 3: Fill in the numbers**

Replace `#TRACKING` and the "Depends on" placeholders with the real numbers. Edit the issue's PR table with the numbers too, using `gh pr edit` and `gh issue edit`.

- [ ] **Step 4: Verify the published PRs**

Each PR's "Files changed" shows only its unit. CI starts. Record the links in LEDGER.md and in the campaign ledger.

---

## Self-review

- Every requirement in Global Constraints is covered:
  - independent PRs from main: Tasks 1–2;
  - minimal dependencies: Task 1 rules, Tasks 3–5;
  - descriptions that meet the upstream template: Tasks 6–7;
  - build and tests per PR: the gates in Tasks 2–5 and 8;
  - the handover issue: Task 10.
- No placeholders except `#TRACKING` and the PR numbers, which Task 11 fills. These are data that do not exist before publication.
