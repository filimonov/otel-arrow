# Series-parquet PR Series and Handover Issue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the series-parquet work into ten topic PRs that mirror the future upstream PRs. Eight are independent of each other and branch from origin/main. The last two are the series_parquet exporter and the integration of the optional features plus the deployment examples. A handover issue explains every PR. Everything is opened in Altinity/otel-arrow for internal review.

**Architecture:** The content is fixed: the fork's core branch `series-parquet-upstream` (12 commits) plus the eight `sp/*` branches. This plan regroups that content into ten topic PRs. Each PR is based on origin/main unless it truly needs another PR. The plan then proves that all ten branches merged together give the same tree as today. PR 9 and PR 10 need several PRs, so each gets a generated `deps-*` integration branch as its base. Their GitHub diffs then show only their own code.

**Tech Stack:** git (cherry-pick, merge, rebase), cargo workspace `rust/otap-dataflow`, `make chlog-validate`, the Python E2E suite, gh CLI.

**Spec:** the user's requirements (2026-09-28), quoted in Global Constraints, and the ten-PR layout the user approved (the table below).

Background:
- `docs/superpowers/reports/series-parquet-measurement/REPORT.md` (campaign report);
- `docs/superpowers/plans/2026-09-23-plan-4-backlog.md` (backlog);
- scratchpad `upstream-review/SPLIT-REPORT.md` (current branches) and `REVIEW4-FIX-BRIEF.md` (the scope rule).

## Global Constraints

- The user's requirements (2026-09-28):
  - "Every PR contains a fully correct description that meets the upstream requirements, and passes the build and tests."
  - "Everything that can branch from upstream/main must branch from it."
  - "An issue describes all branches, why they were added, the problems they solve, and why they are needed."
  - "Every PR that branches from main contains tests and everything it needs to be merged on its own."
  - "A PR that depends on others depends only on those where there would otherwise be hard conflicts or obvious code defects."
  - "Too many, too fragmented." Hence the ten topic PRs below; do not split them further.
- Upstream base: origin/main `137215724`. Altinity/otel-arrow `main` is 3 commits behind it and is a fast-forward ancestor.
- The content source of truth is the fork refs at plan start, recorded in Task 0. The core is `1ab1402ea`. The sp/* tips:

  | Branch | Tip |
  |---|---|
  | otlp-framing | `4d61d0441` |
  | pdata-id-overflow | `64675dcce` |
  | graceful-shutdown-acks | `5ebe4b63c` |
  | s3-unsigned-azure | `21b4e6227` |
  | jemalloc-background-thread | `47cd1cd87` |
  | receiver-refusal-extras | `fa2baf4cb` |
  | durable-buffer-small | `c3bcc91e4` |
  | misc | `ea4d4ebca` |

- No behaviour changes. Edits are allowed only where a PR must stand alone:
  - a moved test helper;
  - an import;
  - a README sentence or link that names a later PR;
  - a changelog entry;
  - a feature flag.

  Record each edit in the ledger.
- Commits inside a PR stay logical and unmixed, as today. A PR may hold several commits. Commit messages stay as they are unless content moves between PRs.
- PR descriptions follow `.github/pull_request_template.md`: Change summary, Related issue, Validation, User-facing changes. A chore PR follows `.github/PULL_REQUEST_TEMPLATE/chore.md` and carries `chore` in its title.
- Every user-facing Rust change carries a `rust/otap-dataflow/.chloggen/*.yaml` entry:
  - its component is listed in `config.yaml`;
  - the note is at most 200 characters and the subtext at most 300;
  - a `breaking` entry has a `Migration:` subtext;
  - `issues:` stays `[0]` until Task 9 fills in real numbers.
- Commit trailers carry only `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Branch names:
  - `series-parquet/pr-NN-<slug>` for PR heads;
  - `series-parquet/deps-<slug>` for generated bases.

  They are pushed to the fork `filimonov` first, and to Altinity only in Task 9.
- cargo runs only under the host lease: `flock -w 3600 /tmp/series-parquet-host-measurement.lock` or the wrapper `scratchpad/u1/cargo.sh`. Test binaries run under `timeout -s KILL 900`.
- Nothing is published to Altinity/otel-arrow without the user's explicit go (Task 9). No upstream (open-telemetry) PRs.

## The ten PRs

| PR | Topic | Content (current commits) | Base |
|---|---|---|---|
| 01 | otlp-receiver: retryable refusals | d95727c3b shared RetryInfo; 6fb95528d OTLP UNAVAILABLE; aad3ba6a3 OTAP batch UNAVAILABLE; a6fd83777 refuse at max_concurrent_requests; 2df326a84 oversized INVALID_ARGUMENT, over-burst retryable; 8b078760d lowered-limit warning | main |
| 02 | otap: object store for exporters | 0126f656b shared wiring; cf3208b91 S3 unsigned_payload; d3d024842 Azure endpoint | main |
| 03 | pdata: id overflow and conversion failures | d67df116d u16 ids; 381768422 delta ids; d36b7cdd7 permanent refusal of conversion failures (otap/parquet exporters, durable_buffer convert branch); dd40aa280 UTF-8 repair count | main |
| 04 | pdata + exporters: OTLP framing check | f9097ce40 framing check; ee55a5ed1 prost-conformant views; f94e19c22 file/otap/parquet refuse broken framing | main, or PR 03 if d36b7cdd7 and f94e19c22 cannot be separated without real rewriting |
| 05 | durable_buffer: operability | 24163c215 oldest_pending.age; 6b8eaf48a conversion_failed outcome; 8d3050f63 docs; 5bb7247cb log rate limits | main |
| 06 | engine + durable_buffer: graceful shutdown | 43c92c5dc completion phase; b3c71e1a0 ack recording at shutdown + await_acks; 6079bd7ad completion-slot reservation | main |
| 07 | engine: jemalloc background thread | cc49d917d | main |
| 08 | series-lake crate | 9dec6284f | main |
| 09 | series_parquet exporter | 4bce2c1ef byte-size deserializers; 5946d85fb completion route; c134a8f9d exporter; 7f7d594d8 configs + Alloy; 3f6a68d0d E2E + workflow | `deps-exporter` = main + PR 02 + PR 08 |
| 10 | series_parquet: optional features + deployment | the adapters 5ebe4b63c, 4d61d0441, 64675dcce, 21b4e6227, fa2baf4cb, 12613cf46, 47cd1cd87, c3bcc91e4, then 1ab1402ea deploy examples | `deps-all` = PR 09 + PRs 01, 03–07 |

Not opened as a PR: ea4d4ebca (node-panic characterization test). The handover issue records the limitation it documents, and the backlog keeps it.

Ordering inside PR 10: the deploy commit 1ab1402ea is the base content. The adapters that edit deploy (47cd1cd87, c3bcc91e4, and those that touch alerts) come after it. Task 1 fixes the exact order.

## Review Focus

- **A PR's tests use a helper that only another PR adds.** Such a PR compiles only in the stack. Task 2 builds and tests every PR on its own base.
- **A README, config comment, workflow path or alert in a PR names a file, metric or feature of a later PR.** Examples: PR 09's README naming unsigned_payload or await_acks; PR 05's docs naming the shutdown storage worker from PR 06. The per-PR grep in Task 2 catches this.
- **A changelog entry sits in the wrong PR, or a PR has user-facing changes and no entry.** Task 4 checks every branch on its own.
- **A PR's GitHub diff shows its dependencies' commits** because its base is wrong. Task 6 compares each `base..head` with the table.
- **Merge drift.** The ten branches merged must equal the reference tree R (core plus all sp/* merged). Task 5 checks the tree and runs the full E2E on the merge.

---

### Task 0: Setup and reference tree

**Files:**
- Create: `scratchpad/handover/LEDGER.md`
- Create: `scratchpad/handover/refs-at-start.txt`
- Create: `scratchpad/handover/ref-tree.txt`

- [ ] **Step 1: Record the starting refs**

```bash
cd /home/mfilimonov/workspace/otel-arrow
git fetch filimonov
git ls-remote filimonov 'refs/heads/series-parquet-upstream' 'refs/heads/sp/*' \
  > $S/handover/refs-at-start.txt
```

Expected: the tips listed in Global Constraints. If any differ, stop and report.

- [ ] **Step 2: Build the reference tree R (everything together)**

```bash
git worktree add $S/handover/wt filimonov/series-parquet-upstream
cd $S/handover/wt && git checkout -b ref-all
for b in otlp-framing pdata-id-overflow graceful-shutdown-acks s3-unsigned-azure \
         jemalloc-background-thread receiver-refusal-extras durable-buffer-small misc; do
  git merge --no-edit filimonov/sp/$b || break
done
git rev-parse 'HEAD^{tree}' > $S/handover/ref-tree.txt
```

Resolve any conflict by keeping both sides, as the sp/* rebases did. Record every resolution in LEDGER.md.

- [ ] **Step 3: Gate R**

Under the lease, run:
- `cargo check --workspace --all-targets`;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`;
- the full Python E2E.

Expected: all green. R is the target of Task 5.

### Task 1: Probe the bases

**Files:**
- Create: `scratchpad/handover/DEPENDENCY-MAP.md`

- [ ] **Step 1: Probe PRs 01–08 alone on origin/main**

For each PR, on a throwaway branch from `137215724`:

```bash
git checkout -B probe 137215724
git cherry-pick <the PR's commits in table order>
cargo check --workspace --all-targets
```

Then run the tests of the crates the PR touches. For each PR, record one of:
- clean;
- a conflict (files);
- a build error (missing items, and which PR defines them);
- a test failure (names).

- [ ] **Step 2: Resolve every non-clean result**

Use these rules, in order:
1. The PR stays on main after a minimal stand-alone edit, if the obstacle is context lines, a doc sentence, a test helper or an import. Record the edit.
2. The PR moves onto one other PR only for a hard conflict or a real code dependency. Record the evidence. The expected case is PR 04 onto PR 03.
3. Content is never moved between PRs without recording why.

- [ ] **Step 3: Probe PR 09 and PR 10 on their deps bases**

Build `deps-exporter`: main, then merge PR 02, then merge PR 08. Cherry-pick PR 09's commits onto it. Build `deps-all`: PR 09, then merge PRs 01 and 03–07. Cherry-pick PR 10's commits onto it. Record the results as above, and fix PR 10's internal commit order.

- [ ] **Step 4: Stop for review**

Send DEPENDENCY-MAP.md to the controller before building the final branches.

### Task 2: Build and gate PRs 01–08

- [ ] **Step 1: Create each branch**

```bash
git checkout -B series-parquet/pr-NN-<slug> <base from the map>
git cherry-pick <commits>
```

Apply the recorded stand-alone edits as `--fixup` commits onto the owning commit. Then run `git -c sequence.editor=: rebase -i --autosquash <base>`.

- [ ] **Step 2: Gate each branch**

Under the lease:
- `cargo check --workspace --all-targets` with 0 warnings, and the same per commit;
- tests of every crate the PR touches;
- clippy `-D warnings` on those crates with their features;
- `cargo fmt --check`;
- markdownlint on changed md;
- `make chlog-validate CHLOGGEN=~/go/bin/chloggen`;
- `python3 tools/sanitycheck.py`;
- a grep that no changed file names a path, metric, config key or feature that exists only in a later PR.

Expected: all green. Record the results per branch in LEDGER.md.

### Task 3: Build and gate PR 09 and PR 10

- [ ] **Step 1: Build the deps bases and the two PRs**

Build `series-parquet/deps-exporter` and `series-parquet/deps-all` from the final PR branches, with `--no-ff` merges. Build `series-parquet/pr-09-series-parquet-exporter` and `series-parquet/pr-10-series-parquet-integration` on them, in the order fixed in Task 1.

- [ ] **Step 2: Gate PR 09**

Run everything from Task 2's gate, plus:
- the full Python E2E;
- `scripts/validate-configs.sh`.

PR 09 is the minimal working exporter, so its E2E must pass without any feature from PRs 01 or 03–07.

- [ ] **Step 3: Gate PR 10**

Run Task 2's gate, plus:
- the full Python E2E;
- `scripts/validate-configs.sh`;
- the deploy checks: `check.py`, `test_check.py`, `promtool check rules` and kubeconform.

### Task 4: Changelog audit

- [ ] **Step 1: Check every PR's changelog entries**

For each PR, list the `.chloggen/*.yaml` files added by `base..head`. Every user-facing change of the PR has an entry, and no entry describes another PR's change. Breaking entries have `Migration:`.

- [ ] **Step 2: Fix and re-gate**

Fix any problem with a fixup and re-run the PR's gate.

### Task 5: Equivalence and full-system check

- [ ] **Step 1: Merge the ten PR branches**

Merge them in order 01–10 onto a scratch branch from `137215724`.

- [ ] **Step 2: Compare with R**

```bash
test "$(git rev-parse 'HEAD^{tree}')" = "$(cat $S/handover/ref-tree.txt)" \
  || git diff --stat "$(cat $S/handover/ref-tree.txt)" HEAD
```

Expected: equal, except for ea4d4ebca, which is not in any PR, and the recorded stand-alone edits. Every other difference is a bug; fix it at its PR.

- [ ] **Step 3: Gate the merge**

Run clippy with `--all-features`, the tests of all touched crates, and the full E2E.

### Task 6: PR descriptions and branch audit

**Files:**
- Create: `scratchpad/handover/prs/pr-NN-<slug>.md` (ten files)

- [ ] **Step 1: Write each description from the upstream template**

Sections:
- **Change summary:** what and why, from the commit messages, 5–10 sentences.
- **Related issue:** `Part of #TRACKING`.
- **Depends on:** "none", or the PR list.
- **Validation:** the exact gate commands and results from LEDGER.md.
- **User-facing changes:** each chloggen entry, or "None".
- **Commits in this PR:** one line per commit.
- **Notes for reviewers:** where to start reading, known limits, backlog items that stay open.

- [ ] **Step 2: Check each description against its branch**

Grep every file, key, metric and behaviour the description names on that branch.

- [ ] **Step 3: Audit each branch's commits**

`git log --oneline <base>..<head>` must list exactly the PR's commits.

- [ ] **Step 4: Push and review**

Push all `series-parquet/*` branches to the fork. Run a codex check (`codex exec -m gpt-5.6-sol`) of DEPENDENCY-MAP.md and the ten descriptions against the branches. Report only false or missing items; fix and re-push.

### Task 7: Handover issue draft

**Files:**
- Create: `scratchpad/handover/ISSUE.md`

- [ ] **Step 1: Write the issue in English**

Sections:
1. **Goal and context:**
   - the Alloy → OTLP receiver → durable_buffer → series_parquet deployment;
   - targets: 100k–1M records/s, bounded memory, at-least-once;
   - the Parquet lake.
2. **Architecture in brief:**
   - the series-lake format;
   - the ACK contract;
   - the memory model;
   - shutdown and restart;
   - the operator guide.
3. **What was done and how it was verified:**
   - the campaign stages;
   - the key numbers, each with its REPORT.md section;
   - the E2E suite, including the SIGKILL tests;
   - the soak and fault runs.
4. **Review history:**
   - four multi-seat reviews and the codex rounds;
   - the scope rule;
   - why the work became eight independent topic PRs, a minimal exporter PR and an integration PR.
5. **The ten PRs.** For each one:
   - the problem it solves;
   - why it is needed, or why it is optional for the exporter;
   - its base and dependencies;
   - how it was tested;
   - its changelog type.
6. **Known limitations:**
   - the power-loss window;
   - the WAL tail written after a restart, and one possibly repeated block;
   - the OTLP/HTTP 413 answer for over-burst requests under pressure;
   - the replace-rollout directory overlap;
   - node-panic behaviour (ea4d4ebca);
   - flaky tests seen.
7. **Backlog:** plan-4 P0–P3 plus the review-4 and split leftovers, as a checklist.
8. **Questions for reviewers:**
   - completion phase upstream or not;
   - framing check upstream or not;
   - core-nodes or contrib-nodes;
   - the format freeze;
   - deploy examples in the repository or not.
9. **How to try it:** the reference configs, deploy, and the E2E commands.

- [ ] **Step 2: Fact-check the issue**

Every claim links to a file, a commit or a REPORT.md section. Run the codex check of Task 6 on ISSUE.md.

### Task 8: User review

- [ ] **Step 1: Show the drafts**

Show the user DEPENDENCY-MAP.md, the ten descriptions and ISSUE.md. Offer a single readable page. Apply the user's edits.

### Task 9: Publish to Altinity/otel-arrow (explicit user go required)

- [ ] **Step 1: Get the user's decisions**

Ask the user:
- whether to fast-forward Altinity `main` to `137215724`, or to use a separate base branch;
- whether to enable Issues on Altinity/otel-arrow, or where else the issue goes;
- whether PR 10 is opened as ready or as a draft;
- reviewers and labels.

- [ ] **Step 2: Push, open the issue, then the PRs**

```bash
git push altinity 'refs/heads/series-parquet/*:refs/heads/series-parquet/*'
gh issue create -R Altinity/otel-arrow --title "<title>" --body-file $S/handover/ISSUE.md
gh pr create -R Altinity/otel-arrow --base <base> \
  --head series-parquet/pr-NN-<slug> --title "<title>" \
  --body-file $S/handover/prs/pr-NN-<slug>.md
```

Open the PRs in order 01–10.

- [ ] **Step 3: Fill in the numbers**

Replace `#TRACKING`, the "Depends on" numbers and the issue's PR table with the real numbers, using `gh pr edit` and `gh issue edit`.

- [ ] **Step 4: Verify the published PRs**

Each PR's "Files changed" shows only its own commits, and CI starts. Record the links in LEDGER.md and in the campaign ledger.

---

## Self-review

- Coverage of Global Constraints:
  - independent PRs from main: Tasks 1–2;
  - minimal dependencies: the Task 1 rules and the deps bases;
  - template-conformant descriptions: Task 6;
  - build and tests per PR: Tasks 2, 3 and 5;
  - changelog: Task 4;
  - the issue: Task 7;
  - the ten-PR layout: the table.
- Placeholders left on purpose: `#TRACKING` and the PR numbers. These are data that exist only after Task 9 publishes.
