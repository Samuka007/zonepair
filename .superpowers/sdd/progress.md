# SDD Progress — Mirror 增量同步 Phase 1

Baseline: main @ da7218bc1405b8ed3e8250a5e3fe85893e480003 (2026-11-17)
Plan: tmp/impl/plan.md (10 tasks). Specs: tmp/impl/{tech-spec,test-spec,structure}.md, tmp/rfc/0001-0004.
Constraints: NO local nix (disk). Local: cargo only, resource-limited. Heavy checks → CI. Models: devin/swe-2 | devin/swe-2-flash only.

## Gates
- [ ] T1 workspace skeleton & file migration (serial)
- [ ] T2 zonepair-proto sync types
- [ ] T3 B-side Store primitives + atomic write
- [ ] T4 B-side changes/publish routes
- [ ] T5 A-side disk protocol (state/journal/apply/classify)
- [ ] T6 A-side sync API client
- [ ] T7 zonepaird execution (worker/reconcile/wait)
- [ ] T8 zonepaird process shell + startup
- [ ] T9 transport adapter + CLI surface + push retirement
- [ ] T10 e2e extension + crash injection
- [ ] Final integration review + CI verification

## CLOSED — Phase 1 complete (2026-11-18)
- [x] T1–T10 all tasks + T-11 defect fixes + T-12 polish
- [x] Integration review: 1 false-critical (Phase-2 materialization, declared non-goal), 1 real important (request timeout — fixed T-12)
- [x] Evidence: 164 client + 15 server store + 15 sync_routes + 18 proto tests; E2E PASS ×N (21 sessions); cargo check --workspace zero warnings; nix build .#stub EXIT=0
- [x] 60 files staged on impl/mirror-sync, +19507/-3788
- Next: user commits/pushes, tags v* for cloud image build

## CLOSED — 单阶段交付（Phase 1 + Phase 2 物化面合并）2026-11-18
- [x] T1–T17 全部落地 + T-18 e2e 重构 + 五轮 review（integration/wave-2/四路全量/三路简化/三路必要性）
- [x] 修复：2 Critical（body-limit、deferred-evict provenance）+ 10+ Important + ~20 Minor
- [x] 删除波：wait_cursor 体系（RFC 同步标注）、ensure-task writing 收窄、死代码/僵尸依赖
- [x] 证据：client 207 / server 32 / proto 18 / e2e 23 sessions PASS / workspace 零警告 / nix build EXIT=0
- [x] 84 files staged on impl/mirror-sync，+23867/-2995（含 tmp/ spec 文档 1400 行，git add -f 强制——入库与否用户决定）
- 用户动作：commit/push/tag v* → release.yml 出镜像
