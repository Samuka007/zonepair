# Tests, mocks and protocol drivers are Rust; nix is the only orchestration

Accepted, 2026-11-06. Supersedes the bash/python/node test and mock stack.

The pair is two Rust binaries speaking a line-and-frame protocol. Testing that
protocol through bash scripts that shell out to a python mock and a node driver
meant three toolchains for one job: assertions scattered across shell quoting,
python stdout and node exit codes, and test logic leaking into product images
(the no-dial check's dependence on `iproute2` is the concrete case).

Everything that tests, mocks or drives the pair moves into Rust:

- the mock `claude` and the MCP helper become Rust binaries in this workspace;
- protocol drivers become a client library plus Rust integration tests;
- fixtures (store seeding, mirror assertions, no-dial check) become Rust
  helpers;
- nix is the only orchestration: `nix flake check` runs everything that runs
  natively; a container run starts the images and execs the same Rust harness,
  and contains no assertions of its own.

## Consequences

- python, node and the test-only bash scripts leave the repository; the docker
  images shrink accordingly (no `python3`, no `nodejs`, no `iproute2`).
- bash may still *start containers and exec the Rust harness*; it may not
  contain assertions.
- New integration tests drive the pair through its public entries rather than
  by shelling out, so a carrier change does not rewrite the suite.
- `wiring-env.py`'s job (reading `models.yml` for provider credentials) moves
  into the Rust harness.
