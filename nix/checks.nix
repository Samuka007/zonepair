{ config, ... }:
{
  # `stub` runs `cargo test` (doCheck) — unit + e2e, the suite the CI gate
  # depends on.
  checks.default = config.packages.stub;
}
