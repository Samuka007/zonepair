{
  perSystem =
    { pkgs, config, ... }:
    {
      # `stub` runs `cargo test` (doCheck) — unit + e2e, the suite the CI gate
      # depends on.
      checks.default = config.packages.stub;
      checks.shellcheck =
        pkgs.runCommand "zonepair-shellcheck" { nativeBuildInputs = [ pkgs.shellcheck ]; }
          ''
            shellcheck -x ${../scripts}/*.sh
            touch $out
          '';
    };
}
