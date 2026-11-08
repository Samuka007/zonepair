{ lib, pkgs, system, product, toolchains, ... }:
let
  # musl static triple per host system — the portable release artifact.
  muslTarget = {
    x86_64-linux = "x86_64-unknown-linux-musl";
    aarch64-linux = "aarch64-unknown-linux-musl";
  };

  # One Cargo build definition, reused by every target. `extra` overrides
  # buildRustPackage args per target (cargoBuildFlags, NIX_LDFLAGS, …).
  #
  # `bins` (optional) restricts which binaries are built AND installed —
  # production images ship only the binaries they run, so the mock claude
  # and the e2e harness never end up in a production artifact.
  mkStub =
    {
      rustPlatform,
      bins ? null,
      mainProgram ? "stub-server",
      doCheck ? true,
      meta ? { },
      extra ? { },
    }:
    rustPlatform.buildRustPackage (
      {
        inherit (product)
          pname
          version
          description
          homepage
          ;
        inherit doCheck;
        src = ../.;
        cargoLock.lockFile = ../Cargo.lock;
        meta = {
          inherit (product) description homepage;
          inherit mainProgram;
          platforms = lib.platforms.linux;
        }
        // meta;
      }
      // extra
    );

  tcHost = toolchains.hostToolchain system;
  tcStatic = toolchains.toolchainWith system muslTarget.${system};

  # rustc bakes its own sysroot path into std panic locations, which drags the
  # whole toolchain into the binary's closure and into every image. Remap that
  # prefix to a neutral path (what upstream rustc does when it ships) so the
  # reference, and the closure bloat, disappear.
  remap = tc: "--remap-path-prefix=${tc}=/rustc";
in
{
  # --- native (glibc) build; runs the unit + e2e tests in `checkPhase` -------
  packages.stub = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs tcHost;
    extra.RUSTFLAGS = remap tcHost;
  };

  # --- single-bin production builds -------------------------------------------
  # Each machine ships only the binary it runs: machine B runs stub-server
  # (the mock claude and the e2e harness are test-only and must not land in a
  # production image), machine A runs stub-client.
  packages.stub-server = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs tcHost;
    bins = [ "stub-server" ];
    extra.RUSTFLAGS = remap tcHost;
  };

  packages.stub-client = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs tcHost;
    bins = [ "stub-client" ];
    meta.mainProgram = "stub-client";
    extra.RUSTFLAGS = remap tcHost;
  };

  # --- static musl release binary ---------------------------------------------
  # pkgsStatic forces a musl + crt-static stdenv, so buildRustPackage passes
  # --target <musl> and links statically; the fenix toolchain supplies the
  # matching rust-std. Release ships only the two production binaries.
  packages.stub-static = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs.pkgsStatic tcStatic;
    bins = [
      "stub-server"
      "stub-client"
    ];
    extra.RUSTFLAGS = "${remap tcStatic} -C target-feature=+crt-static";
  };
}
