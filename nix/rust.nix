{ inputs, lib, ... }:
{
  perSystem =
    { pkgs, system, ... }:
    let
      product = import ./meta.nix;
      toolchains = import ./toolchains.nix { fenix = inputs.fenix; };

      # musl static triple per host system — the portable release artifact.
      muslTarget = {
        x86_64-linux = "x86_64-unknown-linux-musl";
        aarch64-linux = "aarch64-unknown-linux-musl";
      };

      # One Cargo build definition, reused by every target. `extra` overrides
      # buildRustPackage args per target (cargoBuildFlags, NIX_LDFLAGS, …).
      mkStub =
        {
          rustPlatform,
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
              mainProgram = "stub-server";
              platforms = lib.platforms.linux;
            }
            // meta;
          }
          // extra
        );

      tcHost = toolchains.hostToolchain system;
      tcStatic = toolchains.toolchainWith system muslTarget.${system};

      # rustc bakes its own sysroot path into std panic locations, which drags
      # the whole toolchain into the binary's closure and into every image.
      # Remap that prefix to a neutral path (what upstream rustc does when it
      # ships) so the reference, and the closure bloat, disappear.
      remap = tc: "--remap-path-prefix=${tc}=/rustc";
    in
    {
      # --- native (glibc) build; runs the unit + e2e tests in `checkPhase` ---
      packages.stub = mkStub {
        rustPlatform = toolchains.rustPlatformFrom pkgs tcHost;
        extra.RUSTFLAGS = remap tcHost;
      };

      # --- static musl release binary ---------------------------------------
      # pkgsStatic forces a musl + crt-static stdenv, so buildRustPackage
      # passes --target <musl> and links statically; the fenix toolchain
      # supplies the matching rust-std.
      packages.stub-static = mkStub {
        rustPlatform = toolchains.rustPlatformFrom pkgs.pkgsStatic tcStatic;
        extra.RUSTFLAGS = "${remap tcStatic} -C target-feature=+crt-static";
      };
    };
}
