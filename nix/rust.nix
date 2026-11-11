{
  lib,
  pkgs,
  system,
  product,
  toolchains,
  config,
  ...
}:
let
  # musl static triple per host system — the portable release artifact.
  # Static linking is a linux/musl story: darwin has no musl target, and
  # Apple's codesigning gate refuses unknown LC segments that manual static
  # tricks produce, so darwin ships a normal (dynamic) release build instead.
  muslTarget = {
    x86_64-linux = "x86_64-unknown-linux-musl";
    aarch64-linux = "aarch64-unknown-linux-musl";
  };

  # One Cargo build definition, reused by every target. `extra` overrides
  # buildRustPackage args per target (RUSTFLAGS, NIX_LDFLAGS, …).
  #
  # `bins` (optional) restricts which binaries are built AND installed:
  # each bin becomes a `--bin` flag on cargoBuildFlags, so cargo only builds
  # what the package should install (the install hook copies the built bins).
  # null = no flags = the whole workspace, all four bins (`stub`).
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
          # linux + darwin; callers narrow this via `meta` for cross builds
          platforms = lib.platforms.linux ++ lib.platforms.darwin;
        }
        // meta;
      }
      // lib.optionalAttrs (bins != null) {
        cargoBuildFlags = builtins.concatMap (b: [
          "--bin"
          b
        ]) bins;
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

  # --- platform-gated release binaries -----------------------------------------
  # stub-static (linux): pkgsStatic forces a musl + crt-static stdenv, so
  # buildRustPackage passes --target <musl> and links statically; the fenix
  # toolchain supplies the matching rust-std. Release ships only the two
  # production binaries. linux-only: darwin has no musl target — declared in
  # meta.platforms, so darwin eval refuses locally instead of breaking.
  # stub-darwin (darwin): plain dynamic release build on the host toolchain —
  # darwin has no static-linking story. Its meta.platforms is darwin-only, so
  # linux machines simply can't build it (and never need to).
  packages.stub-static = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs.pkgsStatic tcStatic;
    bins = [
      "stub-server"
      "stub-client"
    ];
    meta.platforms = [
      "x86_64-linux"
      "aarch64-linux"
    ];
    extra.RUSTFLAGS = "${remap tcStatic} -C target-feature=+crt-static";
  };

  packages.stub-darwin = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs tcHost;
    bins = [
      "stub-server"
      "stub-client"
    ];
    meta.platforms = [ "aarch64-darwin" ];
    extra.RUSTFLAGS = remap tcHost;
  };

  # --- dynamic linux production build ------------------------------------------
  # Maintained alongside the static release artifact — apt/deb packaging later
  # wants the loader-interfacing glibc variant. Same production bins, dynamic
  # link, host toolchain. Not a release asset; `nix flake check` keeps it green
  # so the future deb input never rots.
  packages.stub-linux = mkStub {
    rustPlatform = toolchains.rustPlatformFrom pkgs tcHost;
    bins = [
      "stub-server"
      "stub-client"
    ];
    meta.platforms = lib.platforms.linux;
    extra.RUSTFLAGS = remap tcHost;
  };

  # --- release staging ----------------------------------------------------------
  # dist/ with the published asset names. Linux stages the static musl pair
  # (the portable release artifact); macOS stages its dynamic pair; the
  # cross-built .exe stages from the windows package. Names are byte-stable
  # across releases — consumers and the tag workflow both rely on them.
  packages.release-dist =
    let
      pkg =
        if pkgs.stdenv.hostPlatform.isDarwin then
          config.packages.stub-darwin
        else
          config.packages.stub-static;
    in
    pkgs.runCommand "zonepair-release-dist" { } ''
      mkdir -p $out/dist
      install -m755 ${pkg}/bin/stub-server $out/dist/stub-server-${system}
      install -m755 ${pkg}/bin/stub-client $out/dist/stub-client-${system}
    '';

  packages.release-dist-windows = pkgs.runCommand "zonepair-release-dist-windows" { } ''
    mkdir -p $out/dist
    install -m755 \
      ${config.packages.stub-client-windows}/bin/stub-client.exe \
      $out/dist/stub-client-x86_64-windows.exe
  '';
}
