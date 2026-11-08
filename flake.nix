{
  description = "zonepair (claude-io-stub): drive a real `claude` on machine B from an agent harness on machine A — stdio relay + session-store projection + MCP, in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      inherit (nixpkgs) lib;

      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      eachSystem = lib.genAttrs systems;

      # claude-code carries an unfree license; permit just that package so the
      # server image can bundle it without a blanket allowUnfree.
      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          config.allowUnfreePredicate = pkg: builtins.elem (lib.getName pkg) [ "claude-code" ];
        };

      pname = "zonepair";
      version = "0.1.0";
      description = "Cross-machine claude stdio relay + session-store projection";

      # musl static triple per host system — the portable release artifact.
      muslTarget = {
        x86_64-linux = "x86_64-unknown-linux-musl";
        aarch64-linux = "aarch64-unknown-linux-musl";
      };

      # fenix ships prebuilt Rust toolchains, so no compiler bootstrap happens
      # per build; flake.lock pins the exact rustc revision.
      # `combine` merges a toolchain with extra rust-std components into one
      # derivation whose bin/ holds both cargo and rustc, which is exactly what
      # makeRustPlatform wants for both arguments. The extra rust-std is what
      # lets the same rustc emit for musl / windows-gnu.
      fenixPkgs = system: fenix.packages.${system};
      hostToolchain = system: (fenixPkgs system).stable.toolchain;
      toolchainWith =
        system: target:
        (fenixPkgs system).combine [
          (fenixPkgs system).stable.toolchain
          (fenixPkgs system).targets.${target}.stable.rust-std
        ];
      rustPlatformFrom =
        pkgs: toolchain:
        pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
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
            inherit pname version doCheck;
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            meta = {
              inherit description;
              homepage = "https://github.com/Samuka007/zonepair";
              mainProgram = "stub-server";
              platforms = lib.platforms.linux;
            }
            // meta;
          }
          // extra
        );

      # The windows-gnu cross build is the crane one (crane.lib's
      # overrideToolchain exists precisely for this): crane drives cargo
      # against a mingwW64 stdenv, whose cc wrapper is the cross linker.
      # The host-side fenix toolchain must be built for the build platform,
      # hence toolchainWith on the *host* system.
      stubClientWindows =
        system:
        let
          mingw = (pkgsFor system).pkgsCross.mingwW64;
          craneWinLib = (crane.mkLib mingw).overrideToolchain (_: toolchainWith system "x86_64-pc-windows-gnu");
        in
        craneWinLib.buildPackage {
          inherit pname version;
          src = craneWinLib.cleanCargoSource self;
          strictDeps = true;
          # Cross builds have nothing to run tests on; the e2e harness needs
          # real claude/docker and stays out of the build gate anyway.
          doCheck = false;
          meta = {
            inherit description;
            homepage = "https://github.com/Samuka007/zonepair";
            mainProgram = "stub-client";
            platforms = lib.platforms.windows;
          };
          CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
          CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
          cargoBuildFlags = [
            "--bin"
            "stub-client"
          ];
          # rustc's windows-gnu link line asks for -l:libpthread.a, which
          # its self-contained dir does not ship; point the linker at
          # mingw-w64's winpthreads.
          NIX_LDFLAGS = "-L${mingw.windows.pthreads}/lib";
        };
    in
    {
      packages = eachSystem (
        system:
        let
          pkgs = pkgsFor system;

          # A-side harness: the Claude Agent provider (and the
          # OPENCODE_CLAUDE_PATH knob the lab relies on) needs opencode >= 2.2.3.
          opencode = pkgs.opencode;

          tcHost = hostToolchain system;
          tcStatic = toolchainWith system muslTarget.${system};

          # rustc bakes its own sysroot path into std panic locations, which
          # drags the whole toolchain into the binary's closure and into every
          # image. Remap that prefix to a neutral path (what upstream rustc does
          # when it ships) so the reference, and the closure bloat, disappear.
          remap = tc: "--remap-path-prefix=${tc}=/rustc";

          # --- native (glibc) build; runs the unit tests in `checkPhase` -----
          stub = mkStub {
            rustPlatform = rustPlatformFrom pkgs tcHost;
            extra.RUSTFLAGS = remap tcHost;
          };

          # --- static musl release binary -----------------------------------
          # pkgsStatic forces a musl + crt-static stdenv, so buildRustPackage
          # passes --target <musl> and links statically; the fenix toolchain
          # supplies the matching rust-std.
          staticPkgs = pkgs.pkgsStatic;
          stub-static = mkStub {
            rustPlatform = rustPlatformFrom staticPkgs tcStatic;
            extra.RUSTFLAGS = "${remap tcStatic} -C target-feature=+crt-static";
          };

          # --- client .exe cross-built for windows-gnu (crane) ---------------
          stub-client-windows = stubClientWindows system;

          # The mock `claude` and MCP helper are Rust binaries from the same
          # workspace (ADR-0006): they ship inside the images with the stub
          # bins, so there is no separate mock package and no python/node in
          # any image.

          # --- OCI images built by nixpkgs dockerTools (no Dockerfile) -------
          # /usr/local/bin carries the stub package's bins (server, mock claude,
          # e2e harness) as symlinks so the hardcoded paths in scripts/ keep
          # working and the container's default PATH resolves them.
          # Baseline every dockerTools image needs: /bin/sh, /usr/bin/env (so
          # `#!/usr/bin/env bash` shebangs resolve) and /etc/{passwd,group,
          # nsswitch.conf} (glibc reads nsswitch.conf to resolve names — without
          # it a container cannot look up sibling services on the compose net).
          dockerBase = with pkgs.dockerTools; [
            binSh
            usrBinEnv
            fakeNss
          ];

          imageMeta = {
            "org.opencontainers.image.source" = "https://github.com/Samuka007/zonepair";
            "org.opencontainers.image.version" = version;
            "org.opencontainers.image.description" = description;
          };

          docker-server = pkgs.dockerTools.buildLayeredImage {
            name = "claude-io-stub-server";
            tag = "latest";
            contents = dockerBase ++ [
              stub
              pkgs.bashInteractive
              pkgs.coreutils
              pkgs.curl
              pkgs.claude-code
              pkgs.dockerTools.caCertificates
            ];
            extraCommands = ''
              mkdir -p usr/local/bin root tmp
              chmod 1777 tmp
              ln -s ${stub}/bin/stub-server usr/local/bin/stub-server
              ln -s ${stub}/bin/claude-mock usr/local/bin/claude-mock
              ln -s ${stub}/bin/zonepair-e2e usr/local/bin/zonepair-e2e
            '';
            # No entrypoint script: stub-server reads STORE / LOG_DIR /
            # LISTEN / CLAUDE_CMD from the environment itself, so the image
            # command is just the binary. CLAUDE_CMD is left unset => the real
            # `claude` bundled above; the mock suite overrides it.
            config = {
              Env = [
                "LISTEN=0.0.0.0:7789"
                "STORE=/data/claude"
                "LOG_DIR=/data/logs"
                "HOME=/root"
                "LANG=C.UTF-8"
              ];
              Cmd = [ "stub-server" ];
              ExposedPorts = {
                "7789/tcp" = { };
              };
              Volumes = {
                "/data" = { };
              };
              WorkingDir = "/";
              Labels = imageMeta;
            };
          };

          docker-client = pkgs.dockerTools.buildLayeredImage {
            name = "claude-io-stub-client";
            tag = "latest";
            contents = dockerBase ++ [
              stub
              opencode
              pkgs.bashInteractive
              pkgs.coreutils
              pkgs.dockerTools.caCertificates
            ];
            extraCommands = ''
              mkdir -p usr/local/bin work tmp
              chmod 1777 tmp
              ln -s ${stub}/bin/stub-client usr/local/bin/stub-client
            '';
            config = {
              Env = [
                "CLAUDE_STUB_URL=ws://b:7789"
                "LANG=C.UTF-8"
              ];
              WorkingDir = "/work";
              Cmd = [ "opencode" ];
              Labels = imageMeta;
            };
          };
        in
        {
          inherit
            stub
            opencode
            stub-static
            stub-client-windows
            docker-server
            docker-client
            ;
          default = stub;
        }
      );

      devShells = eachSystem (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = pkgs.mkShell {
            packages = [
              (hostToolchain system)
              pkgs.opencode
              pkgs.claude-code
              pkgs.rust-analyzer
              pkgs.jq
              pkgs.iproute2
              pkgs.shellcheck
            ];
          };

          windows = pkgs.mkShell {
            packages = [
              (toolchainWith system "x86_64-pc-windows-gnu")
              pkgs.pkgsCross.mingwW64.stdenv.cc
            ];
            # See the NIX_LDFLAGS note on stub-client-windows above.
            NIX_LDFLAGS = "-L${pkgs.pkgsCross.mingwW64.windows.pthreads}/lib";
            CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
          };
        }
      );

      checks = eachSystem (system: {
        # `stub` runs `cargo test` (doCheck) over tests/store.rs — the unit
        # suite the CI gate depends on.
        default = self.packages.${system}.stub;
        shellcheck =
          (pkgsFor system).runCommand "zonepair-shellcheck"
            {
              nativeBuildInputs = [ (pkgsFor system).shellcheck ];
            }
            ''
              shellcheck -x ${./scripts}/*.sh
              touch $out
            '';
      });

      formatter = eachSystem (system: (pkgsFor system).nixfmt);
    };
}
