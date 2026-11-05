{
  description = "zonepair (claude-io-stub): drive a real `claude` on machine B from an agent harness on machine A — stdio relay + session-store projection + MCP, in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
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
    in
    {
      packages = eachSystem (
        system:
        let
          pkgs = pkgsFor system;
          tcHost = hostToolchain system;
          tcStatic = toolchainWith system muslTarget.${system};
          tcWindows = toolchainWith system "x86_64-pc-windows-gnu";

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

          # --- client .exe cross-built for windows-gnu ----------------------
          # pkgsCross.mingwW64 makes buildRustPackage target windows-gnu; the
          # fenix toolchain supplies the windows rust-std and the binutils come
          # from the mingw stdenv.
          mingw = pkgs.pkgsCross.mingwW64;
          stub-client-windows = mkStub {
            rustPlatform = rustPlatformFrom mingw tcWindows;
            doCheck = false;
            meta = {
              mainProgram = "stub-client";
              platforms = lib.platforms.windows;
            };
            extra = {
              cargoBuildFlags = [
                "--bin"
                "stub-client"
              ];
              RUSTFLAGS = remap tcWindows;
              # rustc's windows-gnu link line asks for -l:libpthread.a, which
              # its self-contained dir does not ship; point the linker at
              # mingw-w64's winpthreads.
              NIX_LDFLAGS = "-L${mingw.windows.pthreads}/lib";
            };
          };

          # --- payload trees laid out at /opt inside the images -------------
          serverOpt = pkgs.runCommand "zonepair-server-opt" { } ''
            mkdir -p $out/opt/mock $out/opt/scripts
            cp -r ${./mock}/. $out/opt/mock/
            cp ${./docker/mcp.json} $out/opt/mcp.json
            cp ${./docker/server-entrypoint.sh} $out/opt/server-entrypoint.sh
            cp ${./scripts/assert-nodial.sh} $out/opt/scripts/assert-nodial.sh
            chmod +x $out/opt/server-entrypoint.sh $out/opt/scripts/assert-nodial.sh
          '';

          clientOpt = pkgs.runCommand "zonepair-client-opt" { } ''
            mkdir -p $out/opt/mock $out/opt/driver $out/opt/scripts
            cp -r ${./mock}/. $out/opt/mock/
            cp -r ${./driver}/. $out/opt/driver/
            cp -r ${./scripts}/. $out/opt/scripts/
            chmod +x $out/opt/driver/*.mjs $out/opt/scripts/*.sh
          '';

          # --- OCI images built by nixpkgs dockerTools (no Dockerfile) -------
          # /usr/local/bin/stub-{server,client} are symlinks into the store so
          # the hardcoded paths in scripts/ and docker/mcp.json keep working.
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
              pkgs.gawk
              pkgs.python3
              pkgs.iproute2
              pkgs.curl
              pkgs.claude-code
              pkgs.dockerTools.caCertificates
            ];
            extraCommands = ''
              mkdir -p usr/local/bin root tmp
              chmod 1777 tmp
              cp -r ${serverOpt}/opt opt
              chmod +x opt/server-entrypoint.sh opt/scripts/assert-nodial.sh
              ln -s ${stub}/bin/stub-server usr/local/bin/stub-server
            '';
            config = {
              Env = [
                "STORE=/data/claude"
                "LOG_DIR=/data/logs"
                "CLAUDE_MOCK=/opt/mock/claude-mock.py"
                "HOME=/root"
                "LANG=C.UTF-8"
              ];
              Entrypoint = [ "/opt/server-entrypoint.sh" ];
              ExposedPorts = {
                "7788/tcp" = { };
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
              pkgs.bashInteractive
              pkgs.coreutils
              pkgs.python3
              pkgs.nodejs
              pkgs.dockerTools.caCertificates
            ];
            extraCommands = ''
              mkdir -p usr/local/bin work tmp
              chmod 1777 tmp
              cp -r ${clientOpt}/opt opt
              chmod +x opt/driver/*.mjs opt/scripts/*.sh
              ln -s ${stub}/bin/stub-client usr/local/bin/stub-client
            '';
            config = {
              Env = [ "LANG=C.UTF-8" ];
              WorkingDir = "/work";
              Cmd = [
                "bash"
                "/opt/scripts/run-e2e-inner.sh"
              ];
              Labels = imageMeta;
            };
          };
        in
        {
          inherit
            stub
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
              pkgs.rust-analyzer
              pkgs.python3
              pkgs.nodejs
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
              shellcheck -x ${./scripts}/*.sh ${./docker/server-entrypoint.sh}
              touch $out
            '';
      });

      formatter = eachSystem (system: (pkgsFor system).nixfmt);
    };
}
