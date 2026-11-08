{ pkgs, config, product, ... }:
let
  # Production binaries: each machine image ships only the binary it runs.
  stubServer = config.packages.stub-server;
  stubClient = config.packages.stub-client;

  # /usr/local/bin carries the stub bins as symlinks so the hardcoded paths in
  # scripts/ keep working and the container's default PATH resolves them.
  # Baseline every dockerTools image needs: /bin/sh, /usr/bin/env (so
  # `#!/usr/bin/env bash` shebangs resolve) and /etc/{passwd,group,
  # nsswitch.conf} (glibc reads nsswitch.conf to resolve names — without it a
  # container cannot look up sibling services on the compose net).
  dockerBase = with pkgs.dockerTools; [
    binSh
    usrBinEnv
    fakeNss
  ];

  imageMeta = {
    "org.opencontainers.image.source" = product.homepage;
    "org.opencontainers.image.version" = product.version;
    "org.opencontainers.image.description" = product.description;
  };
in
{
  # --- production server image -----------------------------------------------
  # Ships stub-server and the real claude only. No mock claude, no e2e harness:
  # what runs in production is exactly what the image contains.
  packages.docker-server = pkgs.dockerTools.buildLayeredImage {
    name = "claude-io-stub-server";
    tag = "latest";
    contents = dockerBase ++ [
      stubServer
      pkgs.bashInteractive
      pkgs.coreutils
      pkgs.curl
      pkgs.claude-code
      pkgs.dockerTools.caCertificates
    ];
    extraCommands = ''
      mkdir -p usr/local/bin root tmp
      chmod 1777 tmp
      ln -s ${stubServer}/bin/stub-server usr/local/bin/stub-server
    '';
    # No entrypoint script: stub-server reads STORE / LOG_DIR / LISTEN /
    # CLAUDE_CMD from the environment itself, so the image command is just the
    # binary. CLAUDE_CMD is left unset => the real `claude` bundled above.
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

  # --- e2e server image -------------------------------------------------------
  # The production image plus the Rust mock claude and the e2e harness, so
  # `scripts/run-e2e-docker.sh` can drive the full suite inside machine B's
  # network namespace (ADR-0001 no-dial attribution). Not published.
  packages.docker-server-test = pkgs.dockerTools.buildLayeredImage {
    name = "claude-io-stub-server-test";
    tag = "latest";
    contents = dockerBase ++ [
      config.packages.stub
      pkgs.bashInteractive
      pkgs.coreutils
      pkgs.curl
      pkgs.claude-code
      pkgs.dockerTools.caCertificates
    ];
    extraCommands = ''
      mkdir -p usr/local/bin root tmp
      chmod 1777 tmp
      ln -s ${config.packages.stub}/bin/stub-server usr/local/bin/stub-server
      ln -s ${config.packages.stub}/bin/claude-mock usr/local/bin/claude-mock
      ln -s ${config.packages.stub}/bin/zonepair-e2e usr/local/bin/zonepair-e2e
    '';
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

  # --- production client image -------------------------------------------------
  # Machine A: stub-client (as `claude`) plus opencode.
  packages.docker-client = pkgs.dockerTools.buildLayeredImage {
    name = "claude-io-stub-client";
    tag = "latest";
    contents = dockerBase ++ [
      stubClient
      pkgs.opencode
      pkgs.bashInteractive
      pkgs.coreutils
      pkgs.dockerTools.caCertificates
    ];
    extraCommands = ''
      mkdir -p usr/local/bin work tmp
      chmod 1777 tmp
      ln -s ${stubClient}/bin/stub-client usr/local/bin/stub-client
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
}
