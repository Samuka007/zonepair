{
  perSystem =
    { pkgs, config, ... }:
    let
      product = import ./meta.nix;
      stub = config.packages.stub;

      # /usr/local/bin carries the stub package's bins (server, mock claude,
      # e2e harness) as symlinks so the hardcoded paths in scripts/ keep
      # working and the container's default PATH resolves them.
      # Baseline every dockerTools image needs: /bin/sh, /usr/bin/env (so
      # `#!/usr/bin/env bash` shebangs resolve) and /etc/{passwd,group,
      # nsswitch.conf} (glibc reads nsswitch.conf to resolve names — without it
      # a container cannot look up sibling services on the compose net).
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
      packages.docker-server = pkgs.dockerTools.buildLayeredImage {
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
        # No entrypoint script: stub-server reads STORE / LOG_DIR / LISTEN /
        # CLAUDE_CMD from the environment itself, so the image command is just
        # the binary. CLAUDE_CMD is left unset => the real `claude` bundled
        # above; the mock suite overrides it.
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

      packages.docker-client = pkgs.dockerTools.buildLayeredImage {
        name = "claude-io-stub-client";
        tag = "latest";
        contents = dockerBase ++ [
          stub
          pkgs.opencode
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
    };
}
