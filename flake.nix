{
  description = "zonepair: drive a real `claude` on machine B from an agent harness on machine A — stdio relay + session-store projection, in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      perSystem =
        { system, lib, ... }:
        {
          # claude-code carries an unfree license; permit just that package so
          # the server image can bundle it without a blanket allowUnfree.
          # (Documented override point for flake-parts' default pkgs.)
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            config.allowUnfreePredicate = pkg: builtins.elem (lib.getName pkg) [ "claude-code" ];
          };
          # Shared, system-scoped values, injected into every perSystem module
          # as module arguments.
          _module.args.product = import ./nix/meta.nix;
          _module.args.toolchains = import ./nix/toolchains.nix { fenix = inputs.fenix; };
          _module.args.crane = inputs.crane;

          imports = [
            ./nix/rust.nix
            ./nix/windows.nix
            ./nix/images.nix
            ./nix/apps.nix
            ./nix/devshells.nix
            ./nix/checks.nix
            ./nix/formatter.nix
          ];
        };
    };
}
