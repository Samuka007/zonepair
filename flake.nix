{
  description = "claude-io-stub: dev shells (cargo-in-shell) and reproducible package builds (crane)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix.url = "github:nix-community/fenix";
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
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # Official prebuilt rust-std for windows-gnu + the mingw-w64 linker;
      # cargo build --target x86_64-pc-windows-gnu produces stub-client.exe.
      windowsToolchain = fenix.packages.${system}.combine [
        fenix.packages.${system}.stable.toolchain
        fenix.packages.${system}.targets.x86_64-pc-windows-gnu.stable.rust-std
      ];

      # Cross pkgs for the mingw target: its stdenv cc wrapper IS the cross
      # linker, same one the `windows` dev shell uses.
      pkgsWin = pkgs.pkgsCross.mingwW64;

      craneLib = crane.mkLib pkgs;
      craneWinLib = (crane.mkLib pkgsWin).overrideToolchain windowsToolchain;

      commonArgs = {
        src = craneLib.cleanCargoSource self;
        strictDeps = true;
        # Integration tests drive the e2e harness (scripts/, claude binary);
        # they are not unit tests and must not gate the package build.
        doCheck = false;
      };
    in
    {
      packages.${system} = {
        # Both linux binaries: stub-server, stub-client.
        default = craneLib.buildPackage commonArgs;

        # Windows-gnu cross build (stub-client.exe et al). Same linker flags
        # as the `windows` dev shell.
        windows-exe = craneWinLib.buildPackage (
          commonArgs
          // {
            CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
            CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
            # rustc's windows-gnu link line asks for -l:libpthread.a; the fenix
            # rust-std self-contained dir only ships crt objects, so point the
            # mingw linker at winpthreads (string ref: not a packages entry,
            # whose meta platform check would reject a windows-only package).
            NIX_LDFLAGS = "-L${pkgsWin.windows.pthreads}/lib";
          }
        );
      };

      devShells.${system} = {
        default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            python3
            nodejs
            jq
            iproute2
          ];
        };

        windows = pkgs.mkShell {
          packages = [
            windowsToolchain
            pkgs.pkgsCross.mingwW64.stdenv.cc
          ];
          # rustc's windows-gnu link line asks for -l:libpthread.a; the fenix
          # rust-std self-contained dir only ships crt objects, so point the
          # mingw linker at winpthreads (string ref: not a packages entry,
          # whose meta platform check would reject a windows-only package).
          NIX_LDFLAGS = "-L${pkgs.pkgsCross.mingwW64.windows.pthreads}/lib";
          CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
        };
      };
    };
}
