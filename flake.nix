{
  description = "claude-io-stub dev shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.fenix.url = "github:nix-community/fenix";

  outputs =
    {
      self,
      nixpkgs,
      fenix,
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
    in
    {
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
