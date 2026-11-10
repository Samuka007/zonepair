{ pkgs, system, toolchains, ... }:
{
  devShells.default = pkgs.mkShell {
    packages = [
      (toolchains.hostToolchain system)
      pkgs.opencode
      pkgs.claude-code
      pkgs.rust-analyzer
      pkgs.jq
      pkgs.iproute2
    ];
  };

  devShells.windows = pkgs.mkShell {
    packages = [
      (toolchains.toolchainWith system "x86_64-pc-windows-gnu")
      pkgs.pkgsCross.mingwW64.stdenv.cc
    ];
    # See the NIX_LDFLAGS note in ./windows.nix.
    NIX_LDFLAGS = "-L${pkgs.pkgsCross.mingwW64.windows.pthreads}/lib";
    CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
  };
}
