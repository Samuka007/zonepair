{ inputs, ... }:
{
  perSystem =
    {
      lib,
      pkgs,
      system,
      product,
      toolchains,
      ...
    }:
    let
      mingw = pkgs.pkgsCross.mingwW64;

      # crane.lib's overrideToolchain exists precisely for this: crane drives
      # cargo against a mingwW64 stdenv, whose cc wrapper is the cross linker.
      # The host-side fenix toolchain must be built for the build platform,
      # hence toolchainWith on the *host* system.
      craneWinLib = (inputs.crane.mkLib mingw).overrideToolchain (
        _: toolchains.toolchainWith system "x86_64-pc-windows-gnu"
      );
    in
    {
      packages.stub-client-windows = craneWinLib.buildPackage {
        inherit (product)
          pname
          version
          description
          homepage
          ;
        src = craneWinLib.cleanCargoSource ../.;
        strictDeps = true;
        # Cross builds have nothing to run tests on; the e2e harness needs real
        # claude/docker and stays out of the build gate anyway.
        doCheck = false;
        meta = {
          inherit (product) description homepage;
          mainProgram = "stub-client";
          platforms = lib.platforms.windows;
        };
        CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
        CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = "x86_64-w64-mingw32-gcc";
        cargoBuildFlags = [
          "--bin"
          "stub-client"
        ];
        # rustc's windows-gnu link line asks for -l:libpthread.a, which its
        # self-contained dir does not ship; point the linker at mingw-w64's
        # winpthreads.
        NIX_LDFLAGS = "-L${mingw.windows.pthreads}/lib";
      };
    };
}
