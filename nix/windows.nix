{ lib, pkgs, system, product, toolchains, crane, ... }:
let
  mingw = pkgs.pkgsCross.mingwW64;

  # crane.lib's overrideToolchain exists precisely for this: crane drives cargo
  # against a mingwW64 stdenv, whose cc wrapper is the cross linker. The
  # host-side fenix toolchain must be built for the build platform, hence
  # toolchainWith on the *host* system.
  craneWinLib = (crane.mkLib mingw).overrideToolchain (
    _: toolchains.toolchainWith system "x86_64-pc-windows-gnu"
  );
in
{
  # linux hosts only. On darwin crane's cross-toolchain env asks nixpkgs
  # whether a wine emulator exists for windows binaries
  # (`hostPlatform.emulatorAvailable`), and that probe *evaluates* wine64,
  # which refuses to evaluate on aarch64-darwin (unsupported platform) and
  # takes the whole flake eval down with it. So the derivation is only
  # *evaluated* on linux; on darwin the package exists (flake-parts requires
  # every `packages.<name>` to exist on every system) but refuses with
  # meta.platforms, keeping the refusal local instead of poisoning eval.
  # The darwin leg is lazy: craneWinLib is only forced when actually building.
  packages.stub-client-windows =
    if pkgs.stdenv.hostPlatform.isLinux then
      craneWinLib.buildPackage {
        inherit (product) pname version description homepage;
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
      }
    else
      pkgs.runCommand "stub-client-windows-unsupported"
        {
          meta = {
            description = "placeholder: windows cross build requires a linux host (wine eval limitation)";
            platforms = [ ];
          };
        }
        ''
          echo "stub-client-windows must be built on a linux host" > $out
          echo "see nix/windows.nix for why darwin eval is unsupported" >> $out
        '';
}
