{ fenix }:
{
  # fenix ships prebuilt Rust toolchains, so no compiler bootstrap happens per
  # build; flake.lock pins the exact rustc revision. `combine` merges a
  # toolchain with extra rust-std components into one derivation whose bin/
  # holds both cargo and rustc, which is what makeRustPlatform wants for both
  # arguments. The extra rust-std is what lets the same rustc emit for musl /
  # windows-gnu.
  hostToolchain = system: fenix.packages.${system}.stable.toolchain;

  toolchainWith =
    system: target:
    (fenix.packages.${system}).combine [
      (fenix.packages.${system}).stable.toolchain
      (fenix.packages.${system}).targets.${target}.stable.rust-std
    ];

  rustPlatformFrom =
    pkgs: toolchain:
    pkgs.makeRustPlatform {
      cargo = toolchain;
      rustc = toolchain;
    };
}
