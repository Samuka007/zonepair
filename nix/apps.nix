# Registry operations as flake apps. CI and a maintainer checkout run the
# identical command: `nix run .#<app> -- <args>` with credentials in the
# environment. Everything the runner used to own — image repo names, tool
# versions (flake.lock), tag conventions — lives here instead of workflow
# scripts.
{
  lib,
  pkgs,
  system,
  ...
}:
let
  # ghcr repo suffix per production image; the only place this mapping
  # exists. images.nix keeps its internal load names; the registry-facing
  # names are zonepair-<name>.
  images = [
    "server"
    "client"
  ];
  imagesStr = lib.concatStringsSep " " images;

  # Systems the release matrix pushes; merge-images references their per-arch
  # tags. Overridable (SYSTEMS env) for local single-arch smoke tests.
  indexSystems = lib.concatStringsSep " " [
    "x86_64-linux"
    "aarch64-linux"
  ];

  pushImage = pkgs.writeShellApplication {
    name = "zonepair-push-image";
    runtimeInputs = [
      pkgs.coreutils
      pkgs.nix
      pkgs.skopeo
    ];
    text = ''
      arch=''${1:-${system}}
      : "''${OWNER:?env OWNER required}"
      : "''${PASSWORD:?env PASSWORD required}"
      registry=''${REGISTRY:-ghcr.io}
      owner=''${OWNER,,}
      # ghcr speaks TLS; anything else (local smoke registry) may not.
      insecure=()
      if [ "$registry" != "ghcr.io" ]; then
        insecure=(--dest-tls-verify=false)
      fi

      tmp=$(mktemp -d)
      trap 'rm -rf "$tmp"' EXIT
      for name in ${imagesStr}; do
        nix build -L ".#docker-$name" --out-link "$tmp/$name"
        skopeo --insecure-policy copy \
          "''${insecure[@]}" \
          --dest-creds "$OWNER:$PASSWORD" \
          "docker-archive:$tmp/$name" \
          "docker://$registry/$owner/zonepair-$name:$arch"
      done
    '';
  };

  mergeImages = pkgs.writeShellApplication {
    name = "zonepair-merge-images";
    runtimeInputs = [
      pkgs.coreutils
      pkgs.regclient
    ];
    text = ''
      version=''${1:?usage: merge-images <version>}
      : "''${OWNER:?env OWNER required}"
      : "''${PASSWORD:?env PASSWORD required}"
      registry=''${REGISTRY:-ghcr.io}
      owner=''${OWNER,,}
      tls=enabled
      if [ "$registry" != "ghcr.io" ]; then
        tls=disabled
      fi
      regctl=(regctl --host "reg=$registry,user=$owner,pass=$PASSWORD,tls=$tls")

      for name in ${imagesStr}; do
        repo="$registry/$owner/zonepair-$name"
        refs=()
        for s in ''${SYSTEMS:-${indexSystems}}; do
          refs+=(--ref "$repo:$s")
        done
        # Per-arch images were built natively, so their config blobs carry
        # the right platform; regctl stamps index descriptors from them.
        "''${regctl[@]}" index create "$repo:$version" "''${refs[@]}"
        # Retag as latest: same-repo copy rewrites the manifest only —
        # blobs are already in the registry and keep their digests.
        "''${regctl[@]}" image copy "$repo:$version" "$repo:latest"
      done
    '';
  };
in
{
  apps.push-image = {
    type = "app";
    program = "${pushImage}/bin/zonepair-push-image";
  };

  apps.merge-images = {
    type = "app";
    program = "${mergeImages}/bin/zonepair-merge-images";
  };
}
