# Nix development environment
#
# build:
# export NIX_PATH=BBPpkgs=path/to/bbp-nixpkgs
#
# cd [project] && nix-build ./ -A converter
#

with import <BBPpkgs> { };

{
  parquet-converters = parquet-converters.overrideDerivation (oldAttr: rec {
    name = "parquet-converters-DEV_ENV";
    src = ./.;
    makeFlags = [ "VERBOSE=1" ];
  });
}
