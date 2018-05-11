# Nix development environment
#
# build:
# export NIX_PATH=BBPpkgs=path/to/bbp-nixpkgs
#
# cd [project] && nix-build ./ -A converter
#

with import <BBPpkgs> { };

{
  converters = converters.overrideDerivation (oldAttr: rec {
    name = "converters-DEV_ENV";
    src = ./../.;
    makeFlags = [ "VERBOSE=1" ];
  });
}
