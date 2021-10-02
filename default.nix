# -*- compile-command: "nix-shell --run 'cabal exec -- ghc-pkg list'"; -*-
{ pkgs ? import <nixpkgs> {}, sources ? import ./nix/sources.nix {} }:
  # https://github.com/NixOS/nixpkgs/blob/master/pkgs/development/haskell-modules/make-package-set.nix
let mach-nix = import (builtins.fetchGit {
      url = "https://github.com/DavHau/mach-nix";
      # ref = "refs/tags/3.5.0";
    }) { inherit pkgs; python="python310"; };
    pyEnv = mach-nix.mkPython {
      _.tomli.buildInputs.add = [pkgs.python310Packages.flit-core];
      # _.jedi.buildInputs.add = [pkgs.python310Packages.platformdirs];
      _.black.buildInputs.add = [pkgs.python310Packages.platformdirs];
      # _.black.propagatedBuildInputs.mod = pySelf: _: old:
        # old ++ [pkgs.python310Packages.platformdirs];
      providers =  {default = "nixpkgs,conda,wheel,sdist";};
      requirements = ''
        pandas
        cbor2
        ipython
        platformdirs
      '';
      # probagatedBuildInputs = [pkgs.pkg-config];
    };
    this = pkgs.haskellPackages.developPackage {
      root = ./.;
      withHoogle = false;
      returnShellEnv = false;
      overrides = self: _:  {
        yahp = self.callCabal2nix "yahp" sources.yahp {};
      };
      source-overrides = {
        vector-algorithms = "0.9.0.1";
      };
      modifier = with pkgs.haskell.lib; drv:
        disableLibraryProfiling (dontHaddock (addBuildTools drv
          (with pkgs.haskellPackages; [ cabal-install ghcid pyEnv])));
    };
in this
   // { env = this.env.overrideAttrs(_: prev: { shellHook = prev.shellHook + ''
   export PYTHON_BIN="${pyEnv}/bin/python"

   ''; });}
