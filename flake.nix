{
  description = "velo: stackless coroutines and I/O for Zig";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    # nixpkgs has zig_0_17 but no zls_0_17 yet; drop this input once it does.
    zls.url = "github:zigtools/zls";
    zls.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    { nixpkgs, zls, ... }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      default = import ./shell.nix {
        inherit pkgs;
        zls = zls.packages.${system}.zls;
      };
    in
    {
      devShells.${system} = {
        inherit default;
        bench = pkgs.mkShell {
          inputsFrom = [ default ];
          packages = with pkgs; [ cargo rustc rustfmt libuv pkg-config python3 go util-linux ];
        };
      };
    };
}
