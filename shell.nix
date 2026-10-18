{ pkgs, zls }:

pkgs.mkShell {
  packages = [
    pkgs.zig_0_17
    zls
    pkgs.just
  ];
}
