default:
    @just --list

build:
    zig build

check *args:
    zig build check {{args}}

test:
    zig build test --summary all

fmt:
    zig fmt build.zig build.zig.zon src examples bench
