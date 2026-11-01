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

bench-compare *args:
    python3 bench/compare/run.py {{args}}

bench-check:
    python3 bench/compare/test.py

bench-coroutines *args:
    python3 bench/compare/coroutines.py {{args}}

bench-coroutines-check:
    python3 bench/compare/test_coroutines.py

bench-balance *args:
    python3 bench/compare/balance.py {{args}}

bench-balance-check:
    python3 bench/compare/test_balance.py
