const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("velo", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    switch (target.result.os.tag) {
        .macos => mod.link_libc = true,
        .windows => {
            mod.linkSystemLibrary("ws2_32", .{});
            mod.linkSystemLibrary("kernel32", .{});
        },
        else => {},
    }

    const tests = b.addTest(.{ .root_module = mod });

    const check_step = b.step("check", "Compile tests without running them");
    check_step.dependOn(&tests.step);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);

    inline for (.{ "counters", "wakeup", "echo", "files", "pipeline", "flusher", "shards", "mailbox" }) |name| {
        const example = b.createModule(.{
            .root_source_file = b.path("examples/" ++ name ++ ".zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{
                .name = "velo",
                .module = mod,
            }},
        });

        const exe = b.addExecutable(.{ .name = name, .root_module = example });
        b.installArtifact(exe);

        const run = b.addRunArtifact(exe);
        run.addPassthruArgs();
        b.step("run-" ++ name, "Run examples/" ++ name ++ ".zig").dependOn(&run.step);

        const example_tests = b.addTest(.{ .name = name ++ "-test", .root_module = example });
        check_step.dependOn(&exe.step);
        check_step.dependOn(&example_tests.step);
        test_step.dependOn(&b.addRunArtifact(example_tests).step);
    }

    const bench_step = b.step("bench", "Build and install benchmarks");
    inline for (.{ "scalability", "numa", "echo", "crossloop", "coroutines" }) |name| {
        const benchmark_module = b.createModule(.{
            .root_source_file = b.path("bench/" ++ name ++ ".zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "velo", .module = mod }},
        });
        const executable_name = if (comptime std.mem.eql(u8, name, "echo")) "echo-velo" else name;
        const benchmark = b.addExecutable(.{ .name = executable_name, .root_module = benchmark_module });
        const install_benchmark = b.addInstallArtifact(benchmark, .{});
        bench_step.dependOn(&install_benchmark.step);
        if (comptime std.mem.eql(u8, name, "coroutines"))
            b.step("bench-coroutines", "Build the coroutine comparison benchmark").dependOn(&install_benchmark.step);
        const run_benchmark = b.addRunArtifact(benchmark);
        run_benchmark.addPassthruArgs();
        b.step("run-" ++ executable_name, "Run the " ++ name ++ " benchmark").dependOn(&run_benchmark.step);
        const benchmark_tests = b.addTest(.{ .name = executable_name ++ "-test", .root_module = benchmark_module });
        check_step.dependOn(&benchmark.step);
        check_step.dependOn(&benchmark_tests.step);
        test_step.dependOn(&b.addRunArtifact(benchmark_tests).step);
    }
}
