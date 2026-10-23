const std = @import("std");

const io = @import("../event/root.zig");
const w = @import("../event/backends/win32.zig");

pub fn upgrade(
    threaded: std.Io,
    file: std.Io.File,
    read: bool,
    write: bool,
    direct: bool,
) !std.Io.File {
    const file_type = w.GetFileType(file.handle);
    if (file_type != 1) {
        if (direct and file_type == 0) {
            const code = w.GetLastError();
            if (code != 0) return mapError(.sync, code);
        }
        if (direct) return error.UnsupportedOperation;
        return file;
    }

    const stat = file.stat(threaded) catch |err| {
        if (direct) return err;
        return file;
    };
    if (stat.kind != .file) {
        if (direct) return error.UnsupportedOperation;
        return file;
    }

    // Reopen by identity; only the optional asynchronous upgrade may fall back.
    const handle = w.ReOpenFile(
        file.handle,
        (if (read) @as(u32, 0x80000000) else 0) | (if (write) @as(u32, 0x40000000) else 0),
        1 | 2 | 4,
        w.FILE_FLAG_OVERLAPPED | (if (direct) w.FILE_FLAG_NO_BUFFERING else @as(u32, 0)),
    );
    if (@intFromPtr(handle) == std.math.maxInt(usize)) {
        if (direct) return mapError(.sync, w.GetLastError());
        return file;
    }

    file.close(threaded);

    return .{
        .handle = handle,
        .flags = .{ .nonblocking = true },
    };
}

pub fn transfer(
    comptime kind: io.Kind,
    file: std.Io.File,
    buffer: if (kind == .read_at) []u8 else []const u8,
    offset: u64,
) anyerror!usize {
    if (buffer.len == 0) return 0;

    const event = w.CreateEventW(null, 1, 0, null) orelse return mapError(kind, w.GetLastError());
    defer _ = w.CloseHandle(event);

    var overlapped: w.OVERLAPPED = .{
        .offset = @truncate(offset),
        .offset_high = @truncate(offset >> 32),
        // Workers cannot use Threaded's APCs on IOCP handles or send stack storage to the port.
        .event = @ptrFromInt(@intFromPtr(event) | 1),
    };
    const len: u32 = @intCast(@min(buffer.len, std.math.maxInt(u32)));

    const success = if (kind == .read_at)
        w.ReadFile(file.handle, buffer.ptr, len, null, &overlapped)
    else
        w.WriteFile(file.handle, buffer.ptr, len, null, &overlapped);
    if (success == 0) {
        const code = w.GetLastError();
        if (kind == .read_at and code == w.HANDLE_EOF) return 0;
        if (code != w.IO_PENDING) return mapError(kind, code);
    }

    var bytes: u32 = undefined;
    if (w.GetOverlappedResult(file.handle, &overlapped, &bytes, 1) == 0) {
        const code = w.GetLastError();
        if (kind == .read_at and code == w.HANDLE_EOF) return 0;
        return mapError(kind, code);
    }

    return bytes;
}

pub fn sync(file: std.Io.File) anyerror!void {
    // This synchronous API accepts asynchronous handles without posting an OVERLAPPED.
    if (w.FlushFileBuffers(file.handle) == 0) return mapError(.sync, w.GetLastError());
}

pub fn allocate(file: std.Io.File, end: u64) !void {
    var info: w.FILE_STANDARD_INFO = undefined;
    if (w.GetFileInformationByHandleEx(
        file.handle,
        w.FileStandardInfo,
        &info,
        @sizeOf(w.FILE_STANDARD_INFO),
    ) == 0)
        return mapError(.sync, w.GetLastError());

    if (info.Directory.toBool()) return error.UnsupportedOperation;
    if (info.AllocationSize < 0 or info.EndOfFile < 0) return error.InvalidArgument;

    var attributes: w.FILE_ATTRIBUTE_TAG_INFO = undefined;
    if (w.GetFileInformationByHandleEx(
        file.handle,
        w.FileAttributeTagInfo,
        &attributes,
        @sizeOf(w.FILE_ATTRIBUTE_TAG_INFO),
    ) == 0)
        return mapError(.sync, w.GetLastError());

    const flags: std.os.windows.FILE.ATTRIBUTE = @bitCast(attributes.FileAttributes);
    if (flags.SPARSE_FILE or flags.COMPRESSED) return error.UnsupportedOperation;

    const eof = @max(info.EndOfFile, @as(i64, @intCast(end)));
    const allocation = @max(info.AllocationSize, eof);

    // Win32's synchronous wrapper waits even on overlapped handles; stack IOSBs must not escape.
    if (allocation > info.AllocationSize and
        w.SetFileInformationByHandle(
            file.handle,
            w.FileAllocationInfo,
            &allocation,
            @sizeOf(i64),
        ) == 0)
        return mapError(.sync, w.GetLastError());

    if (eof > info.EndOfFile and
        w.SetFileInformationByHandle(
            file.handle,
            w.FileEndOfFileInfo,
            &eof,
            @sizeOf(i64),
        ) == 0)
        return mapError(.sync, w.GetLastError());
}

pub fn mapError(comptime kind: io.Kind, code: u32) anyerror {
    const err: std.os.windows.Win32Error = @fromBackingInt(code);
    return switch (err) {
        .OPERATION_ABORTED => error.OperationCanceled,
        .ACCESS_DENIED => switch (kind) {
            .read_at => error.NotOpenForReading,
            .write_at => error.NotOpenForWriting,
            else => error.AccessDenied,
        },
        .INVALID_HANDLE => error.BadFileDescriptor,
        .FILE_NOT_FOUND, .PATH_NOT_FOUND => error.FileNotFound,
        .INVALID_PARAMETER => error.InvalidArgument,
        .NOT_ENOUGH_MEMORY, .OUTOFMEMORY, .INVALID_USER_BUFFER, .NOT_ENOUGH_QUOTA => error.SystemResources,
        .DISK_FULL, .HANDLE_DISK_FULL => error.NoSpaceLeft,
        .DISK_QUOTA_EXCEEDED => error.DiskQuota,
        .FILE_TOO_LARGE => error.FileTooBig,
        .WRITE_PROTECT => error.AccessDenied,
        .LOCK_VIOLATION, .SHARING_VIOLATION, .DELETE_PENDING => error.FileBusy,
        .SEEK, .SEEK_ON_DEVICE => error.Unseekable,
        .NOT_SUPPORTED, .INVALID_FUNCTION => error.UnsupportedOperation,
        .BROKEN_PIPE, .NO_DATA => error.BrokenPipe,
        .CRC, .READ_FAULT, .WRITE_FAULT, .GEN_FAILURE, .IO_DEVICE, .UNEXP_NET_ERR => error.InputOutput,
        .SEM_TIMEOUT, .TIMEOUT => error.Timeout,
        else => error.Unexpected,
    };
}
