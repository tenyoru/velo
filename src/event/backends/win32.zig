const std = @import("std");

pub const HANDLE = *anyopaque;
pub const INVALID_SOCKET = std.math.maxInt(usize);
pub const WSA_FLAG_OVERLAPPED = 1;
pub const WSA_FLAG_NO_HANDLE_INHERIT = 0x80;
pub const IO_PENDING = 997;
pub const OPERATION_ABORTED = 995;
pub const WAIT_TIMEOUT = 258;
pub const NOT_FOUND = 1168;
pub const INVALID_PARAMETER = 87;
pub const HANDLE_EOF = 38;
pub const FILE_FLAG_OVERLAPPED = 0x40000000;
pub const FILE_FLAG_NO_BUFFERING = 0x20000000;
pub const FileStandardInfo = 1;
pub const FileAllocationInfo = 5;
pub const FileEndOfFileInfo = 6;
pub const FileAttributeTagInfo = 9;
pub const FileStorageInfo = 16;

pub const FILE_STANDARD_INFO = std.os.windows.FILE.STANDARD_INFORMATION;
pub const FILE_ATTRIBUTE_TAG_INFO = std.os.windows.FILE.ATTRIBUTE_TAG_INFO;

pub const OVERLAPPED = extern struct {
    internal: usize = 0,
    internal_high: usize = 0,
    offset: u32 = 0,
    offset_high: u32 = 0,
    event: ?HANDLE = null,
};

pub const WSABUF = extern struct { len: u32, buf: [*]u8 };

pub const WSAData = extern struct {
    version: u16,
    high_version: u16,
    max_sockets: u16,
    max_udp: u16,
    vendor: ?[*:0]u8,
    description: [257]u8,
    status: [129]u8,
};

comptime {
    // WinSock2.h changes WSADATA field order on WIN64; only 64-bit Windows is supported.
    std.debug.assert(@sizeOf(usize) == 8);
    std.debug.assert(@sizeOf(WSAData) == 408 and @offsetOf(WSAData, "description") == 16);
    std.debug.assert(@sizeOf(WSABUF) == 16 and @offsetOf(WSABUF, "buf") == 8);
    std.debug.assert(@sizeOf(OVERLAPPED) == 32 and @offsetOf(OVERLAPPED, "event") == 24);
}

const SockAddr = std.os.windows.ws2_32.sockaddr;
const Completion = ?*const fn (u32, u32, *OVERLAPPED, u32) callconv(.winapi) void;

pub const ConnectEx = *const fn (
    usize,
    *const SockAddr,
    i32,
    ?*anyopaque,
    u32,
    ?*u32,
    *OVERLAPPED,
) callconv(.winapi) i32;

pub const AcceptEx = *const fn (
    usize,
    usize,
    *anyopaque,
    u32,
    u32,
    u32,
    *u32,
    *OVERLAPPED,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSAStartup(u16, *WSAData) callconv(.winapi) i32;

pub extern "ws2_32" fn WSACleanup() callconv(.winapi) i32;

pub extern "ws2_32" fn WSAGetLastError() callconv(.winapi) i32;

pub extern "ws2_32" fn socket(i32, i32, i32) callconv(.winapi) usize;

pub extern "ws2_32" fn WSASocketW(
    i32,
    i32,
    i32,
    ?*anyopaque,
    u32,
    u32,
) callconv(.winapi) usize;

pub extern "ws2_32" fn bind(usize, *const SockAddr, i32) callconv(.winapi) i32;

pub extern "ws2_32" fn listen(usize, i32) callconv(.winapi) i32;

pub extern "ws2_32" fn accept(usize, ?*SockAddr, ?*i32) callconv(.winapi) usize;

pub extern "ws2_32" fn getsockname(usize, *SockAddr, *i32) callconv(.winapi) i32;

pub extern "ws2_32" fn shutdown(usize, i32) callconv(.winapi) i32;

pub extern "ws2_32" fn closesocket(usize) callconv(.winapi) i32;

pub extern "ws2_32" fn setsockopt(usize, i32, i32, ?[*]const u8, i32) callconv(.winapi) i32;

pub extern "ws2_32" fn WSAIoctl(
    usize,
    u32,
    ?*const anyopaque,
    u32,
    ?*anyopaque,
    u32,
    *u32,
    ?*OVERLAPPED,
    Completion,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSASend(
    usize,
    [*]WSABUF,
    u32,
    ?*u32,
    u32,
    *OVERLAPPED,
    Completion,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSARecv(
    usize,
    [*]WSABUF,
    u32,
    ?*u32,
    *u32,
    *OVERLAPPED,
    Completion,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSASendTo(
    usize,
    [*]WSABUF,
    u32,
    ?*u32,
    u32,
    ?*const SockAddr,
    i32,
    *OVERLAPPED,
    Completion,
) callconv(.winapi) i32;

pub extern "ws2_32" fn WSARecvFrom(
    usize,
    [*]WSABUF,
    u32,
    ?*u32,
    *u32,
    ?*SockAddr,
    ?*i32,
    *OVERLAPPED,
    Completion,
) callconv(.winapi) i32;

pub extern "kernel32" fn CreateIoCompletionPort(
    ?HANDLE,
    ?HANDLE,
    usize,
    u32,
) callconv(.winapi) ?HANDLE;

pub extern "kernel32" fn GetQueuedCompletionStatus(
    HANDLE,
    *u32,
    *usize,
    *?*OVERLAPPED,
    u32,
) callconv(.winapi) i32;

pub extern "kernel32" fn PostQueuedCompletionStatus(
    HANDLE,
    u32,
    usize,
    ?*OVERLAPPED,
) callconv(.winapi) i32;

pub extern "kernel32" fn CancelIoEx(?HANDLE, ?*OVERLAPPED) callconv(.winapi) i32;

pub extern "kernel32" fn ReOpenFile(HANDLE, u32, u32, u32) callconv(.winapi) HANDLE;

pub extern "kernel32" fn GetFileType(HANDLE) callconv(.winapi) u32;

pub extern "kernel32" fn GetFileInformationByHandleEx(HANDLE, u32, *anyopaque, u32) callconv(.winapi) i32;

pub extern "kernel32" fn SetFileInformationByHandle(HANDLE, u32, *const anyopaque, u32) callconv(.winapi) i32;

pub extern "kernel32" fn DeviceIoControl(HANDLE, u32, ?*const anyopaque, u32, ?*anyopaque, u32, ?*u32, ?*OVERLAPPED) callconv(.winapi) i32;

pub extern "kernel32" fn ReadFile(HANDLE, [*]u8, u32, ?*u32, *OVERLAPPED) callconv(.winapi) i32;

pub extern "kernel32" fn WriteFile(HANDLE, [*]const u8, u32, ?*u32, *OVERLAPPED) callconv(.winapi) i32;

pub extern "kernel32" fn CreateEventW(?*anyopaque, i32, i32, ?[*:0]const u16) callconv(.winapi) ?HANDLE;

pub extern "kernel32" fn GetOverlappedResult(HANDLE, *OVERLAPPED, *u32, i32) callconv(.winapi) i32;

pub extern "kernel32" fn FlushFileBuffers(HANDLE) callconv(.winapi) i32;

pub extern "kernel32" fn CloseHandle(HANDLE) callconv(.winapi) i32;

pub extern "kernel32" fn GetLastError() callconv(.winapi) u32;

pub extern "kernel32" fn GetProcessAffinityMask(HANDLE, *usize, *usize) callconv(.winapi) i32;

pub extern "kernel32" fn SetThreadAffinityMask(HANDLE, usize) callconv(.winapi) usize;

pub extern "kernel32" fn QueryPerformanceCounter(*i64) callconv(.winapi) i32;

pub extern "kernel32" fn QueryPerformanceFrequency(*i64) callconv(.winapi) i32;
