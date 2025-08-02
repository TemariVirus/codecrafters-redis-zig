const std = @import("std");
const AnyReader = std.io.AnyReader;
const AnyWriter = std.io.AnyWriter;
const assert = std.debug.assert;
const log = std.log;
const net = std.net;

const Resp = enum(u8) {
    simple_string = '+',
    simple_error = '-',
    integer = ':',
    bulk_string = '$',
    array = '*',

    pub fn format(
        self: @This(),
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.writeByte(@intFromEnum(self));
    }
};

const CommandState = enum {
    read_arr_len,
    read_str_len,
    read_str_data,
    respond,
};

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{ .allocator = allocator, .n_jobs = 8 });
    defer pool.deinit();

    while (true) {
        const conn = listener.accept() catch continue;
        try pool.spawn(workerNoError, .{conn});
    }
}

fn workerNoError(conn: net.Server.Connection) void {
    worker(conn) catch |err| {
        log.warn("Error in worker thread: {}", .{err});
    };
}

fn worker(conn: net.Server.Connection) !void {
    defer log.info("Connection closed", .{});
    defer conn.stream.close();

    var command_buffer: [1024]u8 = undefined;
    log.info("Accepted connection", .{});

    const reader = conn.stream.reader().any();
    const writer = conn.stream.writer().any();
    var str_count: usize = undefined;
    _ = state: switch (CommandState.read_arr_len) {
        .read_arr_len => {
            const len_str = readPart(reader, &command_buffer) orelse return;
            assert(len_str[0] == @intFromEnum(Resp.array));
            str_count = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                log.warn("Failed to parse array length: {s}", .{len_str[1..]});
                return;
            };

            log.debug("Read array length {d}", .{str_count});
            continue :state .read_str_len;
        },
        .read_str_len => {
            const len_str = readPart(reader, &command_buffer) orelse return;
            assert(len_str[0] == @intFromEnum(Resp.bulk_string));
            const len = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                log.warn("Failed to parse string length: {s}", .{len_str[1..]});
                return;
            };

            log.debug("Read str length {d}", .{len});
            continue :state .read_str_data;
        },
        .read_str_data => {
            const data = readPart(reader, &command_buffer) orelse return;
            log.debug("Read str data {s}", .{data});
            str_count -= 1;
            if (str_count == 0) {
                continue :state .respond;
            }
            continue :state .read_str_len;
        },
        .respond => {
            log.debug("responding", .{});

            // Hardcoded PONG
            try writer.print("{}PONG\r\n", .{Resp.simple_string});

            continue :state .read_arr_len;
        },
    };
}

/// Reads a part of a RESP message, blocking until a delimiter is found.
/// Returns `null` if the connection is closed or an error occurs.
/// Otherwise, writes the data without the delimiter to `buf`.
fn readPart(reader: AnyReader, buf: []u8) ?[]u8 {
    var fbs = std.io.fixedBufferStream(buf);
    // Would be nice to do this without blocking, but will have to wait for zig 0.15
    reader.streamUntilDelimiter(fbs.writer(), '\r', null) catch return null;
    // Skip trailing \n
    reader.skipBytes(1, .{ .buf_size = 1 }) catch return null;
    return fbs.getWritten();
}
