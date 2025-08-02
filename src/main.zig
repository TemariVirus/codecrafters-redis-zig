const std = @import("std");
const Allocator = std.mem.Allocator;
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
        self: Resp,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.writeByte(@intFromEnum(self));
    }
};

const CommandType = enum {
    ping,

    pub const string_to_type: std.StaticStringMapWithEql(
        CommandType,
        std.ascii.eqlIgnoreCase,
    ) = .initComptime(.{
        .{ "ping", .ping },
    });

    pub fn parse(str: []const u8) ?CommandType {
        return string_to_type.get(str);
    }
};

const Command = struct {
    command: CommandType,
    args: [][]u8,

    pub const ReadError = error{
        ConnectionClosed,
        Invalid,
        OutOfMemory,
        Unsupported,
    };

    pub fn parse(allocator: Allocator, reader: AnyReader) ReadError!Command {
        // On 64-bit systems, the maximum value of u64 is 20 decimal digits long
        var len_buf: [20]u8 = undefined;
        var args: [][]u8 = undefined;

        {
            const len_str = try readPart(reader, &len_buf);
            assert(len_str[0] == @intFromEnum(Resp.array));
            const len = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                log.warn("Failed to parse array length: {s}", .{len_str[1..]});
                return error.Invalid;
            };
            log.debug("Read array length {d}", .{len});

            if (len == 0) {
                return error.Invalid;
            }
            args = try allocator.alloc([]u8, len);
        }

        for (0..args.len) |i| {
            const len_str = try readPart(reader, &len_buf);
            assert(len_str[0] == @intFromEnum(Resp.bulk_string));
            const len = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                log.warn("Failed to parse string length: {s}", .{len_str[1..]});
                return error.Invalid;
            };
            log.debug("Read str length {d}", .{len});

            args[i] = try allocator.alloc(u8, len);
            const data = try readPart(reader, args[i]);
            log.debug("Read str data {s}", .{data});
            assert(data.len == args[i].len);
        }

        const command = CommandType.parse(args[0]) orelse {
            log.info("Unsupported command: {s}", .{args[0]});
            return error.Unsupported;
        };
        std.mem.copyForwards([]u8, args, args[1..]);
        args = try allocator.realloc(args, args.len - 1);
        return .{ .command = command, .args = args };
    }

    /// Reads a part of a RESP message, blocking until the delimiter is found.
    fn readPart(reader: AnyReader, buf: []u8) ReadError![]u8 {
        var fbs = std.io.fixedBufferStream(buf);
        // Would be nice to do this without blocking, but will have to wait for zig 0.15
        reader.streamUntilDelimiter(fbs.writer(), '\r', null) catch |err| switch (err) {
            // Number was too large, or string length given by protocol was not respected
            error.NoSpaceLeft => return error.Invalid,
            else => return error.ConnectionClosed,
        };
        // Skip trailing \n
        reader.skipBytes(1, .{ .buf_size = 1 }) catch return error.ConnectionClosed;
        return fbs.getWritten();
    }

    pub fn deinit(self: Command, allocator: Allocator) void {
        for (self.args) |arg| {
            allocator.free(arg);
        }
        allocator.free(self.args);
    }

    pub fn format(
        self: Command,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("{} [", .{self.command});
        for (self.args, 0..) |arg, i| {
            if (i > 0) {
                try writer.writeByte(' ');
            }
            try writer.writeAll(arg);
        }
        try writer.writeByte(']');
    }
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
        try pool.spawn(workerNoError, .{ allocator, conn });
    }
}

fn workerNoError(allocator: Allocator, conn: net.Server.Connection) void {
    worker(allocator, conn) catch |err| {
        log.warn("Error in worker thread: {}", .{err});
    };
}

fn worker(allocator: Allocator, conn: net.Server.Connection) !void {
    defer log.info("Connection closed", .{});
    defer conn.stream.close();

    log.info("Accepted connection", .{});

    const reader = conn.stream.reader().any();
    const writer = conn.stream.writer().any();

    while (true) {
        const command = Command.parse(allocator, reader) catch |err| switch (err) {
            error.ConnectionClosed => return,
            error.Unsupported => {
                // TODO: change this from a string to an error
                // Left as a string for now for easier local debugging
                try writer.print("{}Unsupported command\r\n", .{Resp.simple_string});
                continue;
            },
            else => {
                try writer.print("{}Unexpected\r\n", .{Resp.simple_error});
                return err;
            },
        };
        log.info("Received: {}\n", .{command});
        log.debug("responding", .{});

        // Hardcoded PONG
        try writer.print("{}PONG\r\n", .{Resp.simple_string});
    }
}
