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
    echo,
    get,
    ping,
    set,

    pub const string_to_type: std.StaticStringMapWithEql(
        CommandType,
        std.ascii.eqlIgnoreCase,
    ) = .initComptime(.{
        .{ "echo", .echo },
        .{ "get", .get },
        .{ "ping", .ping },
        .{ "set", .set },
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
        var args: std.ArrayList([]u8) = blk: {
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
            break :blk try .initCapacity(allocator, len);
        };
        errdefer {
            for (args.items) |arg| {
                allocator.free(arg);
            }
            args.deinit();
        }

        for (0..args.capacity) |i| {
            const len_str = try readPart(reader, &len_buf);
            assert(len_str[0] == @intFromEnum(Resp.bulk_string));
            const len = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                log.warn("Failed to parse string length: {s}", .{len_str[1..]});
                return error.Invalid;
            };
            log.debug("Read str length {d}", .{len});

            args.appendAssumeCapacity(try allocator.alloc(u8, len));
            const data = try readPart(reader, args.items[i]);
            log.debug("Read str data {s}", .{data});
            assert(data.len == args.items[i].len);
        }

        const command = CommandType.parse(args.items[0]) orelse {
            log.info("Unsupported command: {s}", .{args.items[0]});
            return error.Unsupported;
        };
        _ = args.orderedRemove(0);
        return .{ .command = command, .args = try args.toOwnedSlice() };
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

var store: std.StringHashMap(struct {
    value: []const u8,
    /// Unix epoch in milliseconds
    expiry: i64,
}) = undefined;
var store_lock: std.Thread.Mutex = .{};

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    // Don't bother cleaning up the store as it has the same lifetime as the application
    store = .init(allocator);

    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    var pool: std.Thread.Pool = undefined;
    try pool.init(.{
        .allocator = allocator,
        // We need a few threads to pass one of the tests
        .n_jobs = @max(4, std.Thread.getCpuCount() catch 0),
    });
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
                try respond(writer, .simple_string, "Unsupported command");
                continue;
            },
            else => {
                try respond(writer, .simple_error, "Unexpected");
                return err;
            },
        };
        defer command.deinit(allocator);

        log.info("Received: {}\n", .{command});
        handle(writer, command) catch |err| {
            try respond(writer, .simple_error, @errorName(err));
            return err;
        };
    }
}

fn handle(writer: AnyWriter, command: Command) !void {
    switch (command.command) {
        .echo => {
            if (command.args.len < 1) {
                try respond(writer, .simple_error, "ECHO requries 1 argument");
                return;
            }
            try respond(writer, .bulk_string, command.args[0]);
        },
        .get => {
            if (command.args.len < 1) {
                try respond(writer, .simple_error, "GET requries 1 argument");
                return;
            }

            const now = std.time.milliTimestamp();
            const key = command.args[0];

            const res = blk: {
                store_lock.lock();
                defer store_lock.unlock();

                const record = store.get(key) orelse break :blk null;
                if (now > record.expiry) {
                    _ = store.remove(key);
                    break :blk null;
                } else {
                    break :blk record.value;
                }
            };
            try respond(writer, .bulk_string, res);
        },
        .ping => try respond(writer, .simple_string, "PONG"),
        .set => {
            if (command.args.len < 2) {
                try respond(writer, .simple_error, "SET requries at least 2 arguments");
                return;
            }

            const expiry = if (command.args.len > 2 and std.ascii.eqlIgnoreCase("px", command.args[2])) blk: {
                const now = std.time.milliTimestamp();
                if (command.args.len < 4) {
                    try respond(writer, .simple_error, "Missing milliseconds");
                    return;
                }
                const ms = std.fmt.parseInt(i64, command.args[3], 10) catch {
                    try respond(writer, .simple_error, "Invalid value for milliseconds");
                    return;
                };
                break :blk now + ms;
            } else std.math.maxInt(i64);

            {
                store_lock.lock();
                defer store_lock.unlock();

                const gop = try store.getOrPut(command.args[0]);
                errdefer if (!gop.found_existing) {
                    _ = store.remove(command.args[0]);
                };

                var key = gop.key_ptr.*;
                const value = try store.allocator.dupe(u8, command.args[1]);
                errdefer store.allocator.free(value);
                if (gop.found_existing) {
                    // Don't free the key as it can be reused
                    store.allocator.free(gop.value_ptr.*.value);
                } else {
                    key = try store.allocator.dupe(u8, command.args[0]);
                }
                errdefer if (!gop.found_existing) {
                    store.allocator.free(key);
                };

                gop.key_ptr.* = key;
                gop.value_ptr.* = .{
                    .value = value,
                    .expiry = expiry,
                };
            }

            try respond(writer, .simple_string, "OK");
        },
    }
}

fn respond(writer: AnyWriter, comptime kind: Resp, data: anytype) !void {
    switch (kind) {
        .simple_string, .simple_error => try writer.print("{}{s}\r\n", .{ kind, data }),
        .integer => try writer.print("{}{d}\r\n", .{ kind, data }),
        .bulk_string => switch (@typeInfo(@TypeOf(data))) {
            .null, .optional => if (data) |str| {
                try writer.print("{}{d}\r\n{s}\r\n", .{ kind, str.len, str });
            } else {
                try writer.print("{}-1\r\n", .{kind});
            },
            else => try writer.print("{}{d}\r\n{s}\r\n", .{ kind, data.len, data }),
        },
        .array => @panic("TODO"),
    }
}
