const std = @import("std");
const assert = std.debug.assert;
const AnyReader = std.io.AnyReader;
const AnyWriter = std.io.AnyWriter;
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
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var command_buffer: [1024]u8 = undefined;
    while (true) {
        const conn = try listener.accept();
        defer conn.stream.close();

        std.debug.print("Accepted connection\n", .{});

        const reader = conn.stream.reader().any();
        const writer = conn.stream.writer().any();
        var arg_count: usize = undefined;
        _ = state: switch (CommandState.read_arr_len) {
            .read_arr_len => {
                const len_str = readPart(reader, &command_buffer) orelse continue;
                assert(len_str[0] == @intFromEnum(Resp.array));
                arg_count = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                    std.debug.print("Failed to parse length: {s}\n", .{len_str[1..]});
                    break;
                };

                std.debug.print("Read arr length {d}\n", .{arg_count});
                continue :state .read_str_len;
            },
            .read_str_len => {
                const len_str = readPart(reader, &command_buffer) orelse continue;
                assert(len_str[0] == @intFromEnum(Resp.bulk_string));
                const len = std.fmt.parseInt(usize, len_str[1..], 10) catch {
                    std.debug.print("Failed to parse length: {s}\n", .{len_str[1..]});
                    break;
                };
                std.debug.print("Read str length {d}\n", .{len});
                continue :state .read_str_data;
            },
            .read_str_data => {
                const data = readPart(reader, &command_buffer) orelse continue;
                std.debug.print("Read str data {s}\n", .{data});
                arg_count -= 1;
                if (arg_count == 0) {
                    continue :state .respond;
                }
                continue :state .read_str_len;
            },
            .respond => {
                std.debug.print("responding\n", .{});

                // Hardcoded PONG
                try writer.print("{}PONG\r\n", .{Resp.simple_string});

                continue :state .read_arr_len;
            },
        };

        std.debug.print("Connection closed\n", .{});
    }
}

fn readPart(reader: AnyReader, buf: []u8) ?[]u8 {
    var fbs = std.io.fixedBufferStream(buf);
    while (true) {
        reader.streamUntilDelimiter(fbs.writer(), '\r', null) catch |err| switch (err) {
            // Connection closed
            error.ConnectionResetByPeer, error.ConnectionTimedOut => return null,
            // Wait for data
            error.EndOfStream => continue,
            else => {
                const src = @src();
                std.debug.print(
                    "{} in {s}() in {s}:{d}\n",
                    .{ err, src.fn_name, src.file, src.line },
                );
                return null;
            },
        };
        break;
    }

    // Skip trailing \n
    while (true) {
        reader.skipBytes(1, .{ .buf_size = 1 }) catch |err| switch (err) {
            // Connection closed
            error.ConnectionResetByPeer, error.ConnectionTimedOut => return null,
            // Wait for data
            error.EndOfStream => continue,
            else => {
                std.debug.print("{} in {s}() in {s}:{d}\n", .{
                    err,
                    @src().fn_name,
                    @src().file,
                    @src().line,
                });
                return null;
            },
        };
        break;
    }

    return fbs.getWritten();
}
