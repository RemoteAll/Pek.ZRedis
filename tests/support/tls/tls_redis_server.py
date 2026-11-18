#!/usr/bin/env python3
"""最小 TLS Redis 服务端（仅供 Pek.ZRedis 本地正向测试）。

支持 PING / SET / GET / DEL / SELECT / AUTH / QUIT，其余返回 -ERR。
用法: python tls_redis_server.py <port> <cert.pem> <key.pem>
"""

import socket
import ssl
import sys


def read_command(f):
    line = f.readline()
    if not line:
        return None
    if not line.startswith(b"*"):
        return None
    count = int(line[1:])
    parts = []
    for _ in range(count):
        head = f.readline()
        if not head.startswith(b"$"):
            return None
        length = int(head[1:])
        data = f.read(length)
        f.read(2)  # CRLF
        parts.append(data)
    return parts


def bulk(value):
    if value is None:
        return b"$-1\r\n"
    return b"$%d\r\n%s\r\n" % (len(value), value)


def handle(conn):
    store = {}
    f = conn.makefile("rwb", buffering=0)
    while True:
        parts = read_command(f)
        if not parts:
            break
        cmd = parts[0].upper()
        if cmd == b"PING":
            f.write(b"+PONG\r\n")
        elif cmd == b"QUIT":
            f.write(b"+OK\r\n")
            break
        elif cmd in (b"SELECT", b"AUTH"):
            f.write(b"+OK\r\n")
        elif cmd == b"SET":
            store[parts[1]] = parts[2]
            f.write(b"+OK\r\n")
        elif cmd == b"GET":
            f.write(bulk(store.get(parts[1])))
        elif cmd == b"DEL":
            removed = 0
            for key in parts[1:]:
                if store.pop(key, None) is not None:
                    removed += 1
            f.write(b":%d\r\n" % removed)
        else:
            f.write(b"-ERR unsupported\r\n")


def main():
    port = int(sys.argv[1])
    cert = sys.argv[2]
    key = sys.argv[3]
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen(8)
        print("ready", flush=True)
        while True:
            conn, _ = listener.accept()
            try:
                tls_conn = ctx.wrap_socket(conn, server_side=True)
                handle(tls_conn)
                tls_conn.close()
            except Exception as exc:
                print("handler error:", repr(exc), flush=True)
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()

