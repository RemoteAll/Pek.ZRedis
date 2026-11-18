# TLS 正向往返测试夹具（仅本地测试用）

- `cert.pem` / `key.pem`：自签 RSA2048，SAN `localhost` / `127.0.0.1`，有效期 10 年，**仅供本仓库测试**。
- `tls_redis_server.py`：最小 TLS Redis 服务端（PING/SET/GET/DEL/SELECT/AUTH/QUIT），用于验证 `rediss://` 正向往返。
  启动：`python tests/support/tls/tls_redis_server.py <port> tests/support/tls/cert.pem tests/support/tls/key.pem`
- 客户端连接串：`server=127.0.0.1:<port>;Tls=1;TlsInsecure=1`（自签证书不校链）。

## 当前状态（2026-11-18）

- `TlsInsecure=1` 可以完成 TCP 连接，但客户端首次读应答得到 `error.Protocol`（连接被对端关闭），
  服务端侧未见异常输出 —— **正向往返尚未验证通过，原因未定位**。
- 在定位前，README/移植对照中 TLS 仍标注「正向往返待验证」，不得写成已完成。
