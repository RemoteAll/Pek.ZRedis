# TLS 正向往返测试夹具（仅本地测试用）

- `cert.pem` / `key.pem`：自签 RSA2048，SAN `localhost` / `127.0.0.1`，有效期 10 年，**仅供本仓库测试**。
- `other_cert.pem` / `other_key.pem`：**另一张**自签证书（CN `other-zredis-test`），只用于「证书固定必须拒绝非指定证书」的负向用例；
  两张证书 SHA-1 指纹不同（`EF:B3:…:8B:55` vs `C9:6B:…:96:65`）。
- `tls_redis_server.py`：最小 TLS Redis 服务端（PING/SET/GET/DEL/SELECT/AUTH/QUIT），键空间**进程级共享**
  （跨连接可见，便于 CLI 每次单独建连验证 SET → GET）。
  启动：`python tests/support/tls/tls_redis_server.py <port> tests/support/tls/cert.pem tests/support/tls/key.pem`
- 客户端连接串：`server=127.0.0.1:<port>;Tls=1;TlsInsecure=1`（自签证书不校链）。

## 状态：已通过（2026-11-18 第十二轮）

`tools/regression.ps1` 的 `Invoke-TlsRoundTrip` 阶段会自动起本夹具并跑真实 TLS 往返，实测通过：

```
== TLS PING ==   RESULT=OK
== TLS SET  ==   RESULT=OK
== TLS GET  ==   RESULT=OK
LEN=5
VALUE=world
PASS: TLS 正向 PING / SET / GET（127.0.0.1:<port>）
PASS: TLS 固定证书（Certificate=正确证书）连接成功
PASS: TLS 固定证书拒绝非指定证书（TlsCertificateNotVerified）
PASS: TlsProtocol 单版本明确报错（不静默忽略）
PASS: rediss:// 自动启用 TLS
```

手工复现（两个终端）：

```powershell
python tests\support\tls\tls_redis_server.py 16999 tests\support\tls\cert.pem tests\support\tls\key.pem
.\zig-out\bin\zredis.exe ping "server=127.0.0.1:16999;Tls=1;TlsInsecure=1;timeout=3000"   # RESULT=OK
.\zig-out\bin\zredis.exe set  "server=127.0.0.1:16999;Tls=1;TlsInsecure=1;timeout=3000" hello world
.\zig-out\bin\zredis.exe get  "server=127.0.0.1:16999;Tls=1;TlsInsecure=1;timeout=3000" hello  # VALUE=world
```

## 曾经的失败与根因（别再踩）

第十一轮时表现为 `RESULT=FAIL ... Protocol（连接被对端关闭）`，服务端侧无任何异常输出。用带阶段日志的诊断
服务端取证后发现：**TLS 1.3 握手成功（`TLS_AES_256_GCM_SHA384`），但服务端一个应用层字节都收不到**。

两处问题（缺一不可）：

1. **写方向**：`tls_client.Client.writer.flush()` 只把**密文**写进 `output`（socket 写缓冲），
   不会冲刷 socket 本身（zzig `tls_client.zig` 的 `flush`/`end` 都是如此，只有 `init` 末尾会 `output.flush()`）。
   → 必须在 `writer.flush()` 之后再 `stream_writer.interface.flush()`；`end()` 发 close_notify 后同理。
2. **读方向**：`std.Io.Reader` 允许实现「忽略 `data`、把明文写进自身缓冲并 `return 0`」，
   zzig 的 TLS 客户端正是这种实现（应用数据 `r.end += cleartext.len` 后 `return 0`，
   `NewSessionTicket` 也是 `return 0`）。→ 把「返回 0」当成 EOF 会在握手成功后立刻误判 `ConnectionClosed`。
   正确姿势是 `peekGreedy(1)`（内部走 vtable 填充自身缓冲，直到真有 ≥1 字节明文或真的 close_notify）。

回归护栏：`src/resp.zig` 的单元用例「TLS 明文读取：实现写自身缓冲并返回 0 时不能当成 EOF」
用 mock 复刻了上述实现；**撤掉修复该用例必然失败**（实测 43/44）。
