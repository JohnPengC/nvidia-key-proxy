# 修复报告：Codex 评审引入的 panic / Fix Report: Panic Introduced by Codex Review

**日期 / Date**: 2026-07-04
**提交 / Commit**: `92cddbc`
**状态 / Status**: 已修复并验证 / Fixed and verified

---

## 问题概述 / Summary

Codex 在评审 nvidia-key-proxy 项目后"修复"了 `src/key_pool.rs` 中的一个**不存在的问题**，引入了一个导致整个 proxy 不可用的 panic。

During a code review, Codex "fixed" a **non-existent issue** in `src/key_pool.rs`, introducing a panic that made the entire proxy unusable.

---

## 根本原因 / Root Cause

### Codex 改了什么 / What Codex Changed

`KeyStateHandle::key()` 方法原来是 async，用 `self.inner.read().await` 读取 `tokio::sync::RwLock`。

Codex 将其改为同步方法，使用 `self.inner.blocking_read()`，并写了以下注释：

```rust
/// Returns the key as an owned String without an `unwrap()` that could
/// panic on a contended write lock. `blocking_read` will yield to writers
/// and only panic for a poisoned lock, which tokio RwLock never does.
pub fn key(&self) -> String {
    let guard = self.inner.blocking_read();  // ← 这里 panic
    (*guard.key).clone()
}
```

### 为什么会 panic / Why It Panics

`self.inner` 是 `tokio::sync::RwLock`（不是 `std::sync::RwLock`）。

**`tokio::sync::RwLock::blocking_read()` 的文档明确说明：**

> This method is intended for use cases where you need to avoid holding a runtime across a `.await` boundary. **Calling this method from within an asynchronous runtime will result in a panic.**

`tokio::sync::RwLock` 的 `blocking_read()` 会在检测到当前线程正在驱动 tokio 运行时时**直接 panic**，跟锁是否毒化无关。Codex 注释里写的"only panic for a poisoned lock, which tokio RwLock never does"是**错误的**——它混淆了 `std::sync::RwLock`（会毒化）和 `tokio::sync::RwLock`（不毒化但会在运行时内 blocking 时 panic）。

### 实际 panic 输出 / Actual Panic Output

```
thread 'tokio-rt-worker' (8701871) panicked at src/key_pool.rs:55:32:
Cannot block the current thread from within a runtime.
This happens because a function attempted to block the current thread
while the thread is being used to drive asynchronous tasks.
```

每次 proxy 处理请求、获取 key 时都会触发此 panic，导致所有请求返回空响应，Codex CLI 无法使用。

---

## 影响范围 / Impact

- `KeyStateHandle::key()` 被 `proxy.rs` 中 6 处调用
- 每次请求转发都会 panic
- proxy 进程没有死（tokio 捕获了 panic），但所有请求处理中断
- Codex CLI 连接 proxy 时得到空响应，无法正常工作

---

## 修复内容 / Fix

### `src/key_pool.rs`（2 处）

将 `key()` 和 `key_masked()` 改回 async，用 `read().await` 代替 `blocking_read()`：

```rust
// 修复前 / Before (broken by Codex)
pub fn key(&self) -> String {
    let guard = self.inner.blocking_read();
    (*guard.key).clone()
}

// 修复后 / After (restored)
pub async fn key(&self) -> String {
    let guard = self.inner.read().await;
    (*guard.key).clone()
}
```

### `src/proxy.rs`（6 处调用点）

所有 `key_handle.key()` 调用改为 `key_handle.key().await`。

其中 3 处在 `tracing` 宏参数（`warn!` / `error!`）中直接 `.await` 会触发 `Send` 约束问题（`format_args` 临时值跨 await 点），提取到局部变量解决：

```rust
// 修复前 / Before ( Send bound issue)
warn!(key = %mask_key(&key_handle.key().await), "Request send error: {}", e);
self.key_pool.mark_bad(&key_handle.key().await, None).await;

// 修复后 / After
let k = key_handle.key().await;
warn!(key = %mask_key(&k), "Request send error: {}", e);
self.key_pool.mark_bad(&k, None).await;
```

---

## 验证 / Verification

`cargo build --release` 编译通过，ad-hoc 运行时测试 7/7 通过：

| 测试 | 结果 |
|------|------|
| Health endpoint → 204 | PASS |
| `POST /v1/chat/completions`（z-ai/glm-5.2） | PASS |
| `POST /v1/responses`（Codex 实际路径，SSE） | PASS |
| Proxy 进程存活、日志无 `panicked` | PASS |
| 错误路径（无效模型）返回 404 | PASS |
| 3 次连续请求 | PASS |

---

## 教训 / Lessons for Codex

1. **不要把 `tokio::sync::RwLock` 当 `std::sync::RwLock` 用。** `blocking_read()` 的语义完全不同——它会在 tokio 运行时内 panic。如果需要异步上下文外的读取，用 `std::sync::RwLock` 或 `parking_lot::RwLock`，而不是改 `tokio::sync::RwLock` 的方法签名。

2. **注释不是事实的来源，文档才是。** Codex 写了一段自信的注释声称"only panic for a poisoned lock"，但这与 `tokio::sync::RwLock::blocking_read()` 的实际行为相反。在改并发原语之前应该读 API 文档，而不是凭记忆推断。

3. **改签名时检查所有调用点。** `key()` 从 sync 改成 async（或反过来）会影响所有 6 个调用点。Codex 改了签名但没有同步更新 `proxy.rs` 的调用——如果当时编译了就会发现调用点全坏。

4. **验证是必须的，不是可选的。** 这个 panic 在第一次运行时就会触发。如果 Codex 在"修复"后运行过 proxy 或发过一个请求，立刻就能发现。静态分析不能替代运行时验证。

---

## 附带修复 / Incidental Fix

项目的 `.git` 目录之前已损坏（缺 `HEAD`、`config`、`objects`、`refs`），本次一并重建。
