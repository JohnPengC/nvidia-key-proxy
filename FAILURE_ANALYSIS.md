# failure.log 分析报告（2026-07-24 → 2026-07-31）

428 条错误记录，时间跨度约 7 天，涉及 22 个 NVIDIA API Key、324 个独立 client 请求。
模型分布：`z-ai/glm-5.2` 403 / `openai/gpt-oss-120b` 10 / 空 model 15。

## 一、错误分类总览

| # | 类别 | 数量 | 占比 | HTTP |
|---|---|---|---|---|
| 1 | `empty_choices_shell_first` | 265 | 62% | 200 |
| 2 | `upstream_5xx` | 74 | 17% | 504 / 502 |
| 3 | `empty_choices_shell_retry` | 49 | 11% | 200 |
| 4 | `upstream_4xx` | 21 | 5% | 400 / 404 |
| 5 | `network_error` | 19 | 4% | 无 |

## 二、各类根因

### 1. 空壳响应（empty shell） —— 314 条，主因

**现象**：上游 HTTP 200 + `{"choices":[],"created":0,"id":"","model":"","usage":null}`，没有跑推理。
**量化**（首次 attempt=0 的 216 条唯一 req，**未含重试**）：
- `est_prompt_tokens` 下界 **107,485**，中位 119k，最大 138k。
- 分桶：≤108k → 0% 空壳；≤110k → 0.5%；**≤112k 仍有 13%**；中位 119k 之后是密集区；≤138k 覆盖 96%。
- `messages` 数最低 345，中位 445，最大 553。
- **一天内分布**：07-24 占 104 条，07-25 占 112 条；07-26 起直至 07-31 **0 条空壳**。
- 22 个 key 全都中过招，不是单 key 问题。

**重试完全无效**：49 次 clamp 重试（max_tokens→8192）**全部** 再次空壳并 `retry_exhausted`，恢复率 **0%**。证明根因是 **prompt 太长**而非 max_tokens 过大——clamp 救不了。`$REPO/src/proxy.rs` 现有 clamp 策略对这一类失效。

### 2. 上游 5xx —— 74 条
- 73 条 504（67 glm-5.2 + 6 gpt-oss-120b）+ 1 条 502。
- 全是 `stream=true` 长流式请求超时。集中在 07-29、07-30、07-31（即空壳消失后取而代之的失败模式）。

### 3. 上游 4xx —— 21 条
- **18 条 404** `page not found`：路径 `/v1/props`（15 条 GET）+ 3 条 `/v1/chat/completions` POST。
  - `/v1/props` 不在仓库任何代码里，是 **client 端 SDK 在轮询某个非标准探测端点**；代理透传给上游、上游返 404。无谓消耗 key 占用，非 critical。
  - 3 条 404 chat completions 无 model 字段——需另查。
- **3 条 400** `max_tokens must be at least 1, got -27827`：全是 `openai/gpt-oss-120b`，07-25 11:42–11:43，`stream=true`，messages=306。
  - 报错来自 **NVIDIA 上游**（"max_tokens must be at least 1, got -27827" 是 NVIDIA 的字面错误）。代理侧负值 drop 逻辑(`82e35cd`，07-23 提交)早于这批错误，且 07-26 起再无负值 400——确认代理侧修复已生效，上游这套内部预算计算与代理无关。
  - 但根因仍是上下文超长：输入 30 万 token（messages 306、对应大 prompt）让 NVIDIA 内部 `max_tokens = 模型上限 − 输入` 溢出为负。

### 4. 网络错误 —— 19 条
- 14 条 `unexpected end of file` 连接中途断开 + 5 条 `operation timed out` 连接超时。集中 07-26/07-31，发生在流式请求中。属上游/网络层抖动。

## 三、根因归类（按优先级）

1. ⚠️ **上下文超长 → 空壳**（73%，07-24/25 集中爆发）——业务真实问题。
   - 代理 clamp-重试 0% 恢复，**不应重试**，应在上游前主动 compact，避免到 107k+ 还在请求 glm-5.2。
2. **上游流式 504 + 网络抖动**（共 93 条）——基础设施层，已有重试覆盖。
3. **client 轮询 `/v1/props` → 404**（15 条）——可在代理层拒绝未知 GET endpoint，省 key 占用。
4. 负值 max_tokens——代理侧已修，已无新增。

## 四、阈值依据（供自动 compact）

由于 failure.log 只有失败记录、无成功样本，无法标定"shell 触发上界"，只能给出 **下界**：
- **est_prompt_tokens > 107,000 一定触发 glm-5.2 空壳**（数据全在这之上）。
- 真实临界点必然 < 107k；保守的自适应 compact 阈值应设在 **明显低于 107k**（如 80k–95k）以留余量，并配合 messages 数 > ~340 的旁路判断。

## 五、后续动作指针

- **A** 负值 max_tokens：代理侧已修（`82e35cd` 核对 `proxy.rs:1320-1360`），07-25 后归零。✅ 无需改动。
- **C** 空壳阈值：>107k 必触发；clamp-重试无效。
  - 修复方向 3（超长上下文跳过 clamp-重试、直接 502）✅ 已实施——见下 §七。
  - 修复方向 2（长 prompt 在上游前主动 compact）：经核查，长 prompt 是 Claude Code 的
    agentic 多轮对话历史（messages 345–553，末条多为 tool 结果），**非** `/compact` 请求，
    Claude Code 自己没在压缩。代理擅自改写历史会让 client 与模型对"看过什么"不一致 →
    采用"拒绝并明示"：✅ 已实施——见下 §八（413 上游前硬限）。
- **D** 现有加固是否覆盖这条 case：**未覆盖——pre-clamp 配置阈值=客户常见值，等于不生效**。证据见下 §六。
  - ✅ 已修：guard `>` → `>=`、glm-5.2 pre-clamp target 32000 → 8192（见 §六）。
- **根因 3** `/v1/props` 404：**评估后不做**。15 条/7 天是 client SDK `GET` 探测一个 NVIDIA 网关不认的端点
  （`/v1/props`，仓库无此路径），代理透传后上游返 404 且每次消耗一次 key 占用。但：(1) 收益偏小
  （client 持续重试，省不了 client 侧，只是省代理 key pool 占用 ~几十 ms）；(2) 处理方式都有代价——
  硬编码拒绝 `/v1/props` 难看且可能误拒 NVIDIA 未来端点；对未知 GET 跳过 key acquisition 会改透传路径
  的 key 语义。pre-clamp + clamp-retry 跳过 + 413 硬限已覆盖主问题（空壳 73%）。**存档不处理。**

  附：18 条 404 实为三类：(a) 15 条 `GET /v1/props`（SDK 探测，本项）；(b) 3 条 `POST /v1/chat/completions`
  上游偶发 404（07-30 12:40 三秒内，model 合法、endpoint 合法 → 上游瞬时抖动，现有 4xx 不重试直接透传）；
  (c) 1 条 `POST /v1/embeddings` model=`snowflake/arctic-embed-l` 上游结构化 404（embeddings 路径透传）。

## 六、D：现有 pre-clamp / clamp-retry 覆盖性验证（核心发现）

**数据证据**：216/216 条首次空壳请求的 `max_tokens=32000`，而配置为
`empty_shell_pre_clamp_max_tokens = { "z-ai/glm-5.2" = 32000, ... }`。

**漏洞**：`proxy.rs:1439-1450` 的 pre-clamp 触发条件是 `cur > target`：
cur(32000) == target(32000) → 条件不成立 → **pre-clamp 从不对那 216 条最 dominant 的 case 触发**。
请求以 max_tokens=32000 原样发出，prompt(107k+) + 32000 远超 glm-5.2 上下文预算 → 空壳。

**clamp-retry 同样失效**：空壳后重试时把 max_tokens 降到 8192（`empty_shell_retry_max_tokens`），
49/49 全部再次空壳 → 恢复率 0%。因为根因是 **prompt 本身超长**，砍 output 预算救不回 `prompt+output > 窗口`。

**结论**：`82e35cd` 的 max_tokens 负值修复对负值 400 有效（已归零）；但
`06f0ae7` 的 pre-clamp 对本日志的真实 case **静默失效**——target 配成 32000 恰好等于 client 常发值，
guard `cur > target` 把等号情形漏掉，等于在唯一高频的 case 上没有任何提前防护。

**修复方向**（按效费比排序）：
1. ✅ **已实施**：guard `cur > target` → `cur >= target`；glm-5.2 pre-clamp target 32000 → 8192
   （`config.rs` 默认 + `config.toml` + `config.toml.example` 同步；含两处测试断言更新；
   提交于 `e623607`）。
2. ⏳→✅ **改为拒绝并明示**：经核查长 prompt 是 Claude Code 的 agentic 多轮历史（非 `/compact`），
   代理擅自压缩会让 client 与模型对"看过什么"不一致。已改为上游前硬限 413——见 §八。
3. ✅ **已实施**：见 §七——超长上下文跳过 clamp-重试、直接 502 旁路。

## 七、长上下文跳过 clamp-重试旁路（修复方向 3，已实施）

**配置项** `empty_shell_no_retry_prompt_tokens`（默认 **90000**，env
`NVIDIA_EMPTY_SHELL_NO_RETRY_PROMPT_TOKENS`；0 关闭旁路、永远重试）。

**逻辑**（`proxy.rs` `handle_empty_shell_failure`）：首次空壳且 `estimate_prompt_tokens(request_body) ≥ 阈值`
时，跳过 clamp-重试、直接 502，并把 failure.log 的 `retry_reason` 标为
`empty_shell_no_retry_prompt_too_large`。`estimate_prompt_tokens` = Σ message content chars / 4，
与 `request_body_fingerprint` 的 `est_prompt_tokens` 同一启发式。

**依据**：failure.log 216/216 首次空壳 est_prompt_tokens 全 ≥107,485，clamp-重试恢复率 0%。
阈值取 90k（显低于下界 107k）留余量，避免误杀临界附近本可重试成功的请求。仅对首次空壳
（`retry_attempt_already_done == 0`）短路：已提交重试链不撤回。

**测试**：`empty_shell_skips_retry_when_prompt_exceeds_no_retry_threshold`——构造 prompt 估算超阈值，
断言单次上游调用、502、`retry.attempted=false`、detail 不含 retry-clamp 标记。67 lib + 9 integration 全过。

## 八、长 prompt 上游前硬限 413（修复方向 2，已实施——改为拒绝并明示）

**核查**：长 prompt 是 Claude Code 的 agentic 多轮对话历史（messages 345–553，末条多为 tool 结果），
**非** `/compact` 请求；Claude Code 自己没在压缩。代理擅自改写这些历史会让 client 与模型对
"看过什么"产生不一致 → 不做"代理自动压缩 messages"，改为把决定权交回 client。

**配置项** `request_max_prompt_tokens`（默认 **120000**，env `NVIDIA_REQUEST_MAX_PROMPT_TOKENS`；
0 关闭硬限）。这是**硬限**，位于软限（90k）之上：

- `[0, 90k)`：正常路径，重试照常。
- `[90k, 120k)`：发首次；若空壳则**不重试、502**（§七 软限）。
- `≥ 120k`：**根本不发**，直接 **413**（§八 硬限），零上游调用、零 key 占用。

**逻辑**（`proxy.rs` `forward_upstream`，body normalization 后、并发/key 获取前）：
若 `request_max_prompt_tokens > 0` 且 `estimate_prompt_tokens(body) ≥ limit`，立即返
`413 Payload Too Large` + JSON `{"error":{"type":"request_too_large","code":"prompt_tokens_exceeds_ceiling",
"est_prompt_tokens":…,"limit":…, "message":"…compress your conversation context (e.g. clear history / run /compact) and retry."}}`。
**不改写 client 的 messages**——仅失败并明示。

**依据**：failure.log 216/216 首次空壳 `est_prompt_tokens` 全 ≥107,485。硬限 120k 略高于该下界，
只在数据显示必失败时拒绝；`[90k,120k)` 给可能成功的请求保留一次机会。把"主动 compact"留作
client（Claude Code）的职责，符合"不静默改写别人的成竹之作"原则。

**测试**：`oversized_prompt_request_rejected_with_413_before_upstream`——设硬限=5、发 est≥5 的请求，
断言 **零上游调用**（`expect(0)`）、413、`type=request_too_large`、`code=prompt_tokens_exceeds_ceiling`、
body 含 limit/est。68 lib + 9 integration 全过，clippy 无新增警告。
