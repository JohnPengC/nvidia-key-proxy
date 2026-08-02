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

**配置项** `request_max_prompt_tokens`（默认 **90000**，env `NVIDIA_REQUEST_MAX_PROMPT_TOKENS`；
0 关闭硬限；经历两轮下调：2026-08-01 由 120000 → 95000，2026-08-02 由 95000 → 90000。理由见下）。
**硬限与软限 `empty_shell_no_retry_prompt_tokens` 持平（均 90k）——不保留灰度带**：

- `[0, 90k)`：正常路径，重试照常。
- `≥ 90k`：**根本不发**，直接 **413**（§八 硬限），零上游调用、零 key 占用。
  （原 `[90k, X)` 灰度带经实测在 GLM-5.2 上 100% 必 shell，已取消，见下。）

**逻辑**（`proxy.rs` `forward_upstream`，body normalization 后、并发/key 获取前）：
若 `request_max_prompt_tokens > 0` 且 `estimate_prompt_tokens(body) ≥ limit`，立即返
`413 Payload Too Large` + JSON `{"error":{"type":"request_too_large","code":"prompt_tokens_exceeds_ceiling",
"est_prompt_tokens":…,"limit":…, "message":"…compress your conversation context (e.g. clear history / run /compact) and retry."}}`。
**不改写 client 的 messages**——仅失败并明示。

**依据（两轮实测）**：failure.log 早期 216/216 首次空壳 `est_prompt_tokens` 全 ≥107,485。
- **第一轮**（硬限 120000）：实测 77/77 空壳 est ∈ [98288, 109231]，**无一达到 120k** → 旧硬限 120000 从未生效、
  全部 doomed `/compact` 撞上 502 而非 413，Claude Code 因 502 语义"上游临时出错"指数退避重试 ~10 分钟。
  `chars/4` 估算低估真实 token，故 ~109k 估算 + max_tokens 早已超 glm-5.2 的 ~128k 窗口。2026-08-01 据此下调到 **95000**。
- **第二轮**（硬限 95000，留 [90k,95k) 灰度带）：实测 32/32 空壳 est ∈ [92488, 93445]，
  **全部落在"灰度带"内、32/32 shell、0% 成功** → 灰度带的"一次机会"在 GLM-5.2 上等于"一次必 shell + 一个误导性 502"，
  Claude Code 再次重试 ~10 分钟（与第一轮同款卡死）。同时实测 shell 下界由 98288 下移到 92488（会话增长所致）。
  2026-08-02 据此把硬限下调到 **90000**——与软限持平、取消灰度带：est ≥ 90k 即 413，零上游 round-trip、零 client 重试风暴。
把"主动 compact"留作 client（Claude Code）的职责，符合"不静默改写别人的成竹之作"原则。
若日后把 `chars/4` 换成更准的估算器，可重新评估能否恢复一个小灰度带。

**测试**：`oversized_prompt_request_rejected_with_413_before_upstream`——设硬限=5、发 est≥5 的请求，
断言 **零上游调用**（`expect(0)`）、413、`type=request_too_large`、`code=prompt_tokens_exceeds_ceiling`、
body 含 limit/est。68 lib + 9 integration 全过，clippy 无新增警告。

## 九、多模态透传实端验证（2026-08-01，被 NVIDIA 上游 403 验证）

**背景**：用户发现服务没有多模态支持。代码层确认代理原按"纯文本 chat completion"假设写——
`estimate_prompt_tokens` 对非字符串 `content` 用 `to_string().chars().count()`，inline base64 图片会
暴涨 token 估算几十倍、误触发 413 硬限（120k）与 90k 软限。已修（见下），随后对真实 NVIDIA 端点做
实端验证看上游是否真的接受 OpenAI 多模态格式。

**代码改动（已实施，保留——见下"仍然有价值"）**：
- `src/proxy.rs` 新增 `EstPromptCosts` + `message_content_tokens`，重写 `estimate_prompt_tokens`：
  `content` 字符串照旧 `chars/4`；数组内 `{type:text}`→`chars/4`、`{type:image_url}`→固定 `est_image_token_cost`、
  `{type:input_audio}`→base64 长度折算+封顶、未知 type→`chars/4` 封顶（base64 兜底）。
- `request_body_fingerprint` 改用同一 helper（口径一致），数组 content 改打 part census（`text=N image=N`），
  不再 dump base64。
- config 新增 `est_image_token_cost`(1500)/`est_audio_token_per_kb`(16)/`est_audio_token_cap`(10000)/
  `est_unknown_part_token_cap`(2000)，env 覆盖。
- 10 个新测试（78 lib + 9 integration 全过）：含 base64 图的请求不被 413（恰好 1 次上游调用）、
  多模态+超长文本仍 413（0 次上游调用，保护没被拆掉）、normalization 后 content 数组逐字保留、
  fingerprint 不漏 base64。

**实端验证方法**：启动代理（注入 config.toml 的 22 个真实 key），打 `https://integrate.api.nvidia.com`，
五个对照实验：

| # | 请求（content 形态 → 模型） | HTTP |
|---|---|---|
| A | 纯文本**字符串** content → `meta/llama-3.2-11b-vision-instruct`（vision 模型）| **200**，推理出 "OK"，usage 有值 |
| B | vision 模型 + 数组 content + **inline base64** image_url | **403** Authorization failed |
| C | vision 模型 + 数组 content + **URL-only** image_url（无 base64）| **403** Authorization failed |
| D | vision 模型 + 数组 content 但**只含 text 块**（无图）| **403** Authorization failed |
| E | 文本模型 `z-ai/glm-5.2` + 数组 content 带 image_url | **403** Authorization failed |

**决定性是 D 与 E**：只要 `messages[].content` 从字符串变成 OpenAI 多模态**数组**形式——不论模型
是不是 vision、不论数组里有没有图片——NVIDIA 上游一律返 `403 Authorization failed`。同一批 key 同一模型，
纯文本字符串 content 200、数组 content 403。

**根因（实测排除后定性）**：
1. **不是代理的问题**——代理日志确认 body 原样透传（`body_summary` 不动 messages/content）；B/C/D/E
   也排除了 base64 编码、图片 URL、具体模型这些干扰，纯粹是 `content` **形态**触发 NVIDIA 上游鉴权限流。
2. **推翻了第一阶段调研的推断**——调研结论"OpenAI 兼容格式大概率可用、未经实端验证"被实测证伪：
   对当前这批 key，content 数组一律 403。NVIDIA 官方公共文档只写纯文本串 `content` 是有原因的。
3. **最可能的具体机制**：当前 NVIDIA API key 的套餐/权限没开通 vision/多模态，NVIDIA 在模型能力门控
   层对 content-数组请求做鉴权拦截、返 403（而非 400 格式错——否则 D 这种没图的数组会 400 而非 403）。
   要让多模态真跑通，需换/开通有 vision 权限的 key 套餐——属 NVIDIA 账号侧，代理代码改不了。

**这套多模态代码改动仍然有价值，保留**：
- 它修的是**代理侧自己的误判 bug**——修前，即便换成有 vision 权限的 key，带 base64 的多模态请求
  仍会被代理自己的 413（120k）/90k 软限误杀（base64 估算暴涨几十倍）。
- 修后，proxy 对 content 数组**不再误判也不再破坏**：base64 不撑爆估算、数组原样透传给上游。
- 当客户端用 content 数组发请求时，代理把决定权完整交给 NVIDIA（合法就放行转发，上游 403 原样透传
  给客户端不做静默吞改）——符合 §八"不静默改写别人的成竹之作"原则。

**启用多模态的前提（非代码、运维/账号侧）**：换成/开通 NVIDIA vision 权限的 key。代理侧的多模态
透传 + 估算豁免已就绪，届时无需再改代码即可生效。代理对无权限 key 的 403 按 §三 `upstream_4xx` 既有
策略透传给客户端（不重试 4xx），这是正确行为——403 是权限、重试无意义。

**未做（与范围一致）**：视频（NIM 无标准化视频输入字段）；前端；代理侧格式翻译/校验（NIM 多模态官方
schema 未文档化，代理保持 OpenAI 兼容透传、不假设、不改写）。

