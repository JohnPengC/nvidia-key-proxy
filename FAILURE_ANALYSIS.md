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
- **C** 空壳阈值：>107k 必触发；clamp-重试无效。需新增"主动 compact / 上下文回收"，并停止对 >107k 的 glm-5.2 clamp 重试。
- **D** 现有加固是否覆盖这条 case：**未覆盖——pre-clamp 配置阈值=客户常见值，等于不生效**。证据见下。

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
1. 把 pre-clamp 判定改为 `cur >= 某个明确小于窗口的安全上限`，或把 `empty_shell_pre_clamp_max_tokens["z-ai/glm-5.2"]`
   下调到 **明显小于 32000** 的安全值（如 8000–12000），并改为 `cur >= target` 触发——确保等号与略小都被 clamp。
2. 长 prompt 不应靠砍 max_tokens 救：est_prompt_tokens 超过 ~90k 即应 **在上游前主动 compact**
   或直接 502 退回，避免烧一整轮 round-trip 去撞必然的空壳。
3. clamp-重试对超长上下文已证明 0% 有效；若重启空壳重试，应加"est_prompt_tokens 超阈值则不重试、直接 502"的旁路，
   避免无谓消耗 key 与配额。
