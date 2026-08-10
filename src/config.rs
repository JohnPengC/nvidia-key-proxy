use dotenvy::dotenv;
use serde::Deserialize;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use tracing::{info, warn};

use crate::key_pool::mask_key;

/// 从 `scheme://[userinfo@]host[:port]/...` URL 中提取 host 部分，
/// 无需引入 URL crate。若字符串没有可识别的 scheme://authority 则返回 `None`。
/// 能处理带方括号的 IPv6 字面量（如 `[::1]`）。供 `Config::validate` 使用，
/// 使开发主机检查比较的是真实 host，而不是整个 URL 的某个子串。
fn host_of_url(url: &str) -> Option<String> {
    let idx = url.find("://")?;
    let after = &url[idx + 3..];
    // authority 部分到第一个 '/'、'?' 或 '#' 为止。
    let end = after
        .find(['/', '?', '#'])
        .unwrap_or(after.len());
    let authority = &after[..end];
    // 若存在 `userinfo@` 则剥掉它。
    let authority = match authority.rfind('@') {
        Some(at) => &authority[at + 1..],
        None => authority,
    };
    // 带方括号的 IPv6 字面量：`[::1]` 或 `[::1]:port`。
    // 返回**不带**外层方括号的字符串（如 `::1`），让下游的占位 host 比对
    //（`host_lower == "::1"`）一致命中。此前返回 `"[::1]"` 带方括号，比对
    // 用 `== "::1"` 永远不命中，IPv6 loopback 的 base_url 不会被拒在门禁外。
    if let Some(rest) = authority.strip_prefix('[') {
        let close = rest.find(']')?;
        return Some(rest[..close].to_string());
    }
    // 普通 host —— 剥掉末尾的 `:port`。
    let host = match authority.rfind(':') {
        Some(colon) => {
            // 确保 `colon` 不在 IPv6 之类的东西里；对普通 host 而言
            // 最后一个 ':' 之后的部分必须全是数字。
            let port_part = &authority[colon + 1..];
            if port_part.chars().all(|c| c.is_ascii_digit()) && !port_part.is_empty() {
                &authority[..colon]
            } else {
                authority
            }
        }
        None => authority,
    };
    Some(host.to_string())
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub base_url: String,
    pub keys: Vec<String>,
    pub rate_limiter: RateLimiterConfig,
    pub cooldown: CooldownConfig,
    pub health_check: HealthCheckConfig,
    pub observability: ObservabilityConfig,
    /// 可选的请求超时（秒）。0 表示完全禁用超时
    ///（代理长时间运行的流式请求时推荐此设置）。
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    /// 重试/上游错误策略（指数退避、禁用 key 的阈值）。
    #[serde(default)]
    pub retry: RetryConfig,
    /// 客户端可见的模型名 → NVIDIA 适用的模型 id。默认为空。
    /// 当 chat-completion 请求携带的 `model` 命中这里的某个 key 时，
    /// 代理会在转发前把该字段改写为映射值；未命中的名字原样转发。
    #[serde(default)]
    pub model_aliases: std::collections::HashMap<String, String>,
    /// 允许 `base_url` 指向 loopback / localhost 地址。默认关闭，
    /// 因为生产环境绝不应以 example.com/localhost 为目标；但开启它
    /// 在开发 / 集成测试时把代理指向本地 NVIDIA 兼容 mock 很方便。
    #[serde(default)]
    pub allow_local_base_url: bool,
    /// 代理自身的前门鉴权。池里的 NVIDIA 配额对任何能访问
    /// `listen_addr` 的人都开放；启用此项后，客户端必须在 `/v1/*`
    /// 请求上出示 `Authorization: Bearer <bearer_token>`。默认关闭以
    /// 保留现有客户端依赖的“任意 api_key 都能用”的行为。`/health` 始终豁免。
    #[serde(default)]
    pub auth: AuthConfig,
    /// 向流式 chat 请求注入 `stream_options.include_usage: true`，
    /// 使上游在流结束时返回 token 用量。NVIDIA/OpenAI 仅在该 flag 置位时
    /// 才发送 usage；否则在（常见的）流式场景下代理的 token 指标恒为 0。
    /// 默认 true；通过 `inject_stream_usage = false` 或
    /// `NVIDIA_INJECT_STREAM_USAGE=false` 关闭。
    #[serde(default = "default_inject_stream_usage")]
    pub inject_stream_usage: bool,
    /// 当上游返回 2xx 非 SSE 响应体且其 `choices` 数组为空（`choices: []`）时，
    /// 合成一个占位的 `choices` 条目。cc-switch 的非流式转换器
    /// `openai_to_anthropic` 遇到空 `choices` 会硬报错（"格式转换错误:
    /// Empty choices array" → 向 Claude Code 返回 422）；这种情况常见于
    /// context compaction（`stream:false`）——模型未产生 token 但仍返回了
    /// `usage`。与其依赖 cc-switch 打补丁，代理直接把该响应体原地改写为
    /// 合法的单选择响应（空 assistant message、`finish_reason: stop`，
    /// 保留 `id`/`model`/`usage`），从而让 cc-switch 成功。
    ///
    /// 仅影响非流式、非 SSE 的成功分支——而且在判定时只缓冲有界的前
    /// ≤64 KiB 前缀，因此大型流式补全仍保持零拷贝透传。默认 true；通过
    /// `synthesize_empty_choices = false` 或
    /// `NVIDIA_SYNTHESIZE_EMPTY_CHOICES=false` 关闭。
    #[serde(default = "default_synthesize_empty_choices")]
    pub synthesize_empty_choices: bool,
    /// 当 NVIDIA 返回一个“空壳” 2xx —— 形如
    /// `{"choices":[], "id":"", "usage":null, ...}`、每个字段都为
    /// null/空的响应体——时，用一个降低后的 `max_tokens` 重试一次。
    /// 这是 build.integrate 上 GLM 类模型的静默软失败（没有真正跑过
    /// 推理——所以是 `usage:null`，而合法的 compaction 空 choices 响应体
    /// 的 `usage` 仍会存在）。一条 198 条消息的 `/compact` 带
    /// `max_tokens=64000` 常常踩中：NVIDIA 把整条请求丢进一个空壳，
    /// 而不是截断 `max_tokens`。重试时把同一请求体的 `max_tokens` 钳到
    /// `empty_shell_retry_max_tokens`（默认 8192），通常能让推理真正跑起来。
    /// 默认 true；通过 `retry_on_empty_shell = false` 或
    /// `NVIDIA_RETRY_ON_EMPTY_SHELL=false` 关闭。
    #[serde(default = "default_retry_on_empty_shell")]
    pub retry_on_empty_shell: bool,
    /// 在空壳软失败后重试时使用的 `max_tokens` 值。
    /// 只影响那一次重试；原始请求会先原样转发。8192 对 build.integrate 上的
    /// GLM 系模型是保守取值。可通过 `empty_shell_retry_max_tokens` 配置项
    /// 或 `NVIDIA_EMPTY_SHELL_RETRY_MAX_TOKENS` 环境变量覆盖。
    #[serde(default = "default_empty_shell_retry_max_tokens")]
    pub empty_shell_retry_max_tokens: u64,
    /// 在重试空壳失败前，对 /v1/models 做一次轻量健康探测，
    /// 避免向已退化的上游重试。默认 true。可通过
    /// `NVIDIA_EMPTY_SHELL_PROBE_BEFORE_RETRY` 覆盖。
    #[serde(default = "default_empty_shell_probe_before_retry")]
    pub empty_shell_probe_before_retry: bool,
    /// 熔断阈值。某个 key/model 上连续 N 次空壳或 504 响应后，
    /// 该 key 被冷却 cooldown.base_seconds。0 = 禁用。默认 3。
    /// 可通过 `NVIDIA_CIRCUIT_BREAKER_THRESHOLD` 覆盖。
    #[serde(default = "default_circuit_breaker_threshold")]
    pub circuit_breaker_threshold: u32,

    /// 熔断器是否按 (key, model) 维度而非按 key 全局统计失败。
    /// 默认 true。可通过 `NVIDIA_CIRCUIT_BREAKER_PER_MODEL` 覆盖。
    #[serde(default = "default_circuit_breaker_per_model")]
    pub circuit_breaker_per_model: bool,

    /// 在 503 Service Unavailable（NVIDIA Worker 本地请求上限到达）时重试。
    /// 启用后，503 响应会触发一次指数退避地重试另一个 key
    ///（使用与 401 重试相同的重试预算）。默认 true。
    /// 可通过 `NVIDIA_RETRY_ON_503` 覆盖。
    #[serde(default = "default_retry_on_503")]
    pub retry_on_503: bool,

    /// 在重试 503 失败前，对 /v1/models 做一次轻量健康探测，
    /// 避免向已退化的上游重试。默认 true。
    /// 可通过 `NVIDIA_PROBE_BEFORE_503_RETRY` 覆盖。
    #[serde(default = "default_probe_before_503_retry")]
    pub probe_before_503_retry: bool,

    /// 在 500 Internal Server Error 时重试。NVIDIA/GLM 类模型会间歇性
    /// 返回 `{"message":"Failed to generate
    /// completions","type":"Internal Server Error","code":500}` —— 一次
    /// 瞬态的单请求 worker 失败（同样的 prompt 几秒后换个 key 就能成功）。
    /// 启用后，500 响应会触发一次重试另一个 key，使用与 401/503 相同的
    /// 重试预算。默认 true。可通过 `NVIDIA_RETRY_ON_500` 覆盖。
    #[serde(default = "default_retry_on_500")]
    pub retry_on_500: bool,

    /// 空壳软失败的重试次数。2 = 原始 + 两次重试。
    /// 0 = 不重试（回退为 502）。默认 2。
    /// 可通过 `NVIDIA_EMPTY_SHELL_RETRY_ATTEMPTS` 覆盖。
    #[serde(default = "default_empty_shell_retry_attempts")]
    pub empty_shell_retry_attempts: u32,

    /// 空壳重试之间指数退避的基本延迟（毫秒）。默认 500。
    /// 可通过 `NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_BASE_MS` 覆盖。
    #[serde(default = "default_empty_shell_retry_backoff_base_ms")]
    pub empty_shell_retry_backoff_base_ms: u64,

    /// 单次空壳重试退避睡眠的上限（毫秒）。默认 5000。
    /// 可通过 `NVIDIA_EMPTY_SHELL_RETRY_BACKOFF_MAX_MS` 覆盖。
    #[serde(default = "default_empty_shell_retry_backoff_max_ms")]
    pub empty_shell_retry_backoff_max_ms: u64,

    /// 按模型在首次尝试前进行 max_tokens 预钳制的上限。
    /// 当请求命中的模型在此 map 中且 max_tokens 大于等于该上限时，
    /// 代理会在第一次上游调用前就把它钳下去，从而降低已知问题模型的
    /// 空壳出现率。默认包含 z-ai/glm-5.2 → 8192。
    /// 可通过 `NVIDIA_EMPTY_SHELL_PRE_CLAMP_MAX_TOKENS`（JSON map）覆盖。
    #[serde(default = "default_empty_shell_pre_clamp_max_tokens")]
    pub empty_shell_pre_clamp_max_tokens: std::collections::HashMap<String, u64>,

    /// 当 estimated prompt 超过此阈值时，空壳软失败不再用钳后的
    /// max_tokens 重试。failure.log 显示超长上下文（est_prompt_tokens
    /// 107k+）上的钳制重试恢复率为 0%（49/49 重新空壳）：那里的空壳
    /// 并非由输出预算过大引起，而是 `prompt + 任意 max_tokens` 超过了模型
    /// 的上下文窗口，因此缩小 max_tokens 无济于事。超过此阈值时代理跳过
    /// 重试往返，直接以 `empty_shell_no_retry_prompt_too_large` 为原因
    /// 返回 502，避免浪费上游调用。0 禁用此旁路（始终按既有策略重试）。
    /// 默认 90000（远低于 failure.log 中 107k 的空壳下限）。
    /// 可通过 `NVIDIA_EMPTY_SHELL_NO_RETRY_PROMPT_TOKENS` 覆盖。
    #[serde(default = "default_empty_shell_no_retry_prompt_tokens")]
    pub empty_shell_no_retry_prompt_tokens: u64,

    /// 对请求 estimated prompt tokens 的硬上限：非流式或流式
    /// chat-completions 请求，若其估算 prompt 大小大于等于此上限，则
    /// 在最前面就被 HTTP 413 拒绝（不发起上游调用），要求客户端自行压缩
    /// 其上下文。代理拒绝，而不是浪费一次上游往返和一个 key slot 在一个
    /// 注定不可能成功的请求上。Auto-compact 是客户端的职责（Claude Code
    /// 自己持有它的对话历史）；我们给出清晰的 413，而不是悄悄改写客户端的
    /// messages。
    ///
    /// **设定为等于 `empty_shell_no_retry_prompt_tokens`（两者默认都 90000）
    /// ——软限制和硬限制之间刻意没有灰色地带。** 大于等于硬限制的请求直接
    /// 拒绝（413）；低于软限制的请求走正常重试路径。消除了该灰带的两轮实测
    /// 见下方 `default_*` 部分的说明。0 禁用硬限制（始终放行第一次调用）。
    ///
    /// 默认 90000。从 failure.log 中真实的空壳分布出发，分两轮调出：
    ///(1) 当上限为 120000 时，观察到的 77 个空壳里 est_prompt_tokens 全在
    ///     [98288, 109231] 之间——没有一个到达 120k，所以 413 守卫从不触发，
    ///     每一个注定失败的 /compact 都表现为 502 "empty chat completion"
    ///     被客户端重试约 10 分钟，而不是一个诚实的 413。（`chars/4` 估算
    ///     会低估真实 tokens，所以一旦加上 max_tokens，一个 ~109k 的估算
    ///     实际上就已经让 ~128k 模型窗口溢出。）下调到 95000。
    ///(2) 当上限为 95000 时（仍允许一次尝试的 [90k, 95k) "一次性" 灰带），
    ///     第二天全部 32 个空壳的 est 落在 [92488, 93445]——每一个都落在该
    ///     灰带内，32 个全部空壳（0% 成功）。这条灰带给 Claude Code 一个 502，
    ///     它又重试约 10 分钟（与第一轮同样的失败模式），而不是一条它能
    ///     立刻接受的清晰 413。随着会话增长，实测的空壳下限也从 98288 降到
    ///     了 92488。
    /// 因此 90000 折叠了该灰带：落在两个实测下限之下，任何 est >= 90000
    /// → 立即 413，零上游往返、零客户端重试风暴。若日后用更准确的估算器
    /// 取代 `chars/4`，可重新评估能否重新引入一小段灰带。
    /// 可通过 `NVIDIA_REQUEST_MAX_PROMPT_TOKENS` 覆盖。
    #[serde(default = "default_request_max_prompt_tokens")]
    pub request_max_prompt_tokens: u64,

    // ==================== 多模态 prompt-token 估算 ====================
    /// 在估算多模态请求的 prompt 大小时，给每个 `{type:"image_url",…}` 块
    /// 赋予的粗略单 part token 成本，无论该 URL 是远程 `https://` 引用
    /// 还是内联 `data:…;base64,` 载荷。代理原样透传多模态响应体，但其
    /// prompt 守卫（413 硬上限、空壳不重试软限制）不能被糊弄：把一张 base64
    /// 图片按 `chars/4` 计数会把估算放大好几个数量级，从而用 413 拒掉一个
    /// 合法请求；而只算 URL 又会低估，让过长的 prompt 漏过去。
    /// 默认 1500。0 回退到 unknown-part 成本。
    /// 可通过 `NVIDIA_EST_IMAGE_TOKEN_COST` 覆盖。
    #[serde(default = "default_est_image_token_cost")]
    pub est_image_token_cost: u64,
    /// `{type:"input_audio",…}` 块中每 *KiB* base64 音频载荷的 token 成本，
    /// 上限为 `est_audio_token_cap`。默认 16。0 回退到 unknown-part 成本。
    /// 可通过 `NVIDIA_EST_AUDIO_TOKEN_PER_KB` 覆盖。
    #[serde(default = "default_est_audio_token_per_kb")]
    pub est_audio_token_per_kb: u64,
    /// 对单个块 audio 估算的硬上限，免得一个超大 clip 把估算撑爆。
    /// 默认 10000。可通过 `NVIDIA_EST_AUDIO_TOKEN_CAP` 覆盖。
    #[serde(default = "default_est_audio_token_cap")]
    pub est_audio_token_cap: u64,
    /// 对任何无法识别形状的 content part（未知 `type`，或其专属成本为 0 的
    /// 已知 type）施加的上限：会按其序列化的 `chars/4` 计数但被钳到此，
    /// 这样某个意外字段里的一团 base64 blob 不会把估算撑爆，但仍然算进去
    /// *一点东西*。默认 2000。0 表示完全忽略 unknown parts。
    /// 可通过 `NVIDIA_EST_UNKNOWN_PART_TOKEN_CAP` 覆盖。
    #[serde(default = "default_est_unknown_part_token_cap")]
    pub est_unknown_part_token_cap: u64,

    /// 用于 max_tokens 校验的可选上限。任何 max_tokens > 此值的请求会让该
    /// 字段被丢弃（改用模型默认值）。默认 100000。
    /// 可通过 `MAX_TOKENS_MAX` 环境变量覆盖。
    #[serde(default = "default_max_tokens_max")]
    pub max_tokens_max: u64,

    // ==================== 并发限制（Priority 2）====================
    /// 每个模型的最大并发上游请求数。当某模型并发达到上限时，新请求会被
    /// 排队（最长 `concurrency_queue_timeout_ms`）或以 503 拒绝。
    /// 默认：空（无限制）。可通过
    /// `NVIDIA_CONCURRENCY_LIMIT_PER_MODEL`（JSON map）覆盖。
    #[serde(default)]
    pub concurrency_limit_per_model: std::collections::HashMap<String, u32>,

    /// 为拿到一个并发信号量槽位而等待的最长时间（毫秒）。
    /// 若队列满这么久仍无槽位，则带 Retry-After 返回 503。
    /// 默认 5000（5s）。可通过 `NVIDIA_CONCURRENCY_QUEUE_TIMEOUT_MS` 覆盖。
    #[serde(default = "default_concurrency_queue_timeout_ms")]
    pub concurrency_queue_timeout_ms: u64,

    // ==================== 超时调优（Priority 3）====================
    /// 流式连接的请求超时（Priority 3）。当 > 0 时，流式调用将使用它
    /// 而非 request_timeout_seconds。0 = 用 request_timeout_seconds。
    /// 默认 0（禁用）。可通过 NVIDIA_REQUEST_TIMEOUT_STREAMING_SECONDS
    /// 覆盖。
    #[serde(default)]
    pub request_timeout_streaming_seconds: u64,

    // ==================== 网络韧性（Priority 4）====================
    /// 上游连接超时（秒）。默认 5。
    /// 可通过 NVIDIA_UPSTREAM_CONNECT_TIMEOUT_SECONDS 覆盖。
    #[serde(default = "default_upstream_connect_timeout_secs")]
    pub upstream_connect_timeout_secs: u64,

    /// 上游连接池空闲超时（秒）。默认 90。
    /// 可通过 NVIDIA_UPSTREAM_POOL_IDLE_TIMEOUT_SECONDS 覆盖。
    #[serde(default = "default_upstream_pool_idle_timeout_secs")]
    pub upstream_pool_idle_timeout_secs: u64,

    /// 重试抖动（毫秒）。叠加到指数退避之上。默认 100。
    /// 可通过 NVIDIA_RETRY_JITTER_MS 覆盖。
    #[serde(default = "default_retry_jitter_ms")]
    pub retry_jitter_ms: u64,
}

/// 用于取得默认 pre-clamp map 的公开封装（供测试使用）。
pub fn default_empty_shell_pre_clamp_max_tokens() -> std::collections::HashMap<String, u64> {
    let mut m = std::collections::HashMap::new();
    // 8192，不是 32000：GLM 类模型（~128k 上下文）在 `prompt + max_tokens`
    // 超过其剩余预算时会返回一个 HTTP-200 空壳。failure.log 显示全部 216 个
    // 首次尝试空壳的 max_tokens 都带 32000（一旦把 ~107k+ 的 prompt 算进去
    // 后就是那个空输出预算）。钳到 8192 可为最多 ~120k 的 prompt 留出输出空间。
    m.insert("z-ai/glm-5.2".to_string(), 8192);
    m
}

fn default_request_timeout_seconds() -> u64 {
    60
}

fn default_request_timeout_streaming_seconds() -> u64 {
    0
}

fn default_inject_stream_usage() -> bool {
    true
}

fn default_synthesize_empty_choices() -> bool {
    true
}

fn default_retry_on_empty_shell() -> bool {
    true
}

fn default_empty_shell_retry_max_tokens() -> u64 {
    8192
}

fn default_empty_shell_retry_attempts() -> u32 {
    2
}

fn default_empty_shell_probe_before_retry() -> bool {
    true
}

fn default_empty_shell_retry_backoff_base_ms() -> u64 {
    500
}

fn default_empty_shell_retry_backoff_max_ms() -> u64 {
    5000
}

fn default_empty_shell_no_retry_prompt_tokens() -> u64 {
    90000
}

/// 硬 prompt-token 上限默认值的公开封装。供 `proxy.rs` / `key_pool.rs` /
/// `integration_test.rs` 中的测试脚手架使用，使其手工构造的 `Config` 遵循
/// 生产默认值而无需重述该字面量（与 `default_empty_shell_pre_clamp_max_tokens`
/// 镜像对应）。
pub fn default_request_max_prompt_tokens() -> u64 {
    90000
}

fn default_est_image_token_cost() -> u64 {
    1500
}

fn default_est_audio_token_per_kb() -> u64 {
    16
}

fn default_est_audio_token_cap() -> u64 {
    10000
}

fn default_est_unknown_part_token_cap() -> u64 {
    2000
}

fn default_circuit_breaker_threshold() -> u32 {
    3
}

fn default_circuit_breaker_per_model() -> bool {
    true
}

fn default_retry_on_503() -> bool {
    true
}

fn default_probe_before_503_retry() -> bool {
    true
}

fn default_retry_on_500() -> bool {
    true
}

fn default_concurrency_queue_timeout_ms() -> u64 {
    5000
}

fn default_upstream_connect_timeout_secs() -> u64 {
    5
}

fn default_upstream_pool_idle_timeout_secs() -> u64 {
    90
}

fn default_retry_jitter_ms() -> u64 {
    100
}

fn default_max_tokens_max() -> u64 {
    100_000
}

/// 前门鉴权配置。理由见 `Config::auth`。
#[derive(Debug, Deserialize, Clone)]
#[derive(Default)]
pub struct AuthConfig {
    /// 要求客户端在代理转发其请求前完成鉴权。为 false 时，请求会被转发
    /// 且不校验 token。
    #[serde(default)]
    pub enabled: bool,
    /// 客户端必须出示的 Bearer token。也可通过
    /// `NVIDIA_AUTH_BEARER_TOKEN` 覆盖。未设置时为空。
    #[serde(default)]
    pub bearer_token: String,
    /// `/admin/*` 管理 API（key 增删 / 状态）所要求的 Bearer token。
    /// 为空（且未设 `NVIDIA_AUTH_ADMIN_TOKEN`）时，admin API 被完全禁用。
    /// 也可通过 `NVIDIA_AUTH_ADMIN_TOKEN` 覆盖。
    #[serde(default)]
    pub admin_token: String,
}


impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let masked_keys: Vec<String> = self.keys.iter().map(|k| mask_key(k)).collect();
        f.debug_struct("Config")
            .field("listen_addr", &self.listen_addr)
            .field("base_url", &self.base_url)
            .field(
                "keys",
                &format!("<{} keys: {:?}>", self.keys.len(), masked_keys),
            )
            .field("rate_limiter", &self.rate_limiter)
            .field("cooldown", &self.cooldown)
            .field("health_check", &self.health_check)
            .field("observability", &self.observability)
            .field("request_timeout_seconds", &self.request_timeout_seconds)
            .field("request_timeout_streaming_seconds", &self.request_timeout_streaming_seconds)
            .field("retry", &self.retry)
            .field("model_aliases", &self.model_aliases)
            .field("allow_local_base_url", &self.allow_local_base_url)
            .field("inject_stream_usage", &self.inject_stream_usage)
            .field("synthesize_empty_choices", &self.synthesize_empty_choices)
            .field("retry_on_empty_shell", &self.retry_on_empty_shell)
            .field(
                "empty_shell_retry_max_tokens",
                &self.empty_shell_retry_max_tokens,
            )
            .field(
                "empty_shell_retry_attempts",
                &self.empty_shell_retry_attempts,
            )
            .field(
                "empty_shell_probe_before_retry",
                &self.empty_shell_probe_before_retry,
            )
            .field(
                "empty_shell_retry_backoff_base_ms",
                &self.empty_shell_retry_backoff_base_ms,
            )
            .field(
                "empty_shell_retry_backoff_max_ms",
                &self.empty_shell_retry_backoff_max_ms,
            )
            .field(
                "empty_shell_pre_clamp_max_tokens",
                &self.empty_shell_pre_clamp_max_tokens,
            )
            .field(
                "empty_shell_no_retry_prompt_tokens",
                &self.empty_shell_no_retry_prompt_tokens,
            )
            .field(
                "request_max_prompt_tokens",
                &self.request_max_prompt_tokens,
            )
            .field(
                "est_image_token_cost",
                &self.est_image_token_cost,
            )
            .field(
                "est_audio_token_per_kb",
                &self.est_audio_token_per_kb,
            )
            .field("est_audio_token_cap", &self.est_audio_token_cap)
            .field(
                "est_unknown_part_token_cap",
                &self.est_unknown_part_token_cap,
            )
            .field(
                "circuit_breaker_threshold",
                &self.circuit_breaker_threshold,
            )
            .field(
                "circuit_breaker_per_model",
                &self.circuit_breaker_per_model,
            )
            .field("max_tokens_max", &self.max_tokens_max)
            .field("concurrency_limit_per_model", &self.concurrency_limit_per_model)
            .field("concurrency_queue_timeout_ms", &self.concurrency_queue_timeout_ms)
            .field("request_timeout_streaming_seconds", &self.request_timeout_streaming_seconds)
            .field("upstream_connect_timeout_secs", &self.upstream_connect_timeout_secs)
            .field("upstream_pool_idle_timeout_secs", &self.upstream_pool_idle_timeout_secs)
            .field("retry_jitter_ms", &self.retry_jitter_ms)
            .field(
                "auth",
                &format!(
                    "AuthConfig {{ enabled: {}, bearer_token: {}, admin_token: {} }}",
                    self.auth.enabled,
                    if self.auth.bearer_token.is_empty() {
                        "<none>"
                    } else {
                        "<set>"
                    },
                    if self.auth.admin_token.is_empty() {
                        "<none>"
                    } else {
                        "<set>"
                    }
                ),
            )
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimiterConfig {
    pub qps_per_key: f64,
    pub burst: u64,
    /// 在切换到另一个 key 前，对单个 key 的 token bucket 等待的最长毫秒数
    ///（默认 200）。0 表示禁用切换——保留现有的无界等待行为。
    #[serde(default = "default_token_wait_ms")]
    pub token_wait_ms: u64,
}

fn default_token_wait_ms() -> u64 {
    200
}

#[derive(Debug, Deserialize, Clone)]
pub struct CooldownConfig {
    pub base_seconds: u64,
    pub max_seconds: u64,
    /// `cool_down_task` 扫描"冷却是否已到期"的独立周期（秒）。
    ///
    /// 此前该任务复用 `health_check.interval_seconds`（默认 30s）作扫描周期，
    /// 这在最坏情况下把一次冷却的恢复延迟整整一个扫描间隔——例如
    /// `base_seconds=60` + 一次刚过 tick 的失败，要等到 *下一个* 30s tick 才
    /// 期发现 `ready_at` 已过，恢复延迟 ≈ 90s。把冷却扫描周期从健康检查周期
    /// 解耦，让冷却恢复延迟可控。0 = 回退到旧行为（用
    /// `health_check.interval_seconds`）。默认 5。可通过
    /// `NVIDIA_COOLDOWN_SCAN_INTERVAL_SECONDS` 覆盖。
    #[serde(default = "default_cooldown_scan_interval_seconds")]
    pub scan_interval_seconds: u64,
}

fn default_cooldown_scan_interval_seconds() -> u64 {
    5
}

/// 代理 → NVIDIA 的重试/上游错误策略。
///
/// 只有 *不带* `Retry-After` 的 401 才会触发对另一个健康 key 的、带指数
/// 退避的重试。每一次这样的失败尝试都会累加该 key 的 `fail_count`；
/// 一旦达到 `auth_fail_threshold`，该 key 被移入 `KeyState::Disabled` 且不再
/// 被获取。必须有一轮后台健康探测看到成功，该 key 才会被重新激活。
///
/// 带 `Retry-After` 的 401，以及不论是否带 `Retry-After` 的 429/403，都
/// *不*重试：它们是确定性的上游决策（按 key 限流 / 地区 / 账号门控），
/// 换个 key 重试只会把整池都烧光。`Retry-After` 值会冷却涉事 key；否则
/// 响应原样返回给客户端不做改动。
#[derive(Debug, Deserialize, Clone)]
pub struct RetryConfig {
    /// 每个客户端请求的最大重试次数（不含首次尝试）。默认 3。
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// 指数退避的基本延迟（毫秒）。默认 200。
    #[serde(default = "default_backoff_base_ms")]
    pub backoff_base_ms: u64,
    /// 单次退避睡眠的上限。默认 2000。
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: u64,
    /// 连续多少次该 key 的上游 auth/forbidden 失败后，该 key 被禁用
    ///（直到一次健康探测恢复它）。默认 3。
    #[serde(default = "default_auth_fail_threshold")]
    pub auth_fail_threshold: u32,
    /// 单个客户端请求因当前 key 的 token bucket 被饿死而切换到另一个
    /// key 的最大次数。与 `max_retries`（上游 401 重试预算）解耦，使
    /// key 切换与按 auth 失败重试可独立调参。默认与 `max_retries` 相同。
    #[serde(default = "default_max_key_switches")]
    pub max_key_switches: u32,
}

fn default_max_retries() -> u32 {
    3
}
fn default_backoff_base_ms() -> u64 {
    200
}
fn default_backoff_max_ms() -> u64 {
    2000
}
fn default_auth_fail_threshold() -> u32 {
    3
}
fn default_max_key_switches() -> u32 {
    default_max_retries()
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff_base_ms: default_backoff_base_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            auth_fail_threshold: default_auth_fail_threshold(),
            max_key_switches: default_max_key_switches(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthCheckConfig {
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
    pub max_consecutive_failures: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObservabilityConfig {
    pub enable_metrics: bool,
    pub metrics_path: String,
    pub log_level: String,
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        // 若存在则加载 .env 文件
        let _ = dotenv();

        // 默认配置。base_url 与 README / 示例值保持一致。
        let mut config = Config {
            listen_addr: "0.0.0.0:8080".to_string(),
            base_url: "https://integrate.api.nvidia.com".to_string(),
            keys: Vec::new(),
            rate_limiter: RateLimiterConfig {
                qps_per_key: 0.65,
                burst: 8,
                token_wait_ms: 0,
            },
            cooldown: CooldownConfig {
                base_seconds: 60,
                max_seconds: 600,
                scan_interval_seconds: default_cooldown_scan_interval_seconds(),
            },
            health_check: HealthCheckConfig {
                interval_seconds: 30,
                timeout_seconds: 5,
                max_consecutive_failures: 3,
            },
            observability: ObservabilityConfig {
                enable_metrics: true,
                metrics_path: "/metrics".to_string(),
                log_level: "info".to_string(),
            },
            request_timeout_seconds: default_request_timeout_seconds(),
            retry: RetryConfig::default(),
            model_aliases: std::collections::HashMap::new(),
            allow_local_base_url: false,
            auth: AuthConfig::default(),
            inject_stream_usage: default_inject_stream_usage(),
            synthesize_empty_choices: default_synthesize_empty_choices(),
            retry_on_empty_shell: default_retry_on_empty_shell(),
            empty_shell_retry_max_tokens: default_empty_shell_retry_max_tokens(),
            empty_shell_retry_attempts: default_empty_shell_retry_attempts(),
            empty_shell_probe_before_retry: default_empty_shell_probe_before_retry(),
            empty_shell_retry_backoff_base_ms: default_empty_shell_retry_backoff_base_ms(),
            empty_shell_retry_backoff_max_ms: default_empty_shell_retry_backoff_max_ms(),
            empty_shell_pre_clamp_max_tokens: default_empty_shell_pre_clamp_max_tokens(),
            empty_shell_no_retry_prompt_tokens: default_empty_shell_no_retry_prompt_tokens(),
            request_max_prompt_tokens: default_request_max_prompt_tokens(),
            est_image_token_cost: default_est_image_token_cost(),
            est_audio_token_per_kb: default_est_audio_token_per_kb(),
            est_audio_token_cap: default_est_audio_token_cap(),
            est_unknown_part_token_cap: default_est_unknown_part_token_cap(),
            circuit_breaker_threshold: default_circuit_breaker_threshold(),
            circuit_breaker_per_model: default_circuit_breaker_per_model(),
            retry_on_503: default_retry_on_503(),
            probe_before_503_retry: default_probe_before_503_retry(),
            retry_on_500: default_retry_on_500(),
            max_tokens_max: default_max_tokens_max(),
            concurrency_limit_per_model: std::collections::HashMap::new(),
            concurrency_queue_timeout_ms: default_concurrency_queue_timeout_ms(),
            request_timeout_streaming_seconds: default_request_timeout_streaming_seconds(),
            upstream_connect_timeout_secs: default_upstream_connect_timeout_secs(),
            upstream_pool_idle_timeout_secs: default_upstream_pool_idle_timeout_secs(),
            retry_jitter_ms: default_retry_jitter_ms(),
        };

        // 优先用 NVIDIA_CONFIG_PATH，回退到 ./config.toml
        let config_path =
            std::env::var("NVIDIA_CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

        if let Ok(content) = fs::read_to_string(&config_path) {
            let toml_config: Config = toml::from_str(&content)?;
            config.listen_addr = toml_config.listen_addr;
            config.base_url = toml_config.base_url;
            if !toml_config.keys.is_empty() {
                config.keys = toml_config.keys;
            }
            config.rate_limiter = toml_config.rate_limiter;
            config.cooldown = toml_config.cooldown;
            config.health_check = toml_config.health_check;
            config.observability = toml_config.observability;
            config.request_timeout_seconds = toml_config.request_timeout_seconds;
            config.retry = toml_config.retry;
            config.allow_local_base_url = toml_config.allow_local_base_url;
            config.auth = toml_config.auth;
            // 合并 model_aliases：TOML 中定义的别名优先，但我们对
            // 默认集合是 *新增*（而非替换），这样调用方在常见情况下
            // 是扩展而非覆盖。
            for (k, v) in toml_config.model_aliases.into_iter() {
                config.model_aliases.insert(k, v);
            }
        }

        // 从环境变量覆盖（NVIDIA_*）
        Self::apply_env_overrides(&mut config);

        config.validate()?;

        if config.keys.is_empty() {
            return Err(
                "No API keys configured. Set `keys` in config.toml or NVIDIA_KEYS env var.".into(),
            );
        }

        Ok(config)
    }

    /// 在所有 override 都应用之后校验配置值。拦截那些否则会在运行时
    /// 表现为 401/404 失败的 placeholder/fake 值。
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        // base_url 必须是真实端点，而不是默认的 example 域名或某个
        // loopback 地址。在生产里用 example.com/localhost 意味着每个
        // 上游请求都会 404/401，把重试预算白白烧掉。
        if self.base_url.is_empty() {
            return Err("base_url must not be empty".into());
        }
        let base_lower = self.base_url.to_ascii_lowercase();
        if !base_lower.starts_with("http://") && !base_lower.starts_with("https://") {
            return Err(format!(
                "base_url '{}' must start with http:// or https://",
                self.base_url
            )
            .into());
        }
        if !self.allow_local_base_url {
            // 按 *authority host* 拒绝开发/占位主机，而不是按子串——
            // 子串匹配会误拒那些只是恰好包含 "example.com" 或 "localhost"
            // 的合法域名（如 `https://api.example-cloud.nvidia.com`）。
            // 我们自己解析出 host 部分，以免引入 URL crate。
            if let Some(host) = host_of_url(&self.base_url) {
                let host_lower = host.to_ascii_lowercase();
                if host_lower == "localhost"
                    || host_lower == "example.com"
                    || host_lower == "127.0.0.1"
                    || host_lower == "::1"
                {
                    return Err(format!(
                        "base_url '{}' points at a placeholder/development host ({}); \
                         set a real NVIDIA API base URL",
                        self.base_url, host_lower
                    )
                    .into());
                }
            }
        }

        // 启动前拒绝明显的 fake/test key。日志里出现过 fake***-key /
        // dumm***_key / cursor-dummy-key 导致 401；在这里快速失败可避免
        // 在一个坏掉的 key pool 上静默运行。
        if self.keys.is_empty() {
            return Err(
                "No API keys configured. Set `keys` in config.toml or NVIDIA_KEYS env var.".into(),
            );
        }
        for key in &self.keys {
            if key.len() < 8 {
                return Err(format!(
                    "API key '{}' is too short (must be at least 8 characters)",
                    mask_key(key)
                )
                .into());
            }
            let key_lower = key.to_ascii_lowercase();
            if key_lower.contains("fake")
                || key_lower.contains("dummy")
                || key_lower.contains("test")
                || key_lower.contains("example")
                || key_lower.contains("placeholder")
            {
                return Err(format!(
                    "API key '{}' looks like a placeholder/fake key; \
                     provide real NVIDIA API keys",
                    mask_key(key)
                )
                .into());
            }
        }

        // 若单请求超时短到会撞上 LLM 正常响应延迟，则告警。
        // failure.log 里出现过 chat completions 上 timeout=1s，几乎可肯定是
        // 误配置。
        if self.request_timeout_seconds > 0 && self.request_timeout_seconds < 5 {
            warn!(
                "request_timeout_seconds={} 过短；非流式请求建议 30 秒以上",
                self.request_timeout_seconds
            );
        }
        // 若配置了流式超时但过短，则告警
        if self.request_timeout_streaming_seconds > 0 && self.request_timeout_streaming_seconds < 5 {
            warn!(
                "request_timeout_streaming_seconds={} 过短；流式建议 60 秒以上",
                self.request_timeout_streaming_seconds
            );
        }

        // 硬 prompt 软上限（`empty_shell_no_retry_prompt_tokens`，413 以下的软不重试
        // 阈值）与硬上限（`request_max_prompt_tokens`，413 入口拒绝）必须**相等**——
        // 两者之间任何灰带都会复活 forbidden 灰带（见
        // `default_request_max_prompt_tokens` 文档里两轮实测：[90k,95k) 的灰带让 32/32
        // /compact 空壳、客户端重试约 10 分钟）。这里只在"硬上限严格高于软不重试阈值"
        // 时 warn——逆向默认就是两者相等。不阻断启动（运维可能在调试期主动留灰带）。
        if self.request_max_prompt_tokens > 0
            && self.empty_shell_no_retry_prompt_tokens > 0
            && self.request_max_prompt_tokens > self.empty_shell_no_retry_prompt_tokens
        {
            warn!(
                "request_max_prompt_tokens={} > empty_shell_no_retry_prompt_tokens={}；\
                 硬上限高于软不重试阈值会在其间复活「打一发就过但注定空壳 + 误导 502」的灰带。\
                 建议二者相等（默认都 90000）。",
                self.request_max_prompt_tokens, self.empty_shell_no_retry_prompt_tokens
            );
        }
        // 反向灰带：`request_max < no_retry`。软不重试阈值的语义是"prompt 超过这个
        // 就不再耗一次重试"；硬上限是"超过这个就在入口直接 413 拒绝"。如果硬上限
        // 严格低于软阈值，那么凡是超过硬上限的请求都被 413 拒绝在门外、根本到
        // 不了 `no_retry` 判定——`empty_shell_no_retry_prompt_tokens` 实际上是个死
        // 值、永不生效。它不会复活 forbidden 灰带（更小的硬上限是保守的），但启
        // 用的 `retry_on_empty_shell` 在硬上限以内的合法请求上仍按 `no_retry` 截断
        // 是无意义的。运维多半是调整其中一个忘了同步另一个；用 warn 提醒，不打
        // 断启动（值合法、只是配置意图与生效行为不符）。
        if self.request_max_prompt_tokens > 0
            && self.empty_shell_no_retry_prompt_tokens > 0
            && self.request_max_prompt_tokens < self.empty_shell_no_retry_prompt_tokens
        {
            warn!(
                "request_max_prompt_tokens={} < empty_shell_no_retry_prompt_tokens={}；\
                 硬上限低于软不重试阈值，令 `empty_shell_no_retry_prompt_tokens` 成为死值——\
                 超过硬上限的请求在入口被 413 拦下、到不了 no_retry 判定。建议二者相等。",
                self.request_max_prompt_tokens, self.empty_shell_no_retry_prompt_tokens
            );
        }

        // 前门鉴权开启却没配 bearer_token：见 `validate_auth` 的完整说明。
        // 一并入 `validate` 让 `Config::load` 自动 fail-fast。
        self.validate_auth()?;

        // ── 数值边界（M-tier）──────────────────────────────────────────────
        // 这些都是会让代理在真实流量下"看起来在跑、其实黏死"的静默 misconfig：
        // 把它们从 warn 升为 Err，避免运维以为是别的原因。
        //
        // `cooldown.base_seconds == 0`：`mark_bad(None)` 会立刻 `ready_at = now +
        // 0` → 下一轮 `acquire` 立刻又选它，坏 key 永远不会真正冷却，对端限流/
        // 403 会被无脑循环。允许 0 是把"冷却被禁用"语义悄悄混进来，不值得。
        if self.cooldown.base_seconds == 0 {
            return Err(
                "cooldown.base_seconds must be > 0 (0 makes mark_bad effect a no-op → \
                 bad keys never really cool, retry loop burns the upstream).".into(),
            );
        }
        // `max_seconds < base_seconds`：`mark_bad` 里有 `delay =
        // retry_after.min(max)`，max < base 时会把上游要求冷却 600s 的请求夹成
        // 几秒，违背退避请求。允许时上游反向打脸，挂载期回不可用。允许相等。
        if self.cooldown.max_seconds < self.cooldown.base_seconds {
            return Err(format!(
                "cooldown.max_seconds={} must be >= cooldown.base_seconds={} \
                 (smaller max silently overrides longer upstream Retry-After).",
                self.cooldown.max_seconds, self.cooldown.base_seconds
            )
            .into());
        }
        // `rate_limiter.burst == 0` 且 qps>0：桶容量 0 → 任何 `try_consume(1)` 都
        // `Err(NotEnough)`，每请求都走 `consume_with_timeout` 等到超时再返回错误。
        // 唯一合法的"不限流"形态是 `qps_per_key = 0`（见 make_handle）。
        if self.rate_limiter.burst == 0 && self.rate_limiter.qps_per_key > 0.0 {
            return Err(
                "rate_limiter.burst must be > 0 when qps_per_key > 0 (burst=0 starves every \
                 request; set qps_per_key=0 to actually disable rate limiting).".into(),
            );
        }
        // `qps_per_key` 必须 finite 且有限正数（或 0）。NaN/Inf 在 token 桶的浮点
        // 算术里会让 `store_approx`、`refill_locked` 的 `.min()` / 比较 NaN 假 [],
        // 节流路径退化为永远 Err 或永不补充。
        if !self.rate_limiter.qps_per_key.is_finite() || self.rate_limiter.qps_per_key < 0.0 {
            return Err(format!(
                "rate_limiter.qps_per_key={} must be a finite non-negative number \
                 (NaN/Inf/- breaks token-bucket arithmetic; 0 disables per-key limiting).",
                self.rate_limiter.qps_per_key
            )
            .into());
        }

        info!("配置校验通过");
        Ok(())
    }

    /// 专门针对鉴权配置的校验，可独立于其它校验单独调用。当前唯一不变式：
    /// 开启前门鉴权（`auth.enabled = true`）时必须配 `bearer_token`，否则任何
    /// 发出 `Authorization: Bearer `（空）或根本没有该头的请求都通过——把一个
    /// 应被锁的代理变成开放。空密钥而鉴权开启是 footgun 不是 feature，故
    /// 阻断启动。`Config::validate` 也会调用它，故 `Config::load` 自动覆盖。
    pub fn validate_auth(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.auth.enabled && self.auth.bearer_token.trim().is_empty() {
            return Err(
                "auth.enabled = true 但 bearer_token 为空。开启前门鉴权必须设置 \
                 auth.bearer_token（或 NVIDIA_AUTH_BEARER_TOKEN）；空 token 会让 \
                 \"Authorization: Bearer \"（空）通过校验，等于把代理开放。"
                    .into(),
            );
        }
        Ok(())
    }

    /// 把一个 NVIDIA_* 环境变量 override 解析进目标字段，失败时告警，
    /// 而非静默丢弃。把坏值留在日志里，可让运维发现一个原本很难察觉的
    /// 手误（如 `NVIDIA_REQUEST_MAX_PROMPT_TOKENS=90kk`）——否则
    /// override 会被忽略，默认/已配置值静默保留，此前这让该操作看起来像
    /// 一次没有任何诊断的无操作。
    ///
    /// 返回 `true` = override 成功应用；`false` = 解析失败（已 warn）。调用方
    ///（`apply_env_overrides`）用它收集"已生效 override"清单，在启动时 info 一行，
    /// 让运维能确认一堆 `NVIDIA_*` 旋钮里到底哪些命中了——解析失败的那条已有 warn，
    /// 但解析成功一直是静默的，此前 40+ 旋钮下难以确认 override 是否生效。
    fn env_parse_into<T: std::str::FromStr>(key: &str, value: &str, target: &mut T) -> bool
    where
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        match value.parse::<T>() {
            Ok(v) => {
                *target = v;
                true
            }
            Err(e) => {
                warn!(
                    env = key,
                    raw = value,
                    error = ?e,
                    "忽略格式错误的 NVIDIA_* override；使用已配置/默认值",
                );
                false
            }
        }
    }

    /// 与 `env_parse_into` 相同，但用于可 serde 反序列化的 override
    ///（两个 `*_per_model` JSON-map 字段）。解析错误的告警用 `Display`，
    /// 因为 serde_json 的错误在 Debug-friendly 上并不一致。
    fn env_json_into<T: serde::de::DeserializeOwned>(key: &str, value: &str, target: &mut T) -> bool {
        match serde_json::from_str::<T>(value) {
            Ok(v) => {
                *target = v;
                true
            }
            Err(e) => {
                warn!(
                    env = key,
                    raw = value,
                    error = %e,
                    "忽略格式错误的 NVIDIA_* JSON override；使用已配置/默认值",
                );
                false
            }
        }
    }

    /// 应用所有 `NVIDIA_*` 环境变量 override，并在结束时输出一条 info，列出
    /// **实际生效**的 override 名（不含 `NVIDIA_` 前缀的小写形）。注意：一个
    /// `NVIDIA_*` 变量是否被认得取决于下面 match 中的分支；只被覆盖到目标字段、
    /// 且解析成功，才算"命中"。不在其列的 `NVIDIA_*` 变量名（拼错、或本版本尚未
    /// 接线的字段）不会被列入，也不 warn——它们仍是无操作，保留运维自行核对。
    /// 单条解析失败已由上面的 helper warn；这里只汇总成功的那批。
    fn apply_env_overrides(config: &mut Config) {
        // 已生效 override 名（小写、不含 `NVIDIA_` 前缀）。仅在解析/赋值成功后 push。
        let mut applied: Vec<String> = Vec::new();
        for (key, value) in std::env::vars() {
            if !key.starts_with("NVIDIA_") {
                continue;
            }
            let key = &key["NVIDIA_".len()..];
            let key_lower = key.to_ascii_lowercase();
            // 用本地宏减少噪声：在解析型分支里写 `record!(Self::env_parse_into(...))`
            // 就能"调用 + 命中即记"。字符串赋值型分支直接 `applied.push(key_lower.clone())`。
            macro_rules! record {
                ($ok:expr) => {{
                    let ok = $ok;
                    if ok {
                        applied.push(key_lower.clone());
                    }
                }};
            }
            match key_lower.as_str() {
                "listen_addr" => {
                    // SocketAddr 不是直接存在结构体里（存的是 String），
                    // 所以先 parse 再 stringify；格式错误时告警，让
                    // NVIDIA_LISTEN_ADDR 里的手误足够显眼，而不是一次无操作。
                    match value.parse::<SocketAddr>() {
                        Ok(addr) => {
                            config.listen_addr = addr.to_string();
                            applied.push(key_lower.clone());
                        }
                        Err(e) => warn!(
                            env = key,
                            raw = value,
                            error = ?e,
                            "忽略格式错误的 NVIDIA_* override；使用已配置/默认值",
                        ),
                    }
                }
                "base_url" => {
                    config.base_url = value;
                    applied.push(key_lower.clone());
                }
                // keys：逗号分隔的列表
                "keys" => {
                    config.keys = value
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    applied.push(key_lower.clone());
                }
                "rate_limiter_qps_per_key" => record!(Self::env_parse_into(key, &value, &mut config.rate_limiter.qps_per_key)),
                "rate_limiter_burst" => record!(Self::env_parse_into(key, &value, &mut config.rate_limiter.burst)),
                "rate_limiter_token_wait_ms" => record!(Self::env_parse_into(key, &value, &mut config.rate_limiter.token_wait_ms)),
                "cooldown_base_seconds" => record!(Self::env_parse_into(key, &value, &mut config.cooldown.base_seconds)),
                "cooldown_max_seconds" => record!(Self::env_parse_into(key, &value, &mut config.cooldown.max_seconds)),
                "cooldown_scan_interval_seconds" => record!(Self::env_parse_into(key, &value, &mut config.cooldown.scan_interval_seconds)),
                "health_check_interval_seconds" => record!(Self::env_parse_into(key, &value, &mut config.health_check.interval_seconds)),
                "health_check_timeout_seconds" => record!(Self::env_parse_into(key, &value, &mut config.health_check.timeout_seconds)),
                "health_check_max_consecutive_failures" => record!(Self::env_parse_into(key, &value, &mut config.health_check.max_consecutive_failures)),
                "observability_enable_metrics" => record!(Self::env_parse_into(key, &value, &mut config.observability.enable_metrics)),
                "observability_metrics_path" => {
                    config.observability.metrics_path = value;
                    applied.push(key_lower.clone());
                }
                "observability_log_level" => {
                    config.observability.log_level = value;
                    applied.push(key_lower.clone());
                }
                "request_timeout_seconds" => record!(Self::env_parse_into(key, &value, &mut config.request_timeout_seconds)),
                "retry_max_retries" => record!(Self::env_parse_into(key, &value, &mut config.retry.max_retries)),
                "retry_backoff_base_ms" => record!(Self::env_parse_into(key, &value, &mut config.retry.backoff_base_ms)),
                "retry_backoff_max_ms" => record!(Self::env_parse_into(key, &value, &mut config.retry.backoff_max_ms)),
                "retry_auth_fail_threshold" => record!(Self::env_parse_into(key, &value, &mut config.retry.auth_fail_threshold)),
                "retry_max_key_switches" => record!(Self::env_parse_into(key, &value, &mut config.retry.max_key_switches)),
                "allow_local_base_url" => record!(Self::env_parse_into(key, &value, &mut config.allow_local_base_url)),
                "auth_enabled" => record!(Self::env_parse_into(key, &value, &mut config.auth.enabled)),
                "auth_bearer_token" => {
                    config.auth.bearer_token = value;
                    applied.push(key_lower.clone());
                }
                "auth_admin_token" => {
                    config.auth.admin_token = value;
                    applied.push(key_lower.clone());
                }
                "inject_stream_usage" => record!(Self::env_parse_into(key, &value, &mut config.inject_stream_usage)),
                "synthesize_empty_choices" => record!(Self::env_parse_into(key, &value, &mut config.synthesize_empty_choices)),
                "retry_on_empty_shell" => record!(Self::env_parse_into(key, &value, &mut config.retry_on_empty_shell)),
                "empty_shell_retry_max_tokens" => record!(Self::env_parse_into(key, &value, &mut config.empty_shell_retry_max_tokens)),
                "empty_shell_retry_attempts" => record!(Self::env_parse_into(key, &value, &mut config.empty_shell_retry_attempts)),
                "empty_shell_probe_before_retry" => record!(Self::env_parse_into(key, &value, &mut config.empty_shell_probe_before_retry)),
                "circuit_breaker_threshold" => record!(Self::env_parse_into(key, &value, &mut config.circuit_breaker_threshold)),
                "circuit_breaker_per_model" => record!(Self::env_parse_into(key, &value, &mut config.circuit_breaker_per_model)),
                "empty_shell_retry_backoff_base_ms" => record!(Self::env_parse_into(key, &value, &mut config.empty_shell_retry_backoff_base_ms)),
                "empty_shell_retry_backoff_max_ms" => record!(Self::env_parse_into(key, &value, &mut config.empty_shell_retry_backoff_max_ms)),
                "empty_shell_pre_clamp_max_tokens" => record!(Self::env_json_into(key, &value, &mut config.empty_shell_pre_clamp_max_tokens)),
                "empty_shell_no_retry_prompt_tokens" => record!(Self::env_parse_into(key, &value, &mut config.empty_shell_no_retry_prompt_tokens)),
                "request_max_prompt_tokens" => record!(Self::env_parse_into(key, &value, &mut config.request_max_prompt_tokens)),
                "est_image_token_cost" => record!(Self::env_parse_into(key, &value, &mut config.est_image_token_cost)),
                "est_audio_token_per_kb" => record!(Self::env_parse_into(key, &value, &mut config.est_audio_token_per_kb)),
                "est_audio_token_cap" => record!(Self::env_parse_into(key, &value, &mut config.est_audio_token_cap)),
                "est_unknown_part_token_cap" => record!(Self::env_parse_into(key, &value, &mut config.est_unknown_part_token_cap)),
                "max_tokens_max" => record!(Self::env_parse_into(key, &value, &mut config.max_tokens_max)),
                "retry_on_503" => record!(Self::env_parse_into(key, &value, &mut config.retry_on_503)),
                "probe_before_503_retry" => record!(Self::env_parse_into(key, &value, &mut config.probe_before_503_retry)),
                "retry_on_500" => record!(Self::env_parse_into(key, &value, &mut config.retry_on_500)),
                "request_timeout_streaming_seconds" => record!(Self::env_parse_into(key, &value, &mut config.request_timeout_streaming_seconds)),
                "concurrency_limit_per_model" => record!(Self::env_json_into(key, &value, &mut config.concurrency_limit_per_model)),
                "concurrency_queue_timeout_ms" => record!(Self::env_parse_into(key, &value, &mut config.concurrency_queue_timeout_ms)),
                "upstream_connect_timeout_secs" => record!(Self::env_parse_into(key, &value, &mut config.upstream_connect_timeout_secs)),
                "upstream_pool_idle_timeout_secs" => record!(Self::env_parse_into(key, &value, &mut config.upstream_pool_idle_timeout_secs)),
                "retry_jitter_ms" => record!(Self::env_parse_into(key, &value, &mut config.retry_jitter_ms)),
                _ => {}
            }
        }
        if !applied.is_empty() {
            // 稳定排序，使同一环境在不同次启动里输出一致、便于 diff 日志。
            applied.sort();
            info!(applied = ?applied, "已生效的 NVIDIA_* env override");
        }
    }
}

#[cfg(test)]
mod config_validation_tests {
    use super::*;

    fn base_valid_config() -> Config {
        Config {
            listen_addr: "0.0.0.0:8888".to_string(),
            base_url: "https://integrate.api.nvidia.com".to_string(),
            keys: vec!["nvapi-realvalidkey1234567890".to_string()],
            rate_limiter: RateLimiterConfig {
                qps_per_key: 0.65,
                burst: 8,
                token_wait_ms: 200,
            },
            cooldown: CooldownConfig {
                base_seconds: 60,
                max_seconds: 600,
                scan_interval_seconds: 5,
            },
            health_check: HealthCheckConfig {
                interval_seconds: 30,
                timeout_seconds: 5,
                max_consecutive_failures: 3,
            },
            observability: ObservabilityConfig {
                enable_metrics: true,
                metrics_path: "/metrics".to_string(),
                log_level: "info".to_string(),
            },
            request_timeout_seconds: 60,
            request_timeout_streaming_seconds: 0,
            retry: RetryConfig::default(),
            model_aliases: std::collections::HashMap::new(),
            allow_local_base_url: false,
            auth: AuthConfig::default(),
            inject_stream_usage: true,
            synthesize_empty_choices: true,
            retry_on_empty_shell: true,
            empty_shell_retry_max_tokens: 8192,
            empty_shell_retry_attempts: 2,
            empty_shell_probe_before_retry: true,
            empty_shell_retry_backoff_base_ms: 500,
            empty_shell_retry_backoff_max_ms: 5000,
            empty_shell_pre_clamp_max_tokens: default_empty_shell_pre_clamp_max_tokens(),
            empty_shell_no_retry_prompt_tokens: default_empty_shell_no_retry_prompt_tokens(),
            request_max_prompt_tokens: default_request_max_prompt_tokens(),
            est_image_token_cost: default_est_image_token_cost(),
            est_audio_token_per_kb: default_est_audio_token_per_kb(),
            est_audio_token_cap: default_est_audio_token_cap(),
            est_unknown_part_token_cap: default_est_unknown_part_token_cap(),
            circuit_breaker_threshold: 3,
            circuit_breaker_per_model: true,
            retry_on_503: true,
            probe_before_503_retry: true,
            retry_on_500: true,
            max_tokens_max: 100_000,
            concurrency_limit_per_model: std::collections::HashMap::new(),
            concurrency_queue_timeout_ms: 5000,
            upstream_connect_timeout_secs: 5,
            upstream_pool_idle_timeout_secs: 90,
            retry_jitter_ms: 100,
        }
    }

    #[test]
    fn valid_config_passes() {
        let cfg = base_valid_config();
        assert!(cfg.validate().is_ok(), "valid config should pass");
    }

    #[test]
    fn empty_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn example_com_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "http://example.com".to_string();
        let err = cfg.validate().expect_err("should fail");
        let msg = err.to_string();
        assert!(msg.contains("placeholder"), "error should mention placeholder: {}", msg);
    }

    #[test]
    fn localhost_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "http://127.0.0.1:8080".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn missing_scheme_base_url_fails() {
        let mut cfg = base_valid_config();
        cfg.base_url = "integrate.api.nvidia.com".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn fake_key_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec!["fake-key-1234567890".to_string()];
        let err = cfg.validate().expect_err("should fail");
        assert!(err.to_string().contains("placeholder"));
    }

    #[test]
    fn dummy_key_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec!["cursor-dummy-key".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn short_key_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec!["nvapi-1".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_keys_fails() {
        let mut cfg = base_valid_config();
        cfg.keys = vec![];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn equal_prompt_ceilings_passes_silently() {
        // 两者相等（默认形态）—— validate 绝不能 err。
        let cfg = base_valid_config();
        assert_eq!(
            cfg.request_max_prompt_tokens,
            cfg.empty_shell_no_retry_prompt_tokens,
            "base fixture 应保持默认两者相等"
        );
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn hard_ceiling_above_soft_no_retry_warns_but_passes() {
        // 硬上限严格高于软不重试阈值——复活灰带。validate 不阻断（只 warn），
        // 但仍要 ok（运维可能在调试期主动留灰带）。
        let mut cfg = base_valid_config();
        cfg.request_max_prompt_tokens = 100_000;
        cfg.empty_shell_no_retry_prompt_tokens = 90_000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn soft_below_hard_when_equal_is_fine() {
        // 软 == 硬，或硬低于软都不触发告警分支（本测试只覆盖等号路径）。
        let mut cfg = base_valid_config();
        cfg.request_max_prompt_tokens = 90_000;
        cfg.empty_shell_no_retry_prompt_tokens = 90_000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn disabled_ceilings_skip_gray_zone_check() {
        // 任一为 0（关闭）时跳过该告警分支，validate 仍 ok。
        let mut cfg = base_valid_config();
        cfg.request_max_prompt_tokens = 0;
        cfg.empty_shell_no_retry_prompt_tokens = 90_000;
        assert!(cfg.validate().is_ok());

        let mut cfg = base_valid_config();
        cfg.request_max_prompt_tokens = 100_000;
        cfg.empty_shell_no_retry_prompt_tokens = 0;
        assert!(cfg.validate().is_ok());
    }

    // ── M-tier 数值边界校验测试 ──────────────────────────────────────────

    #[test]
    fn cooldown_base_seconds_zero_fails() {
        // base_seconds = 0 让 mark_bad 的 ready_at 立刻到期 → 坏 key 即时复活，
        // 冷却实质禁用。必须 fail-fast。
        let mut cfg = base_valid_config();
        cfg.cooldown.base_seconds = 0;
        assert!(
            cfg.validate().is_err(),
            "cooldown.base_seconds=0 必须被校验拒绝"
        );
    }

    #[test]
    fn cooldown_max_below_base_fails() {
        // max<base 会让上游要求冷却 600s 的请求被夹成几秒，违反退避。
        let mut cfg = base_valid_config();
        cfg.cooldown.base_seconds = 60;
        cfg.cooldown.max_seconds = 10;
        let err = cfg.validate().expect_err("max<base must fail");
        assert!(err.to_string().contains("must be >="));
    }

    #[test]
    fn cooldown_max_eq_base_passes() {
        // 相等合法（上游 Retry-After 与我们的 base 一致）。
        let mut cfg = base_valid_config();
        cfg.cooldown.base_seconds = 60;
        cfg.cooldown.max_seconds = 60;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rate_limiter_burst_zero_with_positive_qps_fails() {
        // burst=0 + qps>0 → 桶容量 0，每请求都超时。必须失败。
        let mut cfg = base_valid_config();
        cfg.rate_limiter.burst = 0;
        cfg.rate_limiter.qps_per_key = 1.0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rate_limiter_burst_zero_with_qps_zero_passes() {
        // qps=0 = 关闭限流（make_handle 将其构造成正无穷补充）；burst 也
        // 不参与算术，故该组合合法。
        let mut cfg = base_valid_config();
        cfg.rate_limiter.qps_per_key = 0.0;
        cfg.rate_limiter.burst = 0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rate_limiter_qps_nan_fails() {
        // NaN / Inf 会让 token 桶浮点比较退化；必须失败。
        let mut cfg = base_valid_config();
        cfg.rate_limiter.qps_per_key = f64::NAN;
        assert!(cfg.validate().is_err(), "NaN qps must fail");

        let mut cfg2 = base_valid_config();
        cfg2.rate_limiter.qps_per_key = f64::INFINITY;
        assert!(cfg2.validate().is_err(), "Inf qps must fail");

        let mut cfg3 = base_valid_config();
        cfg3.rate_limiter.qps_per_key = -1.0;
        assert!(cfg3.validate().is_err(), "negative qps must fail");
    }

    // ── M-tier IPv6 loopback host 修复 ───────────────────────────────────

    #[test]
    fn ipv6_loopback_base_url_fails() {
        // 此前的 bug：host_of_url 返回 "[::1]"（带方括号），与 validate 的 "host_lower == "::1""
        // 比对永不命中，IPv6 loopback 的 base_url 被门禁放行。
        let mut cfg = base_valid_config();
        cfg.base_url = "http://[::1]:8080".to_string();
        let err = cfg.validate().expect_err("[::1] loopback must fail");
        assert!(err.to_string().contains("placeholder"), "got: {err}");
    }

    #[test]
    fn host_of_url_strips_ipv6_brackets() {
        assert_eq!(host_of_url("http://[::1]:8080/v1").as_deref(), Some("::1"));
        assert_eq!(host_of_url("http://[::1]/v1").as_deref(), Some("::1"));
        // 仍能剥 IPv4 + port。
        assert_eq!(host_of_url("http://127.0.0.1:8080/v1").as_deref(), Some("127.0.0.1"));
        assert_eq!(host_of_url("https://api.nvidia.com/v1").as_deref(), Some("api.nvidia.com"));
    }
}
