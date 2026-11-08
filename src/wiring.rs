//! Provider wiring for the real-model suite: where the API calls go.
//!
//! Credentials are NOT read from other tools' stores. The caller exports
//! `ANTHROPIC_AUTH_TOKEN` — the same variable claude itself consumes, and the
//! standard CI-secret shape — and this module only maps the provider shorthand
//! to its public Anthropic-compatible endpoint (claude speaks the Anthropic
//! protocol, so the provider only has to accept it).

/// Anthropic-compatible endpoint + default model per provider.
pub fn endpoint_for(provider: &str) -> Option<(&'static str, &'static str)> {
    match provider {
        // Official DeepSeek <-> Claude Code integration.
        "deepseek" => Some(("https://api.deepseek.com/anthropic", "deepseek-flash")),
        // Zhipu's Z.ai gateway speaks the Anthropic protocol with GLM models.
        "zai" | "zhipu-coding-plan" => {
            Some(("https://open.bigmodel.cn/api/anthropic", "glm-5.3-flash"))
        }
        _ => None,
    }
}
