//! One configurable retry policy; adapters only classify failures.
use super::*;
pub use symbiotic_ai_runtime::model::item_stream::ErrorClass;

/// Action after a classified failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryAction {
    /// Wait on the shared account, then resend or continue.
    RetrySame,
    /// Try the next configured route's normal admission.
    Switch,
    /// Return the existing fixed response or original stream error.
    Fail,
}
/// Exponential backoff uses the shared retry implementation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryBackoff {
    /// Override the runtime's base delay; absent uses its account setting.
    #[serde(default)]
    pub base_delay_ms: Option<u64>,
}
/// Policy for one failure class. Attempts include the first send.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryRule {
    /// Retry, switch route, or fail.
    pub action: RetryAction,
    /// Absent means unlimited attempts.
    #[serde(default)]
    pub max_attempts: Option<u32>,
    /// Backoff parameters; Retry-After takes precedence.
    #[serde(default)]
    pub backoff: RetryBackoff,
}
/// Sparse overrides of the four default class rules.
pub type RetryPolicy = HashMap<ErrorClass, RetryRule>;
/// Context supplied to the embedded-only policy hook.
pub struct RetryContext {
    /// Adapter classification, after route extensions.
    pub class: ErrorClass,
    /// Provider code or the fixed network/disconnect code.
    pub code: String,
    /// Current request's send attempt, starting at one.
    pub attempt: u32,
    /// Whether the downstream response has started.
    pub bytes_reached_caller: bool,
    /// Current route name.
    pub route_name: String,
}
/// One hook-selected rule, using the same policy engine and backoff.
pub type RetryDecision = RetryRule;
/// Embedded-only retry hook. None delegates to the table; never serializable.
pub type RetryHook = Arc<dyn Fn(&RetryContext) -> Option<RetryDecision> + Send + Sync>;

pub(super) fn rule(policy: &RetryPolicy, class: ErrorClass) -> RetryRule {
    policy.get(&class).cloned().unwrap_or_else(|| RetryRule {
        action: if class == ErrorClass::Final {
            RetryAction::Fail
        } else {
            RetryAction::RetrySame
        },
        max_attempts: if class == ErrorClass::Server {
            Some(3)
        } else {
            None
        },
        backoff: Default::default(),
    })
}
pub(super) fn valid(policy: &RetryPolicy) -> bool {
    policy
        .values()
        .all(|r| r.max_attempts != Some(0) && r.backoff.base_delay_ms != Some(0))
}
pub(super) fn decision(
    policy: &RetryPolicy,
    hook: Option<&RetryHook>,
    context: &RetryContext,
) -> RetryDecision {
    let mut r = hook
        .and_then(|h| h(context))
        .unwrap_or_else(|| rule(policy, context.class));
    if r.max_attempts.is_some_and(|max| context.attempt >= max) {
        r.action = RetryAction::Fail;
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_table_defaults_overrides_hook_and_attempt_limit() {
        let mut context = RetryContext {
            class: ErrorClass::Throttle,
            code: "429".into(),
            attempt: 1,
            bytes_reached_caller: false,
            route_name: "responses".into(),
        };
        let mut policy = RetryPolicy::new();
        for (class, action, max) in [
            (ErrorClass::Throttle, RetryAction::RetrySame, None),
            (ErrorClass::Network, RetryAction::RetrySame, None),
            (ErrorClass::Server, RetryAction::RetrySame, Some(3)),
            (ErrorClass::Final, RetryAction::Fail, None),
        ] {
            context.class = class;
            let actual = decision(&policy, None, &context);
            assert_eq!((actual.action, actual.max_attempts), (action, max));
        }
        context.class = ErrorClass::Server;
        context.attempt = 3;
        assert_eq!(decision(&policy, None, &context).action, RetryAction::Fail);
        policy.insert(
            ErrorClass::Server,
            RetryRule {
                action: RetryAction::Switch,
                max_attempts: None,
                backoff: Default::default(),
            },
        );
        assert_eq!(
            decision(&policy, None, &context).action,
            RetryAction::Switch
        );
        let hook: RetryHook = Arc::new(|c| {
            if c.bytes_reached_caller {
                Some(RetryRule {
                    action: RetryAction::Fail,
                    max_attempts: None,
                    backoff: Default::default(),
                })
            } else {
                None
            }
        });
        assert_eq!(
            decision(&policy, Some(&hook), &context).action,
            RetryAction::Switch
        );
        context.bytes_reached_caller = true;
        assert_eq!(
            decision(&policy, Some(&hook), &context).action,
            RetryAction::Fail
        );
    }
}
