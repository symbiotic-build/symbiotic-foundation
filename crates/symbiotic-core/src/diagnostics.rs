//! Closed diagnostic vocabulary. Codes never retain input or error text.
use serde::{Deserialize, Serialize};

macro_rules! closed_codes {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => ($code:literal, $message:literal),)* }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name {
            $(#[doc = $message] #[serde(rename = $code)] $variant,)*
        }
        impl $name {
            /// Static human-readable diagnostic.
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $message,)* }
            }
            /// Stable code stored in durable records.
            pub const fn code(self) -> &'static str {
                match self { $(Self::$variant => $code,)* }
            }
            /// Refuse unknown stored codes without retaining their text.
            pub fn parse(code: &str) -> Option<Self> {
                match code { $($code => Some(Self::$variant),)* _ => None }
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str { self.as_str() }
        }
    };
}

closed_codes! {
    /// A compile-time diagnostic, safe for errors, logs and durable failures.
    DiagnosticCode {
        AClassifyRequestNeedsAtLeastOneQuestion => ("a_classify_request_needs_at_least_one_question", "a classify request needs at least one question"),
        AdapterBoundsDifferFromConfiguredBinding => ("adapter_bounds_differ_from_configured_binding", "adapter bounds differ from configured binding"),
        AdapterCapabilitiesDifferFromConfiguredModel => ("adapter_capabilities_differ_from_configured_model", "adapter capabilities differ from configured model"),
        BindingDiffersFromConfiguredIdentityModel => ("binding_differs_from_configured_identity_model", "binding differs from configured identity/model"),
        BindingOverridesConfiguredAccountPolicy => ("binding_overrides_configured_account_policy", "binding overrides configured account policy"),
        BindingRequiresAnExplicitExecutionPolicyOrConfiguredRegistry => ("binding_requires_an_explicit_execution_policy_or_configured_registry", "binding requires an explicit execution policy or configured registry"),
        ClassificationQuestionsCannotBeEncoded => ("classification_questions_cannot_be_encoded", "classification questions cannot be encoded"),
        ConfiguredCredentialModeIsUnsupportedOrEmpty => ("configured_credential_mode_is_unsupported_or_empty", "configured credential mode is unsupported or empty"),
        CredentialBearingProviderResponseRefused => ("credential_bearing_provider_response_refused", "credential-bearing provider response refused"),
        CredentialResultBoundaryIsUnavailable => ("credential_result_boundary_is_unavailable", "credential result boundary is unavailable"),
        EffectiveTransportDiffersFromConfiguredBinding => ("effective_transport_differs_from_configured_binding", "effective transport differs from configured binding"),
        FiniteNonzeroRequestResponseByteLimitsAreRequired => ("finite_nonzero_request_response_byte_limits_are_required", "finite nonzero request/response byte limits are required"),
        GeminiBatchResponseMissingEmbeddings => ("gemini_batch_response_missing_embeddings", "Gemini batch response missing embeddings"),
        GeminiEmbeddingContainsNonFiniteComponents => ("gemini_embedding_contains_non_finite_components", "Gemini embedding contains non-finite components"),
        GeminiRequestDimensionsDifferFromConfiguredBinding => ("gemini_request_dimensions_differ_from_configured_binding", "Gemini request dimensions differ from configured binding"),
        OpenaiCompatibleResponseHadNoChoices => ("openai_compatible_response_had_no_choices", "OpenAI-compatible response had no choices"),
        TerminalQueueItemIsMissingItsErrorClass => ("terminal_queue_item_is_missing_its_error_class", "terminal queue item is missing its error class"),
        UnsupportedChatSettingsReasoningEffortRequiresThinkingAndMustBeNonempty => ("unsupported_chat_settings_reasoning_effort_requires_thinking_and_must_be_nonempty", "unsupported chat settings: reasoning effort requires thinking and must be nonempty"),
        AccountSharingKeyIsEmpty => ("account_sharing_key_is_empty", "account sharing key is empty"),
        InvocationCompleted => ("invocation_completed", "invocation completed; recovery answer discarded or expired"),
        AttemptBudgetExhausted => ("attempt_budget_exhausted", "attempt budget exhausted"),
        AuthenticationRejected => ("authentication_rejected", "authentication rejected"),
        BindingIdentityIsRequired => ("binding_identity_is_required", "binding identity is required"),
        CacheFailure => ("cache_failure", "cache failure"),
        CachePathRefused => ("cache_path_refused", "cache path refused"),
        ChatOutputLimitRequired => ("chat_output_limit_required", "chat output limit required"),
        EmbeddingDimensionsMustBeNonzero => ("embedding_dimensions_must_be_nonzero", "embedding dimensions must be nonzero"),
        EmbeddingDimensionsRequired => ("embedding_dimensions_required", "embedding dimensions required"),
        FiniteOutputTokensAreRequired => ("finite_output_tokens_are_required", "finite output tokens are required"),
        FiniteTimeoutIsRequired => ("finite_timeout_is_required", "finite timeout is required"),
        GeminiEmbeddingDimensionMismatch => ("gemini_embedding_dimension_mismatch", "Gemini embedding dimension mismatch"),
        GeminiResponseMissingEmbedding => ("gemini_response_missing_embedding", "Gemini response missing embedding"),
        GeminiTaskOptionIsUnsupported => ("gemini_task_option_is_unsupported", "Gemini task option is unsupported"),
        HttpBudgetExhausted => ("http_budget_exhausted", "HTTP budget exhausted"),
        HttpFailure => ("http_failure", "HTTP failure"),
        HttpRateLimited => ("http_rate_limited", "HTTP rate limited"),
        HttpTimeout => ("http_timeout", "HTTP timeout"),
        HttpUnavailable => ("http_unavailable", "HTTP unavailable"),
        InvalidAdapterResponse => ("invalid_adapter_response", "invalid adapter response"),
        InvalidConfiguration => ("invalid_configuration", "invalid configuration"),
        InvalidCredentialEncoding => ("invalid_credential_encoding", "invalid credential encoding"),
        InvalidHttpClientConfiguration => ("invalid_http_client_configuration", "invalid HTTP client configuration"),
        InvalidProviderResponse => ("invalid_provider_response", "invalid provider response"),
        InvalidResponse => ("invalid_response", "invalid response"),
        LeaseExpired => ("lease_expired", "lease expired"),
        MemoryQueueLockPoisoned => ("memory_queue_lock_poisoned", "memory queue lock poisoned"),
        ModelAdmissionLockPoisoned => ("model_admission_lock_poisoned", "model admission lock poisoned"),
        ModelRegistryIsNotConfigured => ("model_registry_is_not_configured", "model registry is not configured"),
        OutputTokenLimitExceeded => ("output_token_limit_exceeded", "output token limit exceeded"),
        OutputTokenLimitMustBeNonzero => ("output_token_limit_must_be_nonzero", "output token limit must be nonzero"),
        ProviderFailure => ("provider_failure", "provider failure"),
        ProviderRedirectRefused => ("provider_redirect_refused", "provider redirect refused"),
        ProviderRequestLimitExceeded => ("provider_request_limit_exceeded", "provider request limit exceeded"),
        ProviderResponseIsNotValidUtf8 => ("provider_response_is_not_valid_utf8", "provider response is not valid UTF-8"),
        ProviderResponseLimitExceeded => ("provider_response_limit_exceeded", "provider response limit exceeded"),
        ProviderResponseReadFailed => ("provider_response_read_failed", "provider response read failed"),
        QueueResultTooLarge => ("queue_result_too_large", "job result exceeds retained result limit"),
        QueueFailure => ("queue_failure", "queue failure"),
        QueueItemKindMustNotBeEmpty => ("queue_item_kind_must_not_be_empty", "queue item kind must not be empty"),
        RateBucketDisappeared => ("rate_bucket_disappeared", "rate bucket disappeared"),
        RateBucketLockPoisoned => ("rate_bucket_lock_poisoned", "rate bucket lock poisoned"),
        RateGateLockPoisoned => ("rate_gate_lock_poisoned", "rate gate lock poisoned"),
        RuntimePolicyLockPoisoned => ("runtime_policy_lock_poisoned", "runtime policy lock poisoned"),
        SqliteQueueLockPoisoned => ("sqlite_queue_lock_poisoned", "sqlite queue lock poisoned"),
        StaleQueueItem => ("stale_queue_item", "stale queue item"),
        StorageFailure => ("storage_failure", "storage failure"),
        SpendReceiptRefTooLong => ("spend_receipt_ref_too_long", "spend receipt reference exceeds maximum byte length"),
        SpendLedgerUnavailable => ("spend_ledger_unavailable", "spend ledger unavailable"),
        SpendReconciliationRequired => ("spend_reconciliation_required", "spend reconciliation required"),
        SpendBudgetExhausted => ("spend_budget_exhausted", "account spend budget exhausted"),
        ProviderBalanceExhausted => ("provider_balance_exhausted", "provider balance exhausted"),
        SystemOneNeedsABearerApiKey => ("system_one_needs_a_bearer_api_key", "System One needs a bearer API key"),
        UnsupportedQueueSchema => ("unsupported_queue_schema", "unsupported queue schema; rebuild the queue database"),
        WorkerIdMustNotBeEmpty => ("worker_id_must_not_be_empty", "worker_id must not be empty"),
    }
}

closed_codes! {
    /// Closed failure classes shared by execution and durable queue records.
    FailureClass {
        Unavailable => ("unavailable", "unavailable"),
        Auth => ("auth", "auth"),
        RateLimited => ("rate_limited", "rate_limited"),
        BudgetExhausted => ("budget_exhausted", "budget_exhausted"),
        Timeout => ("timeout", "timeout"),
        InvalidRequest => ("invalid_request", "invalid_request"),
        Provider => ("provider", "provider"),
        Queue => ("queue", "queue"),
        Cache => ("cache", "cache"),
        UnsupportedChat => ("unsupported_chat", "unsupported_chat"),
        UnsupportedEmbedding => ("unsupported_embedding", "unsupported_embedding"),
        UnsupportedRerank => ("unsupported_rerank", "unsupported_rerank"),
        UnsupportedClassify => ("unsupported_classify", "unsupported_classify"),
        UnsupportedVision => ("unsupported_vision", "unsupported_vision"),
        UnsupportedImageGeneration => ("unsupported_image_generation", "unsupported_image_generation"),
        UnsupportedVideoGeneration => ("unsupported_video_generation", "unsupported_video_generation"),
        UnsupportedAgentTask => ("unsupported_agent_task", "unsupported_agent_task"),
    }
}
