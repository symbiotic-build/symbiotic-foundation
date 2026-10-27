//! Classification: typed questions about a JSON state, answered with
//! probabilities.
//!
//! [`ClassifierProvider`] is the classification seam beside chat, embedding and
//! rerank. Question kinds mirror TypeSafe System One's primitives:
//!
//! - **Noul**: a yes/no question; the answer is P(yes).
//! - **Choice**: one of an ordered option set; the answer is a probability per
//!   option and the chosen (most probable) option id.
//! - **Score**: ordered levels; the answer is a probability per level and their
//!   probability-weighted index.
//!
//! Implementations:
//!
//! - [`JevClassifierProvider`]: the System One HTTP API, served by TypeSafe or by
//!   a gateway that exposes the same API (e.g. OpenRouter's `/systemone`).
//! - [`ChatClassifierProvider`]: any [`ChatProvider`] answering the same request
//!   with one JSON-mode completion, strictly validated.
//! - [`StaticClassifierProvider`]: scripted answers for tests.
//! - [`QueuedClassifierProvider`] (default `queue` feature): the queue, retry,
//!   cache and trace wrapper shared with the other `Queued*` providers.
//!
//! Callers keep the decision: thresholds are theirs, and the helpers on
//! [`ClassifyResponse`] only evaluate caller-supplied thresholds.

use super::*;

/// TypeSafe's System One API base; requests go to `{base}/systemone`.
pub const TYPESAFE_BASE_URL: &str = "https://api.typesafe.ai/v1";
/// Pinned Jev version for [`JevClassifierProvider::typesafe`].
pub const JEV_DEFAULT_MODEL: &str = "jev-1.13.0";
/// Most options one Choice may have (Jev 1.13).
pub const JEV_MAX_CHOICE_OPTIONS: usize = 255;
/// Most levels one Score may have (Jev 1.13).
pub const JEV_MAX_SCORE_LEVELS: usize = 10;
/// Token budget for the state plus the single longest question (Jev 1.13).
pub const JEV_MAX_STATE_AND_LONGEST_QUESTION_TOKENS: u64 = 32_000;
/// Token budget for the state plus all questions (Jev 1.13).
pub const JEV_MAX_REQUEST_TOKENS: u64 = 64_000;
/// Tokens reserved for the model's own prompt template; TypeSafe's documented
/// examples report about 300 input tokens for a one-question request of under
/// 100 bytes.
const JEV_TEMPLATE_RESERVE_TOKENS: u64 = 512;
/// Tolerance for a probability that should lie in `[0, 1]`.
const PROBABILITY_EPSILON: f64 = 1e-6;
/// How far a provider-reported distribution may sum from 1 before it is
/// rejected (it is then rescaled to sum exactly 1). Jev rounds to two
/// decimals and renormalises; its sums were exact in 1,833 recorded calls.
const REPORTED_SUM_TOLERANCE: f64 = 0.02;
/// Two probabilities this close are a tie.
const TIE_EPSILON: f64 = 1e-9;
/// How far a reported Score may lie from its probability-weighted level.
/// Two-decimal rounding moves the weighted level by at most 0.225 levels
/// (ten levels); half a level means the answer is inconsistent.
const SCORE_TOLERANCE_LEVELS: f64 = 0.5;

// ---------------------------------------------------------------------------
// Request and response types
// ---------------------------------------------------------------------------

/// One option of a [`QuestionKind::Choice`] question.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChoiceOption {
    /// Option id; the answer reports a probability under it.
    pub id: String,
    /// What the option means: the rubric the classifier matches against.
    pub criterion: String,
}

/// The kind of a [`ClassifierQuestion`] and its criteria.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QuestionKind {
    /// A yes/no question; the answer is the probability that it is yes.
    Noul {
        /// What a yes means.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        when_true: Option<String>,
        /// What a no means.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        when_false: Option<String>,
    },
    /// Pick one option of an ordered set.
    Choice {
        /// Options in presentation order.
        options: Vec<ChoiceOption>,
    },
    /// Rate against ordered levels, lowest first.
    Score {
        /// Level descriptions, lowest first.
        levels: Vec<String>,
    },
}

impl QuestionKind {
    /// Wire name of the kind: `noul`, `choice` or `score`.
    pub fn label(&self) -> &'static str {
        match self {
            QuestionKind::Noul { .. } => "noul",
            QuestionKind::Choice { .. } => "choice",
            QuestionKind::Score { .. } => "score",
        }
    }
}

/// A named question in a [`ClassifyRequest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifierQuestion {
    /// Id the answer is reported under; unique within a request.
    pub id: String,
    /// The question. It may refer to state fields by name in backticks
    /// ("Does `message` ask for …?").
    pub instructions: String,
    /// Kind and criteria.
    #[serde(flatten)]
    pub kind: QuestionKind,
}

impl ClassifierQuestion {
    /// A yes/no question with optional meanings for yes and no.
    pub fn noul(
        id: impl Into<String>,
        instructions: impl Into<String>,
        when_true: Option<String>,
        when_false: Option<String>,
    ) -> Self {
        Self {
            id: id.into(),
            instructions: instructions.into(),
            kind: QuestionKind::Noul {
                when_true,
                when_false,
            },
        }
    }

    /// A choice among `(option id, criterion)` pairs, in presentation order.
    pub fn choice<I, O, C>(
        id: impl Into<String>,
        instructions: impl Into<String>,
        options: I,
    ) -> Self
    where
        I: IntoIterator<Item = (O, C)>,
        O: Into<String>,
        C: Into<String>,
    {
        Self {
            id: id.into(),
            instructions: instructions.into(),
            kind: QuestionKind::Choice {
                options: options
                    .into_iter()
                    .map(|(id, criterion)| ChoiceOption {
                        id: id.into(),
                        criterion: criterion.into(),
                    })
                    .collect(),
            },
        }
    }

    /// A score over ordered levels, lowest first.
    pub fn score<I, L>(id: impl Into<String>, instructions: impl Into<String>, levels: I) -> Self
    where
        I: IntoIterator<Item = L>,
        L: Into<String>,
    {
        Self {
            id: id.into(),
            instructions: instructions.into(),
            kind: QuestionKind::Score {
                levels: levels.into_iter().map(Into::into).collect(),
            },
        }
    }
}

/// Request to answer typed questions about a JSON-object state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassifyRequest {
    /// The content classified.
    pub state: serde_json::Map<String, Value>,
    /// Questions, answered independently; answers come back in this order.
    pub questions: Vec<ClassifierQuestion>,
    /// What the state is, for providers that render the request as a prompt:
    /// it completes "You answer two independent questions about …" and ends
    /// with a full stop. `None` renders "the JSON object in the user turn.".
    /// System One models read only `state` and `questions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_description: Option<String>,
    pub role_binding: Option<String>,
    pub source: Option<String>,
    pub metadata: Value,
}

impl ClassifyRequest {
    /// A shareable request with no role binding, source or metadata.
    pub fn new(state: serde_json::Map<String, Value>, questions: Vec<ClassifierQuestion>) -> Self {
        Self {
            state,
            questions,
            state_description: None,
            role_binding: None,
            source: None,
            metadata: serde_json::json!({}),
        }
    }

    /// Check the shape every provider relies on: at least one question,
    /// non-empty unique question ids, a Choice with at least one option and
    /// unique non-empty option ids, a Score with at least two levels.
    /// Provider limits (option counts, token budgets) are the provider's.
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.questions.is_empty() {
            return Err(ModelError::InvalidRequest(
                symbiotic_core::DiagnosticCode::AClassifyRequestNeedsAtLeastOneQuestion,
            ));
        }
        let mut ids = std::collections::HashSet::new();
        for question in &self.questions {
            if question.id.is_empty() || !ids.insert(question.id.as_str()) {
                return Err(ModelError::InvalidRequest(
                    symbiotic_core::DiagnosticCode::InvalidConfiguration,
                ));
            }
            match &question.kind {
                QuestionKind::Noul { .. } => {}
                QuestionKind::Choice { options } => {
                    let mut option_ids = std::collections::HashSet::new();
                    if options.is_empty()
                        || options
                            .iter()
                            .any(|option| option.id.is_empty() || !option_ids.insert(&option.id))
                    {
                        return Err(ModelError::InvalidRequest(
                            symbiotic_core::DiagnosticCode::InvalidConfiguration,
                        ));
                    }
                }
                QuestionKind::Score { levels } => {
                    if levels.len() < 2 {
                        return Err(ModelError::InvalidRequest(
                            symbiotic_core::DiagnosticCode::InvalidConfiguration,
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Probability of one Choice option.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OptionProbability {
    pub id: String,
    pub probability: f64,
}

/// The answer to one question.
///
/// Externally tagged (`{"noul": {"probability": 0.4}}`): an internally tagged
/// or flattened enum buffers its fields, and buffered numbers do not
/// deserialize under serde_json's `arbitrary_precision`, which a workspace
/// crate enables. The response cache round-trips answers through JSON.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerValue {
    /// Probability (0 to 1) that the answer to a Noul question is yes.
    Noul { probability: f64 },
    /// Distribution over a Choice question's options, in request order.
    Choice {
        /// A most probable option; always one of the requested option ids.
        /// On an exact tie it is the provider's pick among the tied options
        /// (Jev rounds probabilities, so its pick reflects unrounded values;
        /// chat-backed and builder answers take the first in request order).
        /// [`ClassifyResponse::decide_choice`] breaks ties the same way.
        chosen: String,
        probabilities: Vec<OptionProbability>,
        /// Provider-reported certainty (0 to 1), when the provider has one.
        #[serde(default)]
        confidence: Option<f64>,
    },
    /// Distribution over a Score question's levels, lowest first.
    Score {
        /// Probability-weighted level index; can land between levels.
        value: f64,
        probabilities: Vec<f64>,
        /// Provider-reported certainty (0 to 1), when the provider has one.
        #[serde(default)]
        confidence: Option<f64>,
    },
}

/// A named answer in a [`ClassifyResponse`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClassifierAnswer {
    pub question_id: String,
    pub value: AnswerValue,
}

impl ClassifierAnswer {
    /// A Noul answer.
    pub fn noul(question_id: impl Into<String>, probability: f64) -> Self {
        Self {
            question_id: question_id.into(),
            value: AnswerValue::Noul { probability },
        }
    }

    /// A Choice answer from `(option id, probability)` pairs; the chosen
    /// option is the most probable one (the first on a tie).
    pub fn choice<I, O>(question_id: impl Into<String>, probabilities: I) -> Self
    where
        I: IntoIterator<Item = (O, f64)>,
        O: Into<String>,
    {
        let probabilities: Vec<OptionProbability> = probabilities
            .into_iter()
            .map(|(id, probability)| OptionProbability {
                id: id.into(),
                probability,
            })
            .collect();
        Self {
            question_id: question_id.into(),
            value: AnswerValue::Choice {
                chosen: most_probable(&probabilities),
                probabilities,
                confidence: None,
            },
        }
    }

    /// A Score answer from per-level probabilities, lowest level first.
    pub fn score(question_id: impl Into<String>, probabilities: Vec<f64>) -> Self {
        Self {
            question_id: question_id.into(),
            value: AnswerValue::Score {
                value: expected_level(&probabilities),
                probabilities,
                confidence: None,
            },
        }
    }
}

/// Response from a classifier provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassifyResponse {
    /// One answer per question, in request order.
    pub answers: Vec<ClassifierAnswer>,
    /// The model that answered, as the provider reports it.
    pub served_model: String,
    /// Usage (`usage.input_tokens`, `usage.output_tokens`), latency
    /// (`timing.provider_ms`) and provider receipt metadata.
    pub trace: ModelInvocationTrace,
    pub raw_provider_response: Option<Value>,
}

/// Outcome of [`ClassifyResponse::decide_choice`].
#[derive(Clone, Debug, PartialEq)]
pub enum ChoiceDecision {
    /// This option won and reached the minimum probability.
    Selected { option: String, probability: f64 },
    /// The abstain option won, or no option reached the minimum probability.
    Abstained,
}

impl ClassifyResponse {
    /// The answer to one question.
    pub fn answer(&self, question_id: &str) -> Option<&AnswerValue> {
        self.answers
            .iter()
            .find(|answer| answer.question_id == question_id)
            .map(|answer| &answer.value)
    }

    /// P(yes) for a Noul question.
    pub fn noul(&self, question_id: &str) -> Option<f64> {
        match self.answer(question_id)? {
            AnswerValue::Noul { probability } => Some(*probability),
            _ => None,
        }
    }

    /// The chosen option of a Choice question.
    pub fn chosen(&self, question_id: &str) -> Option<&str> {
        match self.answer(question_id)? {
            AnswerValue::Choice { chosen, .. } => Some(chosen),
            _ => None,
        }
    }

    /// The probability of one option of a Choice question.
    pub fn choice_probability(&self, question_id: &str, option_id: &str) -> Option<f64> {
        match self.answer(question_id)? {
            AnswerValue::Choice { probabilities, .. } => probabilities
                .iter()
                .find(|entry| entry.id == option_id)
                .map(|entry| entry.probability),
            _ => None,
        }
    }

    /// The probability-weighted level of a Score question.
    pub fn score(&self, question_id: &str) -> Option<f64> {
        match self.answer(question_id)? {
            AnswerValue::Score { value, .. } => Some(*value),
            _ => None,
        }
    }

    /// Whether P(yes) for a Noul question reaches `threshold` (inclusive).
    pub fn noul_at_least(&self, question_id: &str, threshold: f64) -> Option<bool> {
        self.noul(question_id)
            .map(|probability| probability >= threshold)
    }

    /// Evaluate a Choice question that may carry an abstain option such as
    /// `none`: the most probable other option is selected when its
    /// probability reaches `min_probability` and exceeds the abstain option's;
    /// otherwise the decision is to abstain. Exact ties go to the answer's
    /// `chosen` option (then to the first option in request order), so
    /// without an abstain option this selects `chosen` whenever its
    /// probability reaches the minimum. `None` when the question has no
    /// Choice answer.
    pub fn decide_choice(
        &self,
        question_id: &str,
        abstain_option: Option<&str>,
        min_probability: f64,
    ) -> Option<ChoiceDecision> {
        let AnswerValue::Choice {
            chosen,
            probabilities,
            ..
        } = self.answer(question_id)?
        else {
            return None;
        };
        let abstain = abstain_option.map(|abstain| {
            probabilities
                .iter()
                .find(|entry| entry.id == abstain)
                .map_or(0.0, |entry| entry.probability)
        });
        // More probable, or tied and the provider's chosen option.
        let beats = |entry: &OptionProbability, other: f64| {
            entry.probability > other + TIE_EPSILON
                || ((entry.probability - other).abs() <= TIE_EPSILON && &entry.id == chosen)
        };
        let best = probabilities
            .iter()
            .filter(|entry| Some(entry.id.as_str()) != abstain_option)
            .fold(None::<&OptionProbability>, |best, entry| match best {
                Some(best) if !beats(entry, best.probability) => Some(best),
                _ => Some(entry),
            });
        Some(match best {
            Some(best)
                if best.probability >= min_probability
                    && abstain.is_none_or(|abstain| beats(best, abstain)) =>
            {
                ChoiceDecision::Selected {
                    option: best.id.clone(),
                    probability: best.probability,
                }
            }
            _ => ChoiceDecision::Abstained,
        })
    }
}

impl TraceCarrier for ClassifyResponse {
    fn trace(&self) -> &ModelInvocationTrace {
        &self.trace
    }

    fn set_trace(&mut self, trace: ModelInvocationTrace) {
        self.trace = trace;
    }
}

#[cfg(feature = "queue")]
impl BudgetedModelRequest for ClassifyRequest {
    fn input_budget_units(&self) -> Result<u64, ModelError> {
        let state = Value::Object(self.state.clone()).to_string();
        let questions = serde_json::to_string(&self.questions).map_err(|_| {
            ModelError::InvalidRequest(
                symbiotic_core::DiagnosticCode::ClassificationQuestionsCannotBeEncoded,
            )
        })?;
        Ok(estimate_token_budget_units([
            state.as_str(),
            questions.as_str(),
        ]))
    }
}

// ---------------------------------------------------------------------------
// Trait and queue wrapper
// ---------------------------------------------------------------------------

/// Provider that answers typed questions about a JSON state with
/// probabilities.
#[async_trait]
pub trait ClassifierProvider: ModelProvider {
    async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError>;
}

#[async_trait]
impl<T> ClassifierProvider for Arc<T>
where
    T: ClassifierProvider + ?Sized,
{
    async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
        (**self).classify(request).await
    }
}

#[cfg(feature = "queue")]
/// Queue-bound wrapper for a [`ClassifierProvider`], mirroring
/// [`QueuedRerankProvider`]: idempotency, model cap, rate buckets, cooldowns,
/// retry classification, exact response cache and traces.
///
/// Cached responses are scoped to the provider's descriptor (identity,
/// class, auth mode and metadata such as the expected served model), so
/// classifiers sharing a cache directory never read each other's answers.
#[derive(Clone)]
pub struct QueuedClassifierProvider<P> {
    inner: P,
    runtime: QueueRuntime,
}

#[cfg(feature = "queue")]
impl<P> QueuedClassifierProvider<P> {
    pub fn new(
        inner: P,
        queue: Arc<dyn QueueBackend>,
        worker_id: impl Into<String>,
        config: ModelQueueConfig,
    ) -> Self {
        Self {
            inner,
            runtime: QueueRuntime::new(queue, worker_id.into(), config),
        }
    }

    queue_runtime_builders!();
}

#[cfg(feature = "queue")]
#[async_trait]
impl<P> ModelProvider for QueuedClassifierProvider<P>
where
    P: ClassifierProvider + Clone + Send + Sync,
{
    fn descriptor(&self) -> &ProviderDescriptor {
        self.inner.descriptor()
    }

    fn validate_configuration(&self) -> Result<(), ModelError> {
        self.inner.validate_configuration()
    }

    fn credential_fingerprint(&self) -> Option<String> {
        self.inner.credential_fingerprint()
    }

    fn credential_boundary(&self) -> Option<&CredentialBoundary> {
        self.inner.credential_boundary()
    }
}

#[cfg(feature = "queue")]
#[async_trait]
impl<P> ClassifierProvider for QueuedClassifierProvider<P>
where
    P: ClassifierProvider + Clone + Send + Sync + 'static,
{
    async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
        // A malformed request never takes a queue slot or a provider call.
        request.validate()?;
        let descriptor = self.inner.descriptor().clone();
        run_queued(
            &self.runtime,
            descriptor,
            ModelCapability::Classify,
            "classify",
            request,
            |inner: P, request| async move { inner.classify(request).await },
            self.inner.clone(),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Answer validation shared by the providers
// ---------------------------------------------------------------------------

fn check_probability(value: f64) -> Result<f64, ModelError> {
    if value.is_finite() && (-PROBABILITY_EPSILON..=1.0 + PROBABILITY_EPSILON).contains(&value) {
        Ok(value.clamp(0.0, 1.0))
    } else {
        Err(ModelError::Provider(DiagnosticCode::ProviderFailure))
    }
}

/// Exactly the requested options, each a probability, in request order,
/// rescaled to sum to 1. `max_sum_error` rejects a distribution whose sum is
/// further from 1 (a provider that promises a distribution); `None` accepts
/// any positive sum (a text model's stated numbers rarely add up).
fn choice_probabilities(
    options: &[ChoiceOption],
    mut reported: HashMap<String, f64>,
    max_sum_error: Option<f64>,
) -> Result<Vec<OptionProbability>, ModelError> {
    let mut probabilities = Vec::with_capacity(options.len());
    for option in options {
        let value = reported
            .remove(&option.id)
            .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
        probabilities.push(OptionProbability {
            id: option.id.clone(),
            probability: check_probability(value)?,
        });
    }
    if let Some(_extra) = reported.keys().next() {
        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
    }
    let values: Vec<f64> = probabilities
        .iter()
        .map(|entry| entry.probability)
        .collect();
    for (entry, value) in probabilities
        .iter_mut()
        .zip(normalise_distribution(&values, max_sum_error)?)
    {
        entry.probability = value;
    }
    Ok(probabilities)
}

/// Every level `"0"`, `"1"`, … exactly once, lowest first, rescaled to sum
/// to 1; `max_sum_error` as in [`choice_probabilities`].
fn score_probabilities(
    level_count: usize,
    mut reported: HashMap<String, f64>,
    max_sum_error: Option<f64>,
) -> Result<Vec<f64>, ModelError> {
    let mut probabilities = Vec::with_capacity(level_count);
    for level in 0..level_count {
        let key = level.to_string();
        let value = reported
            .remove(&key)
            .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
        probabilities.push(check_probability(value)?);
    }
    if let Some(_extra) = reported.keys().next() {
        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
    }
    normalise_distribution(&probabilities, max_sum_error)
}

fn normalise_distribution(
    values: &[f64],
    max_sum_error: Option<f64>,
) -> Result<Vec<f64>, ModelError> {
    let total: f64 = values.iter().sum();
    if total <= PROBABILITY_EPSILON {
        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
    }
    if let Some(max_error) = max_sum_error
        && (total - 1.0).abs() > max_error
    {
        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
    }
    Ok(values.iter().map(|value| value / total).collect())
}

/// Validate every answer against its question, in request order: the public
/// contract of [`ClassifyResponse`] whatever the provider. Noul: a
/// probability. Choice: exactly the requested option ids in order, each a
/// probability, summing to 1, with `chosen` among the most probable options.
/// Score: one probability per level, summing to 1, `value` their weighted
/// level. Confidence, when present, is a probability.
fn validate_answers(
    request: &ClassifyRequest,
    answers: &[ClassifierAnswer],
) -> Result<(), ModelError> {
    if answers.len() != request.questions.len() {
        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
    }
    let sums_to_one = |values: &mut dyn Iterator<Item = f64>| {
        (values.sum::<f64>() - 1.0).abs() <= PROBABILITY_EPSILON
    };
    for (question, answer) in request.questions.iter().zip(answers) {
        if answer.question_id != question.id {
            return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
        }
        let in_range = |value: f64| value.is_finite() && (0.0..=1.0).contains(&value);
        match (&question.kind, &answer.value) {
            (QuestionKind::Noul { .. }, AnswerValue::Noul { probability }) => {
                if !in_range(*probability) {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
            }
            (
                QuestionKind::Choice { options },
                AnswerValue::Choice {
                    chosen,
                    probabilities,
                    confidence,
                },
            ) => {
                if probabilities.len() != options.len()
                    || probabilities
                        .iter()
                        .zip(options)
                        .any(|(entry, option)| entry.id != option.id)
                {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
                if !probabilities
                    .iter()
                    .all(|entry| in_range(entry.probability))
                    || !sums_to_one(&mut probabilities.iter().map(|entry| entry.probability))
                {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
                let max = probabilities
                    .iter()
                    .map(|entry| entry.probability)
                    .fold(0.0, f64::max);
                if !probabilities
                    .iter()
                    .any(|entry| &entry.id == chosen && entry.probability >= max - TIE_EPSILON)
                {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
                if confidence.is_some_and(|value| !in_range(value)) {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
            }
            (
                QuestionKind::Score { levels },
                AnswerValue::Score {
                    value,
                    probabilities,
                    confidence,
                },
            ) => {
                if probabilities.len() != levels.len()
                    || !probabilities.iter().all(|p| in_range(*p))
                    || !sums_to_one(&mut probabilities.iter().copied())
                {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
                if !value.is_finite()
                    || (value - expected_level(probabilities)).abs() > PROBABILITY_EPSILON
                {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
                if confidence.is_some_and(|value| !in_range(value)) {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
            }
            (_, _) => {
                return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
            }
        }
    }
    Ok(())
}

fn most_probable(probabilities: &[OptionProbability]) -> String {
    let mut best: Option<&OptionProbability> = None;
    for entry in probabilities {
        if best.is_none_or(|best| entry.probability > best.probability) {
            best = Some(entry);
        }
    }
    best.map(|entry| entry.id.clone()).unwrap_or_default()
}

fn expected_level(probabilities: &[f64]) -> f64 {
    probabilities
        .iter()
        .enumerate()
        .map(|(level, probability)| level as f64 * probability)
        .sum()
}

fn classify_trace(
    descriptor: &ProviderDescriptor,
    request: &ClassifyRequest,
    response_text: &str,
    started: Instant,
) -> Result<ModelInvocationTrace, ModelError> {
    let mut trace = success_trace(
        descriptor,
        request.role_binding.clone(),
        request.source.clone(),
        hash_json(request)?,
        Some(response_text),
    );
    trace.timing.provider_ms = Some(started.elapsed().as_millis() as u64);
    Ok(trace)
}

// ---------------------------------------------------------------------------
// TypeSafe System One (Jev)
// ---------------------------------------------------------------------------

/// Classifier over the System One HTTP API: `POST {base_url}/systemone` with
/// `{"model", "state", "questions"}` and a bearer key.
///
/// Before sending it enforces Jev 1.13's documented limits (255 Choice options,
/// 10 Score levels, 32k tokens for the state plus the longest question, 64k
/// for the request) as [`ModelError::InvalidRequest`]; tokens are estimated as
/// one per UTF-8 byte of JSON plus a template reserve, which never
/// under-counts. The response must name the expected served model (by default
/// the requested one; a gateway may report a dated snapshot, set with
/// [`Self::with_served_model`]); anything else, and answers that do not match
/// the questions, are [`ModelError::Provider`]. HTTP statuses share the crate's
/// classification: 408/504 time out, 429 is rate limited and 5xx (TypeSafe's
/// 529 Overloaded included) is unavailable — all retryable.
#[derive(Clone)]
pub struct JevClassifierProvider {
    descriptor: ProviderDescriptor,
    client: HttpClient,
    base_url: String,
    api_key: CredentialBoundary,
    max_request_bytes: Option<usize>,
    max_response_bytes: Option<usize>,
    served_model: String,
}

impl JevClassifierProvider {
    /// A System One endpoint. `operator` names who serves it (`typesafe`, or
    /// a gateway such as `openrouter`) and `base_url` its API base.
    pub fn new(
        operator: impl Into<String>,
        model: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<SecretValue<String>>,
    ) -> Self {
        let model = model.into();
        let base_url = base_url.into();
        Self {
            descriptor: ProviderDescriptor {
                identity: ModelIdentity::new("classify", operator, model.clone()),
                provider_class: ProviderClass::Cloud,
                capabilities: vec![ModelCapability::Classify],
                auth_mode: ProviderAuthMode::ApiKey {
                    secret_ref: "runtime".to_string(),
                },
                metadata: serde_json::json!({ "wire": "systemone", "served_model": model, "endpoint": crate::registry::validate_endpoint(&base_url).ok().map(|()| base_url.as_str()) }),
            },
            client: HttpClient::default(),
            base_url,
            api_key: CredentialBoundary::new(api_key.into()),
            max_request_bytes: None,
            max_response_bytes: None,
            served_model: model,
        }
        .with_request_limit(default_max_request_bytes())
        .with_response_limit(default_max_response_bytes())
    }

    /// Bound the complete encoded System One request before sending.
    pub fn with_request_limit(mut self, max_bytes: usize) -> Self {
        self.descriptor.metadata["max_request_bytes"] = serde_json::json!(max_bytes);
        self.max_request_bytes = Some(max_bytes);
        self
    }
    /// Bound success and error HTTP response bodies with the shared capped reader.
    pub fn with_response_limit(mut self, max_bytes: usize) -> Self {
        self.descriptor.metadata["max_response_bytes"] = serde_json::json!(max_bytes);
        self.max_response_bytes = Some(max_bytes);
        self
    }

    /// TypeSafe's own endpoint with [`JEV_DEFAULT_MODEL`].
    pub fn typesafe(api_key: impl Into<SecretValue<String>>) -> Self {
        Self::new("typesafe", JEV_DEFAULT_MODEL, TYPESAFE_BASE_URL, api_key)
    }

    /// Resolve the key for `auth_mode` through the host's credential
    /// resolver. A bearer token or API key is accepted.
    pub async fn from_resolver(
        resolver: &dyn CredentialResolver,
        auth_mode: ProviderAuthMode,
        operator: impl Into<String>,
        model: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let api_key = match resolver.resolve_auth(&auth_mode).await? {
            ResolvedAuth::Bearer(key) | ResolvedAuth::ApiKey(key) if !key.trim().is_empty() => key,
            _ => {
                return Err(ModelError::Auth(
                    symbiotic_core::DiagnosticCode::SystemOneNeedsABearerApiKey,
                ));
            }
        };
        let mut provider = Self::new(operator, model, base_url, api_key);
        provider.descriptor.auth_mode = auth_mode;
        Ok(provider)
    }

    /// Clients cannot be injected through the public API.
    /// ```compile_fail
    /// use symbiotic_model::JevClassifierProvider;
    /// JevClassifierProvider::new("op", "model", "http://localhost", "key")
    ///     .with_client(reqwest::Client::new());
    /// ```
    /// Set a finite timeout on a Foundation-owned redirect-free, direct client.
    pub fn with_timeout(mut self, timeout_seconds: u64) -> Result<Self, ModelError> {
        self.client = HttpClient(Ok(http_client(Some(timeout_seconds))?));
        Ok(self)
    }

    /// Accept `served_model` as the model the endpoint reports, e.g. the
    /// dated snapshot a gateway names for the requested version.
    pub fn with_served_model(mut self, served_model: impl Into<String>) -> Self {
        self.served_model = served_model.into();
        self.descriptor.metadata["served_model"] = Value::String(self.served_model.clone());
        self
    }

    fn check_limits(request: &ClassifyRequest) -> Result<(), ModelError> {
        for question in &request.questions {
            let (count, max, _what) = match &question.kind {
                QuestionKind::Choice { options } => {
                    (options.len(), JEV_MAX_CHOICE_OPTIONS, "choice options")
                }
                QuestionKind::Score { levels } => {
                    (levels.len(), JEV_MAX_SCORE_LEVELS, "score levels")
                }
                QuestionKind::Noul { .. } => continue,
            };
            if count > max {
                return Err(ModelError::InvalidRequest(
                    symbiotic_core::DiagnosticCode::InvalidConfiguration,
                ));
            }
        }
        let invalid =
            |_| ModelError::InvalidRequest(symbiotic_core::DiagnosticCode::InvalidConfiguration);
        let pair_budget = JEV_MAX_STATE_AND_LONGEST_QUESTION_TOKENS - JEV_TEMPLATE_RESERVE_TOKENS;
        let total_budget = JEV_MAX_REQUEST_TOKENS - JEV_TEMPLATE_RESERVE_TOKENS;
        // Jev's conservative token estimate is one token per encoded byte. Count
        // borrowed JSON with the same writer as encoding, stopping at either token bound.
        let state =
            wire::encoded_len(&request.state, pair_budget as usize).map_err(invalid)? as u64;
        let mut all = 0;
        for question in &request.questions {
            let remaining = (pair_budget - state).min(total_budget - state - all);
            let question = wire::encoded_len(&WireQuestion(question), remaining as usize)
                .map_err(invalid)? as u64;
            all += question;
        }
        Ok(())
    }

    /// Map the typed answers onto the questions, in request order.
    ///
    /// Answers are read field by field from the parsed `Value`, never through
    /// an internally tagged serde enum: that buffers numbers, and a buffered
    /// number with a non-canonical spelling (`0.1200`) fails to deserialize
    /// under serde_json's `arbitrary_precision`, which a workspace crate
    /// enables.
    fn parse_answers(
        request: &ClassifyRequest,
        answers: &serde_json::Map<String, Value>,
    ) -> Result<Vec<ClassifierAnswer>, ModelError> {
        let mut parsed = Vec::with_capacity(request.questions.len());
        for question in &request.questions {
            let id = question.id.as_str();
            let answer = answers
                .get(id)
                .and_then(Value::as_object)
                .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
            let kind = answer.get("type").and_then(Value::as_str).unwrap_or("");
            if kind != question.kind.label() {
                return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
            }
            let number = |field: &str| {
                answer
                    .get(field)
                    .and_then(Value::as_f64)
                    .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))
            };
            let confidence = match answer.get("confidence") {
                None | Some(Value::Null) => None,
                Some(_) => Some(check_probability(number("confidence")?)?),
            };
            let distribution = || {
                answer
                    .get("probabilities")
                    .cloned()
                    .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))
                    .and_then(number_map)
            };
            let value = match &question.kind {
                QuestionKind::Noul { .. } => AnswerValue::Noul {
                    probability: check_probability(number("noul")?)?,
                },
                QuestionKind::Choice { options } => {
                    let chosen = answer
                        .get("choice")
                        .and_then(Value::as_str)
                        .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
                    if !options.iter().any(|option| option.id == chosen) {
                        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                    }
                    AnswerValue::Choice {
                        chosen: chosen.to_string(),
                        probabilities: choice_probabilities(
                            options,
                            distribution()?,
                            Some(REPORTED_SUM_TOLERANCE),
                        )?,
                        confidence,
                    }
                }
                QuestionKind::Score { levels } => {
                    let probabilities = score_probabilities(
                        levels.len(),
                        distribution()?,
                        Some(REPORTED_SUM_TOLERANCE),
                    )?;
                    let value = expected_level(&probabilities);
                    let reported = number("score")?;
                    let top = (levels.len() - 1) as f64;
                    if !(0.0..=top).contains(&reported)
                        || (reported - value).abs() > SCORE_TOLERANCE_LEVELS
                    {
                        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                    }
                    AnswerValue::Score {
                        value,
                        probabilities,
                        confidence,
                    }
                }
            };
            parsed.push(ClassifierAnswer {
                question_id: question.id.clone(),
                value,
            });
        }
        if let Some(_extra) = answers
            .keys()
            .find(|key| !request.questions.iter().any(|q| &q.id == *key))
        {
            return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
        }
        validate_answers(request, &parsed)?;
        Ok(parsed)
    }
}

#[async_trait]
impl ModelProvider for JevClassifierProvider {
    fn descriptor(&self) -> &ProviderDescriptor {
        &self.descriptor
    }

    fn validate_configuration(&self) -> Result<(), ModelError> {
        crate::registry::validate_endpoint(&self.base_url)?;
        self.client.get()?;
        required_byte_limit(self.max_request_bytes)?;
        required_byte_limit(self.max_response_bytes)?;
        Ok(())
    }

    fn credential_fingerprint(&self) -> Option<String> {
        api_key_fingerprint(self.api_key.secret())
    }

    fn credential_boundary(&self) -> Option<&CredentialBoundary> {
        Some(&self.api_key)
    }
}

#[async_trait]
impl ClassifierProvider for JevClassifierProvider {
    async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
        secrets::credential_boundary(
            (async {
                self.validate_configuration()?;
                let body = jev_classify_body(
                    &self.descriptor.identity.model.0,
                    &request,
                    self.max_request_bytes,
                )?;
                let started = Instant::now();
                let builder = self
                    .client
                    .get()?
                    .post(format!("{}/systemone", self.base_url.trim_end_matches('/')))
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body);
                let builder = if self.api_key.secret().is_empty() {
                    builder
                } else {
                    builder.bearer_auth(self.api_key.secret())
                };
                let (raw, text) =
                    provider_response_json(builder, self.max_response_bytes, ModelError::Provider)
                        .await?;
                let served_model = raw
                    .get("model")
                    .and_then(Value::as_str)
                    .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?
                    .to_string();
                if served_model != self.served_model {
                    return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
                }
                let answers = raw
                    .get("answers")
                    .and_then(Value::as_object)
                    .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
                let answers = Self::parse_answers(&request, answers)?;
                let mut trace = classify_trace(&self.descriptor, &request, &text, started)?;
                let usage = |field: &str| {
                    raw.pointer(&format!("/usage/{field}"))
                        .and_then(Value::as_u64)
                };
                trace.usage.input_tokens = usage("input_tokens");
                trace.usage.output_tokens = usage("output_tokens");
                trace.usage.reported_cost_usd = reported_cost_usd(&raw);
                let cache_counter = |field: &str| {
                    raw.pointer(&format!("/usage/{field}"))
                        .map(|value| match value.as_u64() {
                            Some(count) => Ok(Some(count)),
                            None if !egress_observations_enabled() => Ok(None),
                            None => Err(ModelError::Provider(DiagnosticCode::InvalidResponse)),
                        })
                        .transpose()
                        .map(Option::flatten)
                };
                let (hit, miss) = observed_prompt_cache_counts(
                    trace.usage.input_tokens,
                    cache_counter("cache_hit_tokens")?,
                    cache_counter("cache_miss_tokens")?,
                    None,
                )?;
                trace.usage.cache_hit_tokens = hit;
                trace.usage.cache_miss_tokens = miss;
                trace.metadata = serde_json::json!({
                    "provider": {
                        "response_id": raw.get("id").and_then(Value::as_str),
                        "served_model": served_model,
                        "reported_cost_usd": trace.usage.reported_cost_usd,
                    },
                });
                provider_usage_identity(&mut trace, &raw, |identity| {
                    jev_request_contains_identity(&request, identity)
                });
                Ok(ClassifyResponse {
                    answers,
                    served_model,
                    trace,
                    raw_provider_response: Some(raw),
                })
            })
            .await,
            &self.api_key,
        )
    }
}

/// Validate Jev's question/token bounds and encode the complete System One body.
/// Admission and transport share this encoder; question and option order is preserved.
/// Local metadata and state descriptions are excluded from the provider body.
pub fn jev_classify_body(
    model: &str,
    request: &ClassifyRequest,
    max_bytes: Option<usize>,
) -> Result<Vec<u8>, ModelError> {
    // Empty questions keep their shape diagnostic. Allocation-free count checks
    // and bounded token counting preserve provider-limit diagnostic precedence.
    if request.questions.is_empty() {
        request.validate()?;
    }
    JevClassifierProvider::check_limits(request)?;
    let wire_request = JevWireRequest {
        model,
        state: &request.state,
        questions: &request.questions,
    };
    // Cap body buffering before uniqueness allocation and reuse the admitted body.
    let body = max_bytes
        .map(|max| wire::encode(&wire_request, Some(max)))
        .transpose()?;
    request.validate()?;
    match body {
        Some(body) => Ok(body),
        None => wire::encode(&wire_request, None),
    }
}

// Inspect only text sent by JevWireRequest, excluding its model and type labels.
// Local metadata and state_description are not provider-visible on this path.
fn jev_request_contains_identity(request: &ClassifyRequest, identity: &str) -> bool {
    fn value_contains(value: &Value, identity: &str) -> bool {
        match value {
            Value::String(text) => text.contains(identity),
            Value::Array(values) => values.iter().any(|value| value_contains(value, identity)),
            Value::Object(values) => values
                .iter()
                .any(|(key, value)| key.contains(identity) || value_contains(value, identity)),
            _ => false,
        }
    }
    request
        .state
        .iter()
        .any(|(key, value)| key.contains(identity) || value_contains(value, identity))
        || request.questions.iter().any(|question| {
            question.id.contains(identity)
                || question.instructions.contains(identity)
                || match &question.kind {
                    QuestionKind::Noul {
                        when_true,
                        when_false,
                    } => when_true
                        .iter()
                        .chain(when_false.iter())
                        .any(|text| text.contains(identity)),
                    QuestionKind::Choice { options } => options.iter().any(|option| {
                        option.id.contains(identity) || option.criterion.contains(identity)
                    }),
                    QuestionKind::Score { levels } => {
                        levels.iter().any(|text| text.contains(identity))
                    }
                }
        })
}

// Questions and Choice options are JSON maps whose order is the presentation
// order, so they are written from the request's vectors in order (a
// `serde_json::Map` sorts keys unless `preserve_order` is enabled).

struct JevWireRequest<'a> {
    model: &'a str,
    state: &'a serde_json::Map<String, Value>,
    questions: &'a [ClassifierQuestion],
}

impl Serialize for JevWireRequest<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("model", self.model)?;
        map.serialize_entry("state", self.state)?;
        map.serialize_entry("questions", &WireQuestions(self.questions))?;
        map.end()
    }
}

struct WireQuestions<'a>(&'a [ClassifierQuestion]);

impl Serialize for WireQuestions<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for question in self.0 {
            map.serialize_entry(&question.id, &WireQuestion(question))?;
        }
        map.end()
    }
}

struct WireQuestion<'a>(&'a ClassifierQuestion);

impl Serialize for WireQuestion<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let question = self.0;
        let has_criteria = match &question.kind {
            QuestionKind::Noul {
                when_true,
                when_false,
            } => when_true.is_some() || when_false.is_some(),
            QuestionKind::Choice { .. } | QuestionKind::Score { .. } => true,
        };
        let mut map = serializer.serialize_map(Some(if has_criteria { 3 } else { 2 }))?;
        map.serialize_entry("type", question.kind.label())?;
        map.serialize_entry("instructions", &question.instructions)?;
        match &question.kind {
            QuestionKind::Noul {
                when_true,
                when_false,
            } => {
                if has_criteria {
                    map.serialize_entry(
                        "criteria",
                        &NoulCriteria {
                            when_true: when_true.as_deref(),
                            when_false: when_false.as_deref(),
                        },
                    )?;
                }
            }
            QuestionKind::Choice { options } => {
                map.serialize_entry("criteria", &ChoiceCriteria(options))?;
            }
            QuestionKind::Score { levels } => {
                map.serialize_entry("criteria", levels)?;
            }
        }
        map.end()
    }
}

#[derive(Serialize)]
struct NoulCriteria<'a> {
    #[serde(rename = "true", skip_serializing_if = "Option::is_none")]
    when_true: Option<&'a str>,
    #[serde(rename = "false", skip_serializing_if = "Option::is_none")]
    when_false: Option<&'a str>,
}

struct ChoiceCriteria<'a>(&'a [ChoiceOption]);

impl Serialize for ChoiceCriteria<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for option in self.0 {
            map.serialize_entry(&option.id, &option.criterion)?;
        }
        map.end()
    }
}

// ---------------------------------------------------------------------------
// Chat-backed classifier
// ---------------------------------------------------------------------------

/// Classifier that asks any [`ChatProvider`] the questions in one JSON-mode
/// completion.
///
/// The system prompt names the state ([`ClassifyRequest::state_description`]),
/// renders every question (choice: options with criteria; noul: what yes and
/// no mean; score: numbered levels) and the exact reply shape, e.g.
/// `{"route": {"quick": <p>, "goal": <p>}, "goal": <p>}`; the user turn is the
/// state as JSON. The reply must be exactly that object: every question
/// answered, no other keys, each probability a number in `[0, 1]`, Choice and
/// Score keys exactly the requested ids. An answer outside the requested ids
/// is therefore impossible; a reply that breaks the shape is a
/// [`ModelError::Provider`] and the caller decides the fallback. Choice and
/// Score probabilities are normalised to sum to 1.
///
/// The identity is `classify:<chat operator>:<chat model>` and the class is
/// the chat provider's. Class is descriptive; Memory authorizes provider principals.
/// To share the chat model's queue, pass a [`QueuedChatProvider`].
#[derive(Clone)]
pub struct ChatClassifierProvider {
    descriptor: ProviderDescriptor,
    chat: Arc<dyn ChatProvider>,
    max_output_tokens: Option<u32>,
}

impl ChatClassifierProvider {
    pub fn new(chat: Arc<dyn ChatProvider>) -> Self {
        let inner = chat.descriptor();
        let descriptor = ProviderDescriptor {
            identity: ModelIdentity {
                operation: Operation::new("classify"),
                operator: inner.identity.operator.clone(),
                model: inner.identity.model.clone(),
            },
            provider_class: inner.provider_class,
            capabilities: vec![ModelCapability::Classify],
            auth_mode: inner.auth_mode.clone(),
            metadata: serde_json::json!({ "wire": "chat-json", "chat": inner }),
        };
        Self {
            descriptor,
            chat,
            max_output_tokens: None,
        }
    }

    /// Cap the completion length. Reasoning models count thinking against the
    /// cap; a reply cut off by it fails to parse.
    pub fn with_max_output_tokens(mut self, max_output_tokens: u32) -> Self {
        self.descriptor.metadata["max_output_tokens"] = serde_json::json!(max_output_tokens);
        self.max_output_tokens = Some(max_output_tokens);
        self
    }

    /// The system prompt for `request`.
    pub fn render_system_prompt(request: &ClassifyRequest) -> String {
        let count = request.questions.len();
        let subject = request
            .state_description
            .as_deref()
            .unwrap_or("the JSON object in the user turn.");
        let opening = if count == 1 {
            format!("You answer one question about {subject}")
        } else {
            format!(
                "You answer {} independent questions about {subject}",
                count_word(count)
            )
        };
        let blocks: Vec<String> = request.questions.iter().map(render_question).collect();
        let shape: Vec<String> = request
            .questions
            .iter()
            .map(|question| format!("{}: {}", quoted(&question.id), reply_slot(question)))
            .collect();
        format!(
            "{opening}\n\n{}\n\nReply with only this JSON object: {{{}}}",
            blocks.join("\n\n"),
            shape.join(", ")
        )
    }

    /// The user turn: the state as a JSON object.
    pub fn render_user_message(request: &ClassifyRequest) -> String {
        Value::Object(request.state.clone()).to_string()
    }

    /// Parse and validate a reply against the request's questions.
    pub fn parse_reply(
        request: &ClassifyRequest,
        reply: &str,
    ) -> Result<Vec<ClassifierAnswer>, ModelError> {
        let value: Value = serde_json::from_str(reply.trim())
            .map_err(|_| ModelError::Provider(DiagnosticCode::ProviderFailure))?;
        let Value::Object(mut object) = value else {
            return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
        };
        let mut answers = Vec::with_capacity(request.questions.len());
        for question in &request.questions {
            let raw = object
                .remove(&question.id)
                .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
            let value = match &question.kind {
                QuestionKind::Noul { .. } => {
                    let probability = raw
                        .as_f64()
                        .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))?;
                    AnswerValue::Noul {
                        probability: check_probability(probability)?,
                    }
                }
                QuestionKind::Choice { options } => {
                    let probabilities = choice_probabilities(options, number_map(raw)?, None)?;
                    AnswerValue::Choice {
                        chosen: most_probable(&probabilities),
                        probabilities,
                        confidence: None,
                    }
                }
                QuestionKind::Score { levels } => {
                    let probabilities = score_probabilities(levels.len(), number_map(raw)?, None)?;
                    AnswerValue::Score {
                        value: expected_level(&probabilities),
                        probabilities,
                        confidence: None,
                    }
                }
            };
            answers.push(ClassifierAnswer {
                question_id: question.id.clone(),
                value,
            });
        }
        if !object.is_empty() {
            return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
        }
        validate_answers(request, &answers)?;
        Ok(answers)
    }
}

fn number_map(raw: Value) -> Result<HashMap<String, f64>, ModelError> {
    let Value::Object(object) = raw else {
        return Err(ModelError::Provider(DiagnosticCode::ProviderFailure));
    };
    object
        .into_iter()
        .map(|(key, value)| {
            value
                .as_f64()
                .map(|number| (key, number))
                .ok_or(ModelError::Provider(DiagnosticCode::ProviderFailure))
        })
        .collect()
}

fn render_question(question: &ClassifierQuestion) -> String {
    let id = quoted(&question.id);
    let instructions = &question.instructions;
    match &question.kind {
        QuestionKind::Noul {
            when_true,
            when_false,
        } => {
            let mut block = format!(
                "Question {id} (give the probability from 0 to 1 that the answer is yes). {instructions}"
            );
            if let Some(yes) = when_true {
                block.push_str(&format!("\nYes means: {yes}"));
            }
            if let Some(no) = when_false {
                block.push_str(&format!("\nNo means: {no}"));
            }
            block
        }
        QuestionKind::Choice { options } => {
            let lines: Vec<String> = options
                .iter()
                .map(|option| format!("- {}: {}", option.id, option.criterion))
                .collect();
            format!(
                "Question {id} (give a probability for each option; they sum to 1). {instructions}\nOptions:\n{}",
                lines.join("\n")
            )
        }
        QuestionKind::Score { levels } => {
            let lines: Vec<String> = levels
                .iter()
                .enumerate()
                .map(|(index, level)| format!("- {index}: {level}"))
                .collect();
            format!(
                "Question {id} (give a probability for each level; they sum to 1). {instructions}\nLevels:\n{}",
                lines.join("\n")
            )
        }
    }
}

fn reply_slot(question: &ClassifierQuestion) -> String {
    let keyed = |keys: Vec<String>| {
        let entries: Vec<String> = keys
            .iter()
            .map(|key| format!("{}: <p>", quoted(key)))
            .collect();
        format!("{{{}}}", entries.join(", "))
    };
    match &question.kind {
        QuestionKind::Noul { .. } => "<p>".to_string(),
        QuestionKind::Choice { options } => {
            keyed(options.iter().map(|option| option.id.clone()).collect())
        }
        QuestionKind::Score { levels } => keyed((0..levels.len()).map(|i| i.to_string()).collect()),
    }
}

fn quoted(text: &str) -> String {
    Value::String(text.to_string()).to_string()
}

fn count_word(count: usize) -> String {
    const WORDS: [&str; 11] = [
        "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
    ];
    WORDS
        .get(count)
        .map_or_else(|| count.to_string(), |word| (*word).to_string())
}

#[async_trait]
impl ModelProvider for ChatClassifierProvider {
    fn descriptor(&self) -> &ProviderDescriptor {
        &self.descriptor
    }

    fn validate_configuration(&self) -> Result<(), ModelError> {
        self.chat.validate_configuration()
    }

    fn credential_fingerprint(&self) -> Option<String> {
        self.chat.credential_fingerprint()
    }

    fn credential_boundary(&self) -> Option<&CredentialBoundary> {
        self.chat.credential_boundary()
    }
}

#[async_trait]
impl ClassifierProvider for ChatClassifierProvider {
    async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
        let result = (async {
            request.validate()?;
            let started = Instant::now();
            let response = self
                .chat
                .chat(ChatRequest {
                    messages: vec![
                        ChatMessage {
                            role: "system".to_string(),
                            content: Self::render_system_prompt(&request),
                        },
                        ChatMessage {
                            role: "user".to_string(),
                            content: Self::render_user_message(&request),
                        },
                    ],
                    max_output_tokens: self.max_output_tokens,
                    temperature: Some(0.0),
                    response_format: Some("json_object".to_string()),
                    role_binding: request.role_binding.clone(),
                    source: request.source.clone(),
                    metadata: request.metadata.clone(),
                })
                .await?;
            let answers = Self::parse_reply(&request, &response.text)?;
            let served_model = response
                .trace
                .metadata
                .pointer("/provider/served_model")
                .and_then(Value::as_str)
                .map_or_else(
                    || self.descriptor.identity.model.0.clone(),
                    ToString::to_string,
                );
            let mut trace = response.trace;
            trace.model = self.descriptor.identity.clone();
            trace.request_hash = hash_json(&request)?;
            trace
                .timing
                .provider_ms
                .get_or_insert(started.elapsed().as_millis() as u64);
            Ok(ClassifyResponse {
                answers,
                served_model,
                trace,
                raw_provider_response: response.raw_provider_response,
            })
        })
        .await;
        secrets::composed_result(self, result)
    }
}

// ---------------------------------------------------------------------------
// Static test provider
// ---------------------------------------------------------------------------

/// Classifier that returns scripted answers, for tests. Each question needs
/// an answer with its id and kind; Choice answers must use requested option
/// ids.
#[derive(Clone)]
pub struct StaticClassifierProvider {
    descriptor: ProviderDescriptor,
    answers: Vec<ClassifierAnswer>,
}

impl StaticClassifierProvider {
    pub fn new(answers: impl IntoIterator<Item = ClassifierAnswer>) -> Self {
        Self {
            descriptor: ProviderDescriptor {
                identity: ModelIdentity::new("classify", "static", "static-classifier-v1"),
                provider_class: ProviderClass::Local,
                capabilities: vec![ModelCapability::Classify],
                auth_mode: ProviderAuthMode::None,
                metadata: serde_json::json!({}),
            },
            answers: answers.into_iter().collect(),
        }
    }
}

#[async_trait]
impl ModelProvider for StaticClassifierProvider {
    fn descriptor(&self) -> &ProviderDescriptor {
        &self.descriptor
    }
}

#[async_trait]
impl ClassifierProvider for StaticClassifierProvider {
    async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
        request.validate()?;
        let started = Instant::now();
        let mut answers = Vec::with_capacity(request.questions.len());
        for question in &request.questions {
            let answer = self
                .answers
                .iter()
                .find(|answer| answer.question_id == question.id)
                .ok_or_else(|| {
                    ModelError::Provider(symbiotic_core::DiagnosticCode::ProviderFailure)
                })?;
            answers.push(answer.clone());
        }
        validate_answers(&request, &answers)?;
        let text = serde_json::to_string(&answers).map_err(|_err| {
            ModelError::Provider(symbiotic_core::DiagnosticCode::ProviderFailure)
        })?;
        Ok(ClassifyResponse {
            answers,
            served_model: self.descriptor.identity.model.0.clone(),
            trace: classify_trace(&self.descriptor, &request, &text, started)?,
            raw_provider_response: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Mutex;
    #[cfg(feature = "queue")]
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(feature = "queue")]
    use symbiotic_queue_sqlite::SqliteQueue;
    #[cfg(feature = "queue")]
    use symbiotic_trace::InMemoryTraceSink;

    // -- Loopback HTTP server ------------------------------------------------

    /// One scripted HTTP response: status, extra headers, body.
    type Scripted = (u16, Vec<(&'static str, &'static str)>, String);

    struct MockHttp {
        base_url: String,
        /// (request head, request body) per received request.
        requests: Arc<Mutex<Vec<(String, String)>>>,
    }

    /// Serve `responses` in order, one connection each, on a loopback port.
    fn mock_http(responses: Vec<Scripted>) -> MockHttp {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        std::thread::spawn(move || {
            for (status, headers, body) in responses {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                stream.set_nodelay(true).unwrap();
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    let read = stream.read(&mut chunk).unwrap();
                    buffer.extend_from_slice(&chunk[..read]);
                    if let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                    if read == 0 {
                        return;
                    }
                };
                let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
                let length = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                while buffer.len() < head_end + length {
                    let read = stream.read(&mut chunk).unwrap();
                    if read == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..read]);
                }
                let request_body = String::from_utf8_lossy(&buffer[head_end..]).to_string();
                seen.lock().unwrap().push((head, request_body));
                let mut response = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
                    body.len()
                );
                for (name, value) in headers {
                    response.push_str(&format!("{name}: {value}\r\n"));
                }
                response.push_str("\r\n");
                response.push_str(&body);
                stream.write_all(response.as_bytes()).unwrap();
                let _ = stream.flush();
            }
        });
        MockHttp { base_url, requests }
    }

    fn ok(body: Value) -> Scripted {
        (200, Vec::new(), body.to_string())
    }

    // -- Fixtures ------------------------------------------------------------

    fn route_question() -> ClassifierQuestion {
        ClassifierQuestion::choice(
            "route",
            "How should the assistant handle `message`?",
            [
                ("quick", "Reply briefly."),
                ("short_task", "One deliverable."),
                ("goal", "A multi-step project."),
            ],
        )
    }

    fn goal_question() -> ClassifierQuestion {
        ClassifierQuestion::noul(
            "goal",
            "Does `message` need several executed steps?",
            Some("Several steps.".to_string()),
            Some("One reply.".to_string()),
        )
    }

    fn request(questions: Vec<ClassifierQuestion>) -> ClassifyRequest {
        let mut state = serde_json::Map::new();
        state.insert("message".to_string(), serde_json::json!("hello there"));
        let mut request = ClassifyRequest::new(state, questions);
        request.role_binding = Some("stream.route".to_string());
        request.source = Some("test".to_string());
        request
    }

    fn jev_body(model: &str) -> Value {
        serde_json::json!({
            "model": model,
            "answers": {
                "goal": {"type": "noul", "noul": 0.12},
                "route": {
                    "type": "choice",
                    "choice": "quick",
                    "probabilities": {"short_task": 0.25, "goal": 0.05, "quick": 0.7},
                    "confidence": 0.6
                }
            },
            "usage": {"input_tokens": 612, "output_tokens": 20}
        })
    }

    fn jev_at(server: &MockHttp) -> JevClassifierProvider {
        JevClassifierProvider::new("typesafe", JEV_DEFAULT_MODEL, &server.base_url, "test-key")
            .with_request_limit(65536)
            .with_response_limit(65536)
    }

    fn unreachable_jev() -> JevClassifierProvider {
        // Nothing listens on the loopback discard port.
        JevClassifierProvider::new("typesafe", JEV_DEFAULT_MODEL, "http://127.0.0.1:9/v1", "")
            .with_request_limit(65536)
            .with_response_limit(65536)
    }

    #[test]
    fn failed_http_client_construction_refuses_classifier_configuration() {
        let mut provider = unreachable_jev();
        provider.client = HttpClient(Err(()));
        assert!(matches!(
            provider.validate_configuration(),
            Err(ModelError::InvalidRequest(
                symbiotic_core::DiagnosticCode::InvalidHttpClientConfiguration
            ))
        ));
    }

    // -- Types ---------------------------------------------------------------

    #[test]
    fn questions_and_answers_serialize_with_a_type_tag() {
        let question = serde_json::to_value(route_question()).unwrap();
        assert_eq!(question["id"], "route");
        assert_eq!(question["type"], "choice");
        assert_eq!(question["options"][2]["id"], "goal");
        let back: ClassifierQuestion = serde_json::from_value(question).unwrap();
        assert_eq!(back, route_question());

        let answer = serde_json::to_string(&ClassifierAnswer::noul("goal", 0.25)).unwrap();
        assert_eq!(
            answer,
            r#"{"question_id":"goal","value":{"noul":{"probability":0.25}}}"#
        );
        assert_eq!(
            serde_json::to_value(ModelCapability::Classify).unwrap(),
            "classify"
        );
    }

    #[tokio::test]
    async fn classify_responses_round_trip_through_json() {
        // The response cache stores responses as JSON. Numbers must survive
        // serde_json's `arbitrary_precision`, which another workspace crate
        // enables and which breaks flattened numeric fields.
        let response = StaticClassifierProvider::new([
            ClassifierAnswer::noul("goal", 0.45),
            ClassifierAnswer::choice(
                "route",
                [("quick", 0.6), ("short_task", 0.3), ("goal", 0.1)],
            ),
        ])
        .classify(request(vec![route_question(), goal_question()]))
        .await
        .unwrap();
        let json = serde_json::to_string(&response).unwrap();
        let back: ClassifyResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.answers, response.answers);
        let request = request(vec![route_question(), goal_question()]);
        let back: ClassifyRequest =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back.questions, request.questions);
    }

    #[test]
    fn validate_rejects_malformed_requests() {
        let bad = [
            request(Vec::new()),
            request(vec![goal_question(), goal_question()]),
            request(vec![ClassifierQuestion::choice(
                "c",
                "?",
                Vec::<(String, String)>::new(),
            )]),
            request(vec![ClassifierQuestion::choice(
                "c",
                "?",
                [("a", ""), ("a", "")],
            )]),
            request(vec![ClassifierQuestion::score("s", "?", ["only one"])]),
        ];
        for request in bad {
            assert!(
                matches!(request.validate(), Err(ModelError::InvalidRequest(_))),
                "{:?}",
                request.questions
            );
        }
        request(vec![route_question(), goal_question()])
            .validate()
            .unwrap();
    }

    #[test]
    fn decision_helpers_apply_caller_thresholds() {
        let response = ClassifyResponse {
            answers: vec![
                ClassifierAnswer::noul("goal", 0.45),
                ClassifierAnswer::choice(
                    "category",
                    [("invoice", 0.5), ("receipt", 0.2), ("none", 0.3)],
                ),
                ClassifierAnswer::choice("tie", [("a", 0.4), ("none", 0.4), ("b", 0.2)]),
            ],
            served_model: "m".into(),
            trace: success_trace(
                StaticClassifierProvider::new([]).descriptor(),
                None,
                None,
                String::new(),
                None,
            ),
            raw_provider_response: None,
        };
        assert_eq!(response.noul_at_least("goal", 0.45), Some(true));
        assert_eq!(response.noul_at_least("goal", 0.46), Some(false));
        assert_eq!(response.noul_at_least("missing", 0.1), None);
        assert_eq!(response.chosen("category"), Some("invoice"));
        assert_eq!(
            response.decide_choice("category", Some("none"), 0.4),
            Some(ChoiceDecision::Selected {
                option: "invoice".into(),
                probability: 0.5
            })
        );
        assert_eq!(
            response.decide_choice("category", Some("none"), 0.6),
            Some(ChoiceDecision::Abstained),
            "below the minimum probability"
        );
        assert_eq!(
            response.decide_choice("tie", Some("none"), 0.1),
            Some(ChoiceDecision::Selected {
                option: "a".into(),
                probability: 0.4
            }),
            "at a tie with the abstain option the chosen option decides"
        );
        assert_eq!(
            response.decide_choice("tie", None, 0.1),
            Some(ChoiceDecision::Selected {
                option: "a".into(),
                probability: 0.4
            })
        );
        assert_eq!(response.decide_choice("goal", None, 0.1), None);
    }

    // -- Jev -----------------------------------------------------------------

    #[test]
    fn jev_request_body_keeps_question_and_option_order() {
        let request = request(vec![route_question(), goal_question()]);
        let body = serde_json::to_string(&JevWireRequest {
            model: JEV_DEFAULT_MODEL,
            state: &request.state,
            questions: &request.questions,
        })
        .unwrap();
        assert_eq!(
            body,
            concat!(
                r#"{"model":"jev-1.13.0","state":{"message":"hello there"},"questions":{"#,
                r#""route":{"type":"choice","instructions":"How should the assistant handle `message`?","#,
                r#""criteria":{"quick":"Reply briefly.","short_task":"One deliverable.","goal":"A multi-step project."}},"#,
                r#""goal":{"type":"noul","instructions":"Does `message` need several executed steps?","#,
                r#""criteria":{"true":"Several steps.","false":"One reply."}}}}"#
            )
        );
        let bare = ClassifierQuestion::noul("urgent", "Does this convey urgency?", None, None);
        assert_eq!(
            serde_json::to_value(WireQuestion(&bare)).unwrap(),
            serde_json::json!({"type": "noul", "instructions": "Does this convey urgency?"})
        );
        let score = ClassifierQuestion::score("f", "How frustrated?", ["Calm", "Angry"]);
        assert_eq!(
            serde_json::to_value(WireQuestion(&score)).unwrap(),
            serde_json::json!({"type": "score", "instructions": "How frustrated?", "criteria": ["Calm", "Angry"]})
        );
    }

    #[tokio::test]
    async fn jev_posts_to_systemone_with_bearer_key_and_parses_answers() {
        let server = mock_http(vec![ok(jev_body(JEV_DEFAULT_MODEL))]);
        let response = jev_at(&server)
            .classify(request(vec![route_question(), goal_question()]))
            .await
            .unwrap();

        let requests = server.requests.lock().unwrap();
        let (head, body) = &requests[0];
        assert!(head.starts_with("POST /v1/systemone "), "{head}");
        assert!(
            head.to_ascii_lowercase()
                .contains("authorization: bearer test-key"),
            "{head}"
        );
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["model"], "jev-1.13.0");
        assert_eq!(body["state"], serde_json::json!({"message": "hello there"}));
        assert_eq!(
            body["questions"]["route"]["criteria"]["goal"],
            "A multi-step project."
        );

        let ids: Vec<&str> = response
            .answers
            .iter()
            .map(|a| a.question_id.as_str())
            .collect();
        assert_eq!(ids, ["route", "goal"], "answers follow the request order");
        assert_eq!(response.served_model, "jev-1.13.0");
        assert_eq!(response.noul("goal"), Some(0.12));
        assert_eq!(response.chosen("route"), Some("quick"));
        assert_eq!(
            response.choice_probability("route", "short_task"),
            Some(0.25)
        );
        match response.answer("route").unwrap() {
            AnswerValue::Choice {
                probabilities,
                confidence,
                ..
            } => {
                let order: Vec<&str> = probabilities.iter().map(|p| p.id.as_str()).collect();
                assert_eq!(order, ["quick", "short_task", "goal"]);
                assert_eq!(*confidence, Some(0.6));
            }
            other => panic!("expected a choice answer, got {other:?}"),
        }
        let trace = &response.trace;
        assert_eq!(trace.model.queue_id().0, "classify:typesafe:jev-1.13.0");
        assert_eq!(trace.usage.input_tokens, Some(612));
        assert_eq!(trace.usage.output_tokens, Some(20));
        assert!(trace.timing.provider_ms.is_some(), "latency is recorded");
        assert_eq!(trace.role_binding.as_deref(), Some("stream.route"));
        assert_eq!(
            trace.metadata.pointer("/provider/served_model"),
            Some(&serde_json::json!("jev-1.13.0"))
        );
    }

    #[tokio::test]
    async fn jev_parses_score_answers() {
        let server = mock_http(vec![ok(serde_json::json!({
            "model": "jev-1.13.0",
            "answers": {"f": {
                "type": "score", "score": 1.05,
                "legend": {"0": "Calm", "1": "Frustrated", "2": "Very angry"},
                "probabilities": {"0": 0.0, "1": 0.95, "2": 0.05},
                "confidence": 0.92
            }},
            "usage": {"input_tokens": 304, "output_tokens": 18}
        }))]);
        let response = jev_at(&server)
            .classify(request(vec![ClassifierQuestion::score(
                "f",
                "How frustrated?",
                ["Calm", "Frustrated", "Very angry"],
            )]))
            .await
            .unwrap();
        assert_eq!(
            response.answer("f"),
            Some(&AnswerValue::Score {
                value: 1.05,
                probabilities: vec![0.0, 0.95, 0.05],
                confidence: Some(0.92),
            })
        );
    }

    #[tokio::test]
    async fn jev_rejects_a_different_served_model_unless_configured() {
        // A gateway (OpenRouter's `/systemone`) reports a dated snapshot and
        // adds id, provider and usage.cost.
        let mut body = jev_body("typesafe/jev-1.13-20260917");
        body["id"] = serde_json::json!("gen-dec-1");
        body["provider"] = serde_json::json!("TypeSafe");
        body["usage"]["cost"] = serde_json::json!(0.00002);
        let server = mock_http(vec![ok(body.clone()), ok(body)]);
        let gateway = JevClassifierProvider::new(
            "openrouter",
            "typesafe/jev-1.13",
            &server.base_url,
            "or-key",
        )
        .with_request_limit(65536)
        .with_response_limit(65536);

        let err = gateway
            .clone()
            .classify(request(vec![route_question(), goal_question()]))
            .await
            .unwrap_err();
        assert!(matches!(err, ModelError::Provider(_)), "{err:?}");
        #[cfg(feature = "queue")]
        assert!(!is_retryable(&err, &ModelQueueConfig::default()));

        let response = gateway
            .with_served_model("typesafe/jev-1.13-20260917")
            .classify(request(vec![route_question(), goal_question()]))
            .await
            .unwrap();
        assert_eq!(response.served_model, "typesafe/jev-1.13-20260917");
        assert_eq!(
            response.trace.model.queue_id().0,
            "classify:openrouter:typesafe/jev-1.13"
        );
        assert_eq!(
            response
                .trace
                .metadata
                .pointer("/provider/reported_cost_usd"),
            Some(&serde_json::json!("0.00002"))
        );
        assert_eq!(
            response.trace.usage.reported_cost_usd.as_deref(),
            Some("0.00002")
        );
        assert_eq!(
            response.trace.metadata.pointer("/provider/response_id"),
            Some(&serde_json::json!("gen-dec-1"))
        );
    }

    /// A literal HTTP body: numbers keep the provider's spelling (`0.1200`,
    /// `5e-2`), which a float fixture would canonicalize.
    fn literal(body: &str) -> Scripted {
        (200, Vec::new(), body.to_string())
    }

    #[tokio::test]
    async fn jev_parses_noncanonical_number_spellings() {
        let server = mock_http(vec![literal(
            r#"{"model":"jev-1.13.0","answers":{
                "goal":{"type":"noul","noul":0.1200},
                "route":{"type":"choice","choice":"quick",
                         "probabilities":{"quick":0.70,"short_task":0.250,"goal":5e-2},
                         "confidence":0.60},
                "f":{"type":"score","score":1.050,
                     "legend":{"0":"Calm","1":"Frustrated","2":"Very angry"},
                     "probabilities":{"0":0.0,"1":0.950,"2":0.050},"confidence":0.920}},
               "usage":{"input_tokens":612,"output_tokens":20}}"#,
        )]);
        let response = jev_at(&server)
            .classify(request(vec![
                route_question(),
                goal_question(),
                ClassifierQuestion::score(
                    "f",
                    "How frustrated?",
                    ["Calm", "Frustrated", "Very angry"],
                ),
            ]))
            .await
            .unwrap();
        assert_eq!(response.noul("goal"), Some(0.12));
        assert_eq!(response.choice_probability("route", "goal"), Some(0.05));
        assert_eq!(response.chosen("route"), Some("quick"));
        let score = response.score("f").unwrap();
        assert!((score - 1.05).abs() < 1e-9, "{score}");
        assert_eq!(response.trace.usage.input_tokens, Some(612));
    }

    fn jev_route_answer(route: &str) -> Scripted {
        literal(&format!(
            r#"{{"model":"jev-1.13.0","answers":{{"goal":{{"type":"noul","noul":0.1}},"route":{route}}}}}"#
        ))
    }

    #[tokio::test]
    async fn jev_answers_must_be_consistent_distributions() {
        let bad_routes = [
            // Chosen option is not the most probable one.
            r#"{"type":"choice","choice":"quick","probabilities":{"quick":0.1,"short_task":0.9,"goal":0.0}}"#,
            // Probabilities do not sum to 1.
            r#"{"type":"choice","choice":"quick","probabilities":{"quick":0.1,"short_task":0.1,"goal":0.1}}"#,
            // Confidence outside [0, 1] or not a number.
            r#"{"type":"choice","choice":"quick","probabilities":{"quick":0.7,"short_task":0.2,"goal":0.1},"confidence":1.5}"#,
            r#"{"type":"choice","choice":"quick","probabilities":{"quick":0.7,"short_task":0.2,"goal":0.1},"confidence":"high"}"#,
            // Chosen option at a displayed tie must be one of the tied maxima.
            r#"{"type":"choice","choice":"goal","probabilities":{"quick":0.4,"short_task":0.4,"goal":0.2}}"#,
        ];
        for route in bad_routes {
            let server = mock_http(vec![jev_route_answer(route)]);
            let err = jev_at(&server)
                .classify(request(vec![route_question(), goal_question()]))
                .await
                .unwrap_err();
            assert!(
                matches!(err, ModelError::Provider(_)),
                "{route} gave {err:?}"
            );
        }
        let score_question =
            ClassifierQuestion::score("f", "How frustrated?", ["Calm", "Frustrated", "Very angry"]);
        let bad_scores = [
            r#"{"type":"score","score":999,"probabilities":{"0":0.0,"1":0.95,"2":0.05}}"#,
            // Reported score far from the probability-weighted level.
            r#"{"type":"score","score":2.0,"probabilities":{"0":1.0,"1":0.0,"2":0.0}}"#,
            r#"{"type":"score","score":1.0,"probabilities":{"0":0.2,"1":0.2,"2":0.2}}"#,
            r#"{"type":"score","score":1.05,"probabilities":{"0":0.0,"1":0.95,"2":0.05},"confidence":-0.1}"#,
        ];
        for score in bad_scores {
            let server = mock_http(vec![literal(&format!(
                r#"{{"model":"jev-1.13.0","answers":{{"f":{score}}}}}"#
            ))]);
            let err = jev_at(&server)
                .classify(request(vec![score_question.clone()]))
                .await
                .unwrap_err();
            assert!(
                matches!(err, ModelError::Provider(_)),
                "{score} gave {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn jev_tie_keeps_the_providers_choice_and_decisions_agree() {
        // Jev rounds probabilities to two decimals; at a displayed tie its
        // choice reflects the unrounded values and may be the later option.
        let server = mock_http(vec![jev_route_answer(
            r#"{"type":"choice","choice":"short_task","probabilities":{"quick":0.4,"short_task":0.4,"goal":0.2}}"#,
        )]);
        let response = jev_at(&server)
            .classify(request(vec![route_question(), goal_question()]))
            .await
            .unwrap();
        assert_eq!(response.chosen("route"), Some("short_task"));
        assert_eq!(
            response.decide_choice("route", None, 0.0),
            Some(ChoiceDecision::Selected {
                option: "short_task".into(),
                probability: 0.4
            })
        );
    }

    #[tokio::test]
    async fn chosen_and_decide_choice_agree_at_abstain_ties() {
        let answer = |chosen: &str| ClassifyResponse {
            answers: vec![ClassifierAnswer {
                question_id: "category".into(),
                value: AnswerValue::Choice {
                    chosen: chosen.into(),
                    probabilities: vec![
                        OptionProbability {
                            id: "invoice".into(),
                            probability: 0.4,
                        },
                        OptionProbability {
                            id: "none".into(),
                            probability: 0.4,
                        },
                        OptionProbability {
                            id: "receipt".into(),
                            probability: 0.2,
                        },
                    ],
                    confidence: None,
                },
            }],
            served_model: "m".into(),
            trace: success_trace(
                StaticClassifierProvider::new([]).descriptor(),
                None,
                None,
                String::new(),
                None,
            ),
            raw_provider_response: None,
        };
        assert_eq!(
            answer("invoice").decide_choice("category", Some("none"), 0.1),
            Some(ChoiceDecision::Selected {
                option: "invoice".into(),
                probability: 0.4
            })
        );
        assert_eq!(
            answer("none").decide_choice("category", Some("none"), 0.1),
            Some(ChoiceDecision::Abstained)
        );
    }

    #[tokio::test]
    async fn static_answers_must_be_consistent() {
        let inconsistent = ClassifierAnswer {
            question_id: "route".into(),
            value: AnswerValue::Choice {
                chosen: "quick".into(),
                probabilities: vec![
                    OptionProbability {
                        id: "quick".into(),
                        probability: 0.1,
                    },
                    OptionProbability {
                        id: "short_task".into(),
                        probability: 0.9,
                    },
                    OptionProbability {
                        id: "goal".into(),
                        probability: 0.0,
                    },
                ],
                confidence: None,
            },
        };
        let err = StaticClassifierProvider::new([inconsistent])
            .classify(request(vec![route_question()]))
            .await
            .unwrap_err();
        assert!(matches!(err, ModelError::Provider(_)), "{err:?}");
    }

    #[tokio::test]
    async fn static_answers_with_non_finite_values_are_rejected() {
        let score = ClassifierQuestion::score("f", "How frustrated?", ["Calm", "Angry"]);
        let answers = [
            AnswerValue::Score {
                value: f64::NAN,
                probabilities: vec![0.5, 0.5],
                confidence: None,
            },
            AnswerValue::Score {
                value: 0.5,
                probabilities: vec![f64::NAN, 0.5],
                confidence: None,
            },
            AnswerValue::Score {
                value: 0.5,
                probabilities: vec![0.5, 0.5],
                confidence: Some(f64::NAN),
            },
        ];
        for value in answers {
            let err = StaticClassifierProvider::new([ClassifierAnswer {
                question_id: "f".into(),
                value: value.clone(),
            }])
            .classify(request(vec![score.clone()]))
            .await
            .unwrap_err();
            assert!(
                matches!(err, ModelError::Provider(_)),
                "{value:?} gave {err:?}"
            );
        }
    }

    #[cfg(feature = "queue")]
    /// A classifier with a configurable identity and served model.
    #[derive(Clone)]
    struct NamedClassifier {
        descriptor: ProviderDescriptor,
        probability: f64,
        calls: Arc<AtomicUsize>,
    }

    #[cfg(feature = "queue")]
    impl NamedClassifier {
        fn new(model: &str, served_model: &str, probability: f64) -> Self {
            Self {
                descriptor: ProviderDescriptor {
                    identity: ModelIdentity::new("classify", "typesafe", model),
                    provider_class: ProviderClass::Cloud,
                    capabilities: vec![ModelCapability::Classify],
                    auth_mode: ProviderAuthMode::None,
                    metadata: serde_json::json!({ "served_model": served_model }),
                },
                probability,
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[cfg(feature = "queue")]
    #[async_trait]
    impl ModelProvider for NamedClassifier {
        fn descriptor(&self) -> &ProviderDescriptor {
            &self.descriptor
        }
    }

    #[cfg(feature = "queue")]
    #[async_trait]
    impl ClassifierProvider for NamedClassifier {
        async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let answers = vec![ClassifierAnswer::noul("goal", self.probability)];
            Ok(ClassifyResponse {
                served_model: self.descriptor.identity.model.0.clone(),
                trace: classify_trace(&self.descriptor, &request, "x", Instant::now())?,
                answers,
                raw_provider_response: None,
            })
        }
    }

    #[cfg(feature = "queue")]
    #[tokio::test(start_paused = true)]
    async fn queued_classifier_cache_is_scoped_to_the_provider_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Some(dir.path().join("cache"));
        let queued = |provider: NamedClassifier| {
            QueuedClassifierProvider::new(
                provider,
                Arc::new(
                    SqliteQueue::in_memory()
                        .unwrap()
                        .with_clock(crate::test_clock::clock()),
                ),
                "worker",
                queue_config(cache.clone()),
            )
            .with_spend_ledger(crate::test_spend::ledger(), None)
        };
        let a = NamedClassifier::new("jev-1.13.0", "jev-1.13.0", 0.1);
        let other_model = NamedClassifier::new("jev-1.14.0", "jev-1.14.0", 0.8);
        let other_snapshot = NamedClassifier::new("jev-1.13.0", "jev-1.13.0-20260917", 0.6);
        let request = || request(vec![goal_question()]);

        assert_eq!(
            queued(a.clone())
                .classify(request())
                .await
                .unwrap()
                .noul("goal"),
            Some(0.1)
        );
        assert_eq!(
            queued(other_model.clone())
                .classify(request())
                .await
                .unwrap()
                .noul("goal"),
            Some(0.8),
            "another model must not read the first model's cached answer"
        );
        assert_eq!(
            queued(other_snapshot.clone())
                .classify(request())
                .await
                .unwrap()
                .noul("goal"),
            Some(0.6),
            "another expected snapshot must not read it either"
        );
        // The same configuration still hits its own entry.
        assert_eq!(
            queued(a.clone())
                .classify(request())
                .await
                .unwrap()
                .noul("goal"),
            Some(0.1)
        );
        assert_eq!(a.calls.load(Ordering::SeqCst), 1);
        assert_eq!(other_model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(other_snapshot.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn jev_malformed_answers_are_provider_errors() {
        let full_route = serde_json::json!({"type": "choice", "choice": "quick",
            "probabilities": {"quick": 1.0, "short_task": 0.0, "goal": 0.0}});
        let bodies = [
            serde_json::json!({"model": "jev-1.13.0", "answers": {"route": full_route}}),
            serde_json::json!({"model": "jev-1.13.0", "answers": {"route": full_route,
                "goal": {"type": "choice", "choice": "x", "probabilities": {"x": 1.0}}}}),
            serde_json::json!({"model": "jev-1.13.0", "answers": {"route": full_route,
                "goal": {"type": "noul", "noul": 1.7}}}),
            serde_json::json!({"model": "jev-1.13.0", "answers": {
                "route": {"type": "choice", "choice": "finance.incoming_invoice",
                          "probabilities": {"quick": 1.0, "short_task": 0.0, "goal": 0.0}},
                "goal": {"type": "noul", "noul": 0.1}}}),
            serde_json::json!({"model": "jev-1.13.0", "answers": {"route": full_route,
                "goal": {"type": "noul", "noul": 0.1}, "extra": {"type": "noul", "noul": 0.1}}}),
            serde_json::json!("not an object"),
        ];
        for body in bodies {
            let server = mock_http(vec![ok(body.clone())]);
            let err = jev_at(&server)
                .classify(request(vec![route_question(), goal_question()]))
                .await
                .unwrap_err();
            assert!(
                matches!(err, ModelError::Provider(_)),
                "{body} gave {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn jev_bounds_success_bodies_and_classifies_error_status_without_body() {
        for status in [200, 429] {
            let server = mock_http(vec![(status, vec![], "x".repeat(4096))]);
            let provider = jev_at(&server).with_response_limit(1024);
            let error = provider
                .classify(request(vec![goal_question()]))
                .await
                .unwrap_err();
            if status == 429 {
                assert!(matches!(error.primary(), ModelError::RateLimited(_)));
            } else {
                assert!(matches!(
                    error,
                    ModelError::Provider(
                        symbiotic_core::DiagnosticCode::ProviderResponseLimitExceeded
                    )
                ));
            }
        }
    }
    #[tokio::test]
    async fn jev_encoded_request_limit_refuses_before_connecting() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let provider = JevClassifierProvider::new(
            "typesafe",
            JEV_DEFAULT_MODEL,
            format!("http://{}", listener.local_addr().unwrap()),
            "synthetic",
        )
        .with_request_limit(1)
        .with_response_limit(1024);
        assert!(matches!(
            provider.classify(request(vec![goal_question()])).await,
            Err(ModelError::InvalidRequest(_))
        ));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn jev_http_statuses_use_the_shared_retry_classification() {
        let cases: [(u16, &str, bool); 10] = [
            (408, "timeout", true),
            (429, "rate_limited", true),
            (500, "unavailable", true),
            (503, "unavailable", true),
            (504, "timeout", true),
            (520, "unavailable", true),
            (529, "unavailable", true),
            (401, "auth", false),
            (402, "budget", false),
            (422, "provider", false),
        ];
        for (status, expected, retryable) in cases {
            let server = mock_http(vec![(status, vec![("retry-after", "2")], "{}".to_string())]);
            let err = jev_at(&server)
                .classify(request(vec![goal_question()]))
                .await
                .unwrap_err();
            let got = match err.primary() {
                ModelError::Timeout(_) => "timeout",
                ModelError::RateLimited(_) => "rate_limited",
                ModelError::Unavailable(_) => "unavailable",
                ModelError::Auth(_) => "auth",
                ModelError::BudgetExhausted(_) => "budget",
                ModelError::Provider(_) => "provider",
                ref other => panic!("HTTP {status}: unexpected {other:?}"),
            };
            assert_eq!(got, expected, "HTTP {status}");
            // Retry classification belongs to the queue runtime.
            #[cfg(feature = "queue")]
            assert_eq!(
                is_retryable(&err, &ModelQueueConfig::default()),
                retryable,
                "HTTP {status}"
            );
            #[cfg(not(feature = "queue"))]
            let _ = retryable;
        }
    }

    #[tokio::test]
    async fn jev_unreachable_endpoint_is_unavailable() {
        let err = unreachable_jev()
            .classify(request(vec![goal_question()]))
            .await
            .unwrap_err();
        assert!(
            matches!(err.primary(), ModelError::Unavailable(_)),
            "{err:?}"
        );
    }

    #[test]
    fn request_encoding_jev_token_bounds_keep_the_exact_pair_and_total_limits() {
        let mut req = request(vec![goal_question()]);
        let question_bytes = serde_json::to_vec(&WireQuestion(&req.questions[0]))
            .unwrap()
            .len();
        let empty_state_bytes = serde_json::to_vec(&serde_json::json!({"message": ""}))
            .unwrap()
            .len();
        let state_chars = JEV_MAX_STATE_AND_LONGEST_QUESTION_TOKENS as usize
            - JEV_TEMPLATE_RESERVE_TOKENS as usize
            - question_bytes
            - empty_state_bytes;
        req.state
            .insert("message".into(), Value::String("x".repeat(state_chars)));
        assert!(JevClassifierProvider::check_limits(&req).is_ok());
        req.state
            .insert("message".into(), Value::String("x".repeat(state_chars + 1)));
        assert_eq!(
            JevClassifierProvider::check_limits(&req)
                .unwrap_err()
                .code(),
            DiagnosticCode::InvalidConfiguration
        );

        let mut req = ClassifyRequest::new(
            serde_json::Map::new(),
            (0..3)
                .map(|i| ClassifierQuestion::noul(format!("q{i}"), "", None, None))
                .collect(),
        );
        let overhead = 2 + 3 * serde_json::to_vec(&WireQuestion(&req.questions[0]))
            .unwrap()
            .len();
        let chars =
            JEV_MAX_REQUEST_TOKENS as usize - JEV_TEMPLATE_RESERVE_TOKENS as usize - overhead;
        for question in &mut req.questions {
            question.instructions = "x".repeat(chars / 3);
        }
        req.questions[0]
            .instructions
            .push_str(&"x".repeat(chars % 3));
        assert!(JevClassifierProvider::check_limits(&req).is_ok());
        req.questions[0].instructions.push('x');
        assert_eq!(
            JevClassifierProvider::check_limits(&req)
                .unwrap_err()
                .code(),
            DiagnosticCode::InvalidConfiguration
        );
    }

    #[tokio::test]
    async fn jev_limits_are_refused_before_sending() {
        let too_many_options = ClassifierQuestion::choice(
            "pick",
            "Pick one.",
            (0..=JEV_MAX_CHOICE_OPTIONS).map(|i| (format!("o{i}"), String::new())),
        );
        let too_many_levels =
            ClassifierQuestion::score("rate", "Rate.", (0..11).map(|i| format!("level {i}")));
        let mut oversized_state = request(vec![goal_question()]);
        oversized_state
            .state
            .insert("message".to_string(), serde_json::json!("x".repeat(32_000)));
        // ~1 KB each: 70 pass the 32k pair limit but not the 64k request limit.
        let many_questions = request(
            (0..70)
                .map(|i| ClassifierQuestion::noul(format!("q{i}"), "y".repeat(1_000), None, None))
                .collect(),
        );
        for (request, expected) in [
            (request(vec![too_many_options]), "256 choice options"),
            (request(vec![too_many_levels]), "11 score levels"),
            (oversized_state, "state plus longest question"),
            (many_questions, "request is about"),
        ] {
            let err = unreachable_jev().classify(request).await.unwrap_err();
            assert!(
                matches!(err, ModelError::InvalidRequest(_)),
                "{expected}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn jev_from_resolver_uses_the_host_credential() {
        struct Resolver(Option<ResolvedAuth>);
        #[async_trait]
        impl CredentialResolver for Resolver {
            async fn resolve_auth(
                &self,
                mode: &ProviderAuthMode,
            ) -> Result<ResolvedAuth, ModelError> {
                assert_eq!(
                    mode,
                    &ProviderAuthMode::ApiKey {
                        secret_ref: "TYPESAFE_API_KEY".into()
                    }
                );
                self.0.clone().ok_or_else(|| {
                    ModelError::Auth(symbiotic_core::DiagnosticCode::AuthenticationRejected)
                })
            }
        }
        let mode = ProviderAuthMode::ApiKey {
            secret_ref: "TYPESAFE_API_KEY".into(),
        };
        let server = mock_http(vec![ok(jev_body(JEV_DEFAULT_MODEL))]);
        let provider = JevClassifierProvider::from_resolver(
            &Resolver(Some(ResolvedAuth::Bearer("resolved-key".into()))),
            mode.clone(),
            "typesafe",
            JEV_DEFAULT_MODEL,
            &server.base_url,
        )
        .await
        .unwrap()
        .with_request_limit(65536)
        .with_response_limit(65536);
        assert_eq!(provider.descriptor().auth_mode, mode);
        provider
            .classify(request(vec![route_question(), goal_question()]))
            .await
            .unwrap();
        assert!(
            server.requests.lock().unwrap()[0]
                .0
                .to_ascii_lowercase()
                .contains("authorization: bearer resolved-key")
        );
        for resolved in [
            None,
            Some(ResolvedAuth::None),
            Some(ResolvedAuth::ApiKey(" ".into())),
        ] {
            let err = JevClassifierProvider::from_resolver(
                &Resolver(resolved),
                mode.clone(),
                "typesafe",
                JEV_DEFAULT_MODEL,
                TYPESAFE_BASE_URL,
            )
            .await
            .err()
            .unwrap();
            assert!(matches!(err, ModelError::Auth(_)));
        }
    }

    #[test]
    fn rabbithole_jev_encoder_enforces_exact_wire_bound_and_excludes_local_metadata() {
        let request = request(vec![route_question(), goal_question()]);
        let body = jev_classify_body("test-model", &request, None).unwrap();
        assert_eq!(
            jev_classify_body("test-model", &request, Some(body.len())).unwrap(),
            body
        );
        assert!(jev_classify_body("test-model", &request, Some(body.len() - 1)).is_err());
        let text = String::from_utf8(body).unwrap();
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 3);
        assert!(json.get("source").is_none() && json.get("metadata").is_none());
    }

    #[tokio::test]
    async fn regression_jev_supplied_invalid_cache_counters_are_visible_in_egress() {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            for field in ["cache_hit_tokens", "cache_miss_tokens"] {
                for invalid in [
                    serde_json::json!("invalid"),
                    serde_json::json!(null),
                    serde_json::json!(-1),
                    serde_json::json!(1.5),
                    serde_json::json!(true),
                ] {
                    let mut body = jev_body(JEV_DEFAULT_MODEL);
                    body["usage"][field] = invalid;
                    let server = mock_http(vec![ok(body)]);
                    let result = with_egress_http_observations(
                        jev_at(&server).classify(request(vec![goal_question(), route_question()])),
                    )
                    .await;
                    assert!(
                        matches!(
                            result,
                            Err(ModelError::Provider(DiagnosticCode::InvalidResponse))
                        ),
                        "{field}: {result:?}"
                    );
                }
            }
            let server = mock_http(vec![ok(jev_body(JEV_DEFAULT_MODEL))]);
            let response = with_egress_http_observations(
                jev_at(&server).classify(request(vec![goal_question(), route_question()])),
            )
            .await
            .unwrap();
            assert_eq!(response.trace.usage.cache_hit_tokens, None);
            assert_eq!(response.trace.usage.cache_miss_tokens, None);
        })
        .await
        .expect("cache counter fixtures must finish within three seconds");
    }

    #[tokio::test]
    async fn regression_jev_request_echoes_are_not_usage_identities() {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            for location in [
                "state", "instructions", "true", "false", "choice", "score", "local",
            ] {
                let mut req = request(vec![goal_question(), route_question()]);
                match location {
                    "state" => {
                        req.state.insert(
                            "message".into(),
                            serde_json::json!({"type":["prefix PRIVATE_PROMPT suffix"]}),
                        );
                    }
                    "instructions" => {
                        req.questions[0].instructions = "prefix PRIVATE_PROMPT suffix".into();
                    }
                    "true" | "false" => req.questions[0].kind = QuestionKind::Noul {
                        when_true: (location == "true").then(|| "PRIVATE_PROMPT".into()),
                        when_false: (location == "false").then(|| "PRIVATE_PROMPT".into()),
                    },
                    "choice" => {
                        if let QuestionKind::Choice { options } = &mut req.questions[1].kind {
                            options[0].criterion = "PRIVATE_PROMPT".into();
                        }
                    }
                    "score" => req.questions.push(ClassifierQuestion::score(
                        "score",
                        "Rate it",
                        ["PRIVATE_PROMPT", "high"],
                    )),
                    "local" => {
                        req.state_description = Some("PRIVATE_PROMPT".into());
                        req.metadata = serde_json::json!({"text":"PRIVATE_PROMPT"});
                    }
                    _ => unreachable!(),
                }
                let mut body = jev_body(JEV_DEFAULT_MODEL);
                body["id"] = serde_json::json!("PRIVATE_PROMPT");
                if location == "score" {
                    body["answers"]["score"] = serde_json::json!({"type":"score", "score":0.0, "probabilities":{"0":1.0,"1":0.0}});
                }
                let server = mock_http(vec![ok(body)]);
                let response = with_egress_http_observations(
                    jev_at(&server).with_timeout(1).unwrap().classify(req),
                )
                .await
                .expect("screening preserves classification");
                let expected = (location == "local").then_some("PRIVATE_PROMPT");
                assert_eq!(
                    response.trace.usage.response_id.as_deref(),
                    expected,
                    "{location}"
                );
                assert_eq!(
                    response.trace.metadata["provider"]
                        .get("response_id")
                        .and_then(Value::as_str),
                    expected
                );
                assert_eq!(
                    response.trace.usage.served_model.as_deref(),
                    Some(JEV_DEFAULT_MODEL)
                );
                assert_eq!(response.noul("goal"), Some(0.12));
                assert_eq!(response.trace.usage.input_tokens, Some(612));
            }
        })
        .await
        .expect("classifier identity fixtures must finish within three seconds");
    }

    #[tokio::test]
    async fn regression_jev_served_model_matching_local_description_is_accepted() {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let server = mock_http(vec![ok(jev_body(JEV_DEFAULT_MODEL))]);
            let mut req = request(vec![goal_question(), route_question()]);
            req.state_description = Some(JEV_DEFAULT_MODEL.into());
            let response = with_egress_http_observations(jev_at(&server).classify(req))
                .await
                .expect("local content must not screen usage identity");
            assert_eq!(response.served_model, JEV_DEFAULT_MODEL);
            assert_eq!(
                response.trace.usage.served_model.as_deref(),
                Some(JEV_DEFAULT_MODEL)
            );
        })
        .await
        .expect("identity fixture must finish within three seconds");
    }

    #[tokio::test]
    async fn regression_jev_served_model_matching_request_state_is_omitted() {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let server = mock_http(vec![ok(jev_body(JEV_DEFAULT_MODEL))]);
            let mut req = request(vec![goal_question(), route_question()]);
            req.state
                .insert("message".into(), serde_json::json!(JEV_DEFAULT_MODEL));
            let response = with_egress_http_observations(
                jev_at(&server).with_timeout(1).unwrap().classify(req),
            )
            .await
            .expect("screening preserves classification");
            assert_eq!(response.served_model, JEV_DEFAULT_MODEL);
            assert!(response.trace.usage.served_model.is_none());
            assert!(
                response.trace.metadata["provider"]
                    .get("served_model")
                    .is_none()
            );
        })
        .await
        .expect("identity fixture must finish within three seconds");
    }

    #[test]
    fn jev_typesafe_defaults() {
        let provider = JevClassifierProvider::typesafe("k");
        let descriptor = provider.descriptor();
        assert_eq!(
            descriptor.identity.queue_id().0,
            "classify:typesafe:jev-1.13.0"
        );
        assert_eq!(descriptor.capabilities, vec![ModelCapability::Classify]);
        assert_eq!(descriptor.provider_class, ProviderClass::Cloud);
        assert_eq!(provider.base_url, TYPESAFE_BASE_URL);
    }

    // -- Chat-backed classifier ------------------------------------------------

    #[derive(Clone)]
    struct RecordingChat {
        descriptor: ProviderDescriptor,
        reply: String,
        requests: Arc<Mutex<Vec<ChatRequest>>>,
    }

    impl RecordingChat {
        fn new(class: ProviderClass, reply: &str) -> Self {
            Self {
                descriptor: ProviderDescriptor {
                    identity: ModelIdentity::new("chat", "deepseek", "deepseek-v4-flash"),
                    provider_class: class,
                    capabilities: vec![ModelCapability::Chat],
                    auth_mode: ProviderAuthMode::None,
                    metadata: serde_json::json!({}),
                },
                reply: reply.to_string(),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ModelProvider for RecordingChat {
        fn descriptor(&self) -> &ProviderDescriptor {
            &self.descriptor
        }
    }

    #[async_trait]
    impl ChatProvider for RecordingChat {
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ModelError> {
            self.requests.lock().unwrap().push(request.clone());
            let mut trace = success_trace(
                &self.descriptor,
                None,
                None,
                hash_json(&request)?,
                Some(&self.reply),
            );
            trace.usage.input_tokens = Some(700);
            trace.usage.output_tokens = Some(40);
            trace.metadata =
                serde_json::json!({"provider": {"served_model": "deepseek-v4-flash-0927"}});
            Ok(ChatResponse {
                text: self.reply.clone(),
                finish_reason: Some("stop".to_string()),
                trace,
                raw_provider_response: None,
            })
        }
    }

    fn chat_request_fixture() -> ClassifyRequest {
        let mut request = request(vec![
            ClassifierQuestion::choice(
                "route",
                "How should the assistant handle `message`?",
                [("quick", "Reply briefly."), ("goal", "A project.")],
            ),
            goal_question(),
        ]);
        request.state_description =
            Some("a chat message. The user turn is a JSON object.".to_string());
        request
    }

    #[test]
    fn chat_prompt_renders_questions_and_the_exact_reply_shape() {
        assert_eq!(
            ChatClassifierProvider::render_system_prompt(&chat_request_fixture()),
            "You answer two independent questions about a chat message. The user turn is a JSON object.\n\n\
             Question \"route\" (give a probability for each option; they sum to 1). How should the assistant handle `message`?\n\
             Options:\n- quick: Reply briefly.\n- goal: A project.\n\n\
             Question \"goal\" (give the probability from 0 to 1 that the answer is yes). Does `message` need several executed steps?\n\
             Yes means: Several steps.\nNo means: One reply.\n\n\
             Reply with only this JSON object: {\"route\": {\"quick\": <p>, \"goal\": <p>}, \"goal\": <p>}"
        );
        let score = request(vec![ClassifierQuestion::score(
            "f",
            "How frustrated is `message`?",
            ["Calm", "Angry"],
        )]);
        assert_eq!(
            ChatClassifierProvider::render_system_prompt(&score),
            "You answer one question about the JSON object in the user turn.\n\n\
             Question \"f\" (give a probability for each level; they sum to 1). How frustrated is `message`?\n\
             Levels:\n- 0: Calm\n- 1: Angry\n\n\
             Reply with only this JSON object: {\"f\": {\"0\": <p>, \"1\": <p>}}"
        );
    }

    #[tokio::test]
    async fn chat_classifier_sends_one_json_mode_completion_and_normalises() {
        let chat = RecordingChat::new(
            ProviderClass::Local,
            r#"{"route": {"quick": 0.2, "goal": 0.6}, "goal": 0.9}"#,
        );
        let classifier =
            ChatClassifierProvider::new(Arc::new(chat.clone())).with_max_output_tokens(512);
        let request = chat_request_fixture();
        let response = classifier.classify(request.clone()).await.unwrap();

        let sent = chat.requests.lock().unwrap()[0].clone();
        assert_eq!(sent.response_format.as_deref(), Some("json_object"));
        assert_eq!(sent.max_output_tokens, Some(512));
        assert_eq!(sent.temperature, Some(0.0));
        assert_eq!(sent.messages[0].role, "system");
        assert_eq!(
            sent.messages[0].content,
            ChatClassifierProvider::render_system_prompt(&request)
        );
        assert_eq!(sent.messages[1].content, r#"{"message":"hello there"}"#);

        let descriptor = classifier.descriptor();
        assert_eq!(
            descriptor.identity.queue_id().0,
            "classify:deepseek:deepseek-v4-flash"
        );
        assert_eq!(
            descriptor.provider_class,
            ProviderClass::Local,
            "class follows the chat model"
        );
        assert_eq!(response.served_model, "deepseek-v4-flash-0927");
        assert_eq!(response.trace.model, descriptor.identity);
        assert_eq!(response.trace.usage.input_tokens, Some(700));
        assert_eq!(response.noul("goal"), Some(0.9));
        assert_eq!(response.chosen("route"), Some("goal"));
        let quick = response.choice_probability("route", "quick").unwrap();
        assert!((quick - 0.25).abs() < 1e-12, "normalised: {quick}");
    }

    #[tokio::test]
    async fn chat_classifier_rejects_replies_outside_the_requested_ids() {
        let replies = [
            "",
            "quick",
            "```json\n{\"route\": {\"quick\": 1, \"goal\": 0}, \"goal\": 0.1}\n```",
            r#"["route"]"#,
            r#"{"route": {"quick": 1, "goal": 0}}"#,
            r#"{"route": {"quick": 1}, "goal": 0.1}"#,
            r#"{"route": {"quick": 0.5, "finance.incoming_invoice": 0.5}, "goal": 0.1}"#,
            r#"{"route": {"quick": 0.5, "goal": 0.5, "other": 0.1}, "goal": 0.1}"#,
            r#"{"route": "goal", "goal": 0.1}"#,
            r#"{"route": {"quick": 0.5, "goal": 0.5}, "goal": 1.4}"#,
            r#"{"route": {"quick": 0.5, "goal": 0.5}, "goal": "0.4"}"#,
            r#"{"route": {"quick": 0, "goal": 0}, "goal": 0.4}"#,
            r#"{"route": {"quick": 0.5, "goal": 0.5}, "goal": 0.4, "why": "x"}"#,
        ];
        for reply in replies {
            let classifier = ChatClassifierProvider::new(Arc::new(RecordingChat::new(
                ProviderClass::Cloud,
                reply,
            )));
            let err = classifier
                .classify(chat_request_fixture())
                .await
                .unwrap_err();
            assert!(
                matches!(err, ModelError::Provider(_)),
                "{reply:?} gave {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn chat_classifier_scores_are_normalised_and_weighted() {
        let classifier = ChatClassifierProvider::new(Arc::new(RecordingChat::new(
            ProviderClass::Cloud,
            r#"{"f": {"0": 0.2, "1": 0.6}}"#,
        )));
        let response = classifier
            .classify(request(vec![ClassifierQuestion::score(
                "f",
                "?",
                ["Calm", "Angry"],
            )]))
            .await
            .unwrap();
        let Some(AnswerValue::Score {
            value,
            probabilities,
            confidence: None,
        }) = response.answer("f")
        else {
            panic!("expected a score answer");
        };
        assert!((value - 0.75).abs() < 1e-12, "{value}");
        assert!((probabilities[0] - 0.25).abs() < 1e-12 && (probabilities[1] - 0.75).abs() < 1e-12);
    }

    // -- Static provider and queue wrapper ---------------------------------------

    #[tokio::test]
    async fn static_classifier_answers_by_question_id() {
        let provider = StaticClassifierProvider::new([
            ClassifierAnswer::noul("goal", 0.2),
            ClassifierAnswer::choice(
                "route",
                [("quick", 0.6), ("short_task", 0.3), ("goal", 0.1)],
            ),
        ]);
        let response = provider
            .classify(request(vec![route_question(), goal_question()]))
            .await
            .unwrap();
        assert_eq!(response.chosen("route"), Some("quick"));
        assert_eq!(response.noul("goal"), Some(0.2));

        let missing = provider
            .classify(request(vec![ClassifierQuestion::noul(
                "other", "?", None, None,
            )]))
            .await
            .unwrap_err();
        assert!(matches!(missing, ModelError::Provider(_)));
        let wrong_kind = StaticClassifierProvider::new([ClassifierAnswer::noul("route", 0.5)])
            .classify(request(vec![route_question()]))
            .await
            .unwrap_err();
        assert!(matches!(wrong_kind, ModelError::Provider(_)));
    }

    #[cfg(feature = "queue")]
    /// Counts calls; fails with `Unavailable` for the first `failures` calls.
    #[derive(Clone)]
    struct CountingClassifier {
        inner: StaticClassifierProvider,
        calls: Arc<AtomicUsize>,
        failures: usize,
    }

    #[cfg(feature = "queue")]
    #[async_trait]
    impl ModelProvider for CountingClassifier {
        fn failure_charge(&self, _: &ModelError) -> crate::FailureCharge {
            crate::FailureCharge::KnownZero
        }
        fn descriptor(&self) -> &ProviderDescriptor {
            self.inner.descriptor()
        }
    }

    #[cfg(feature = "queue")]
    #[async_trait]
    impl ClassifierProvider for CountingClassifier {
        async fn classify(&self, request: ClassifyRequest) -> Result<ClassifyResponse, ModelError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
                return Err(ModelError::Unavailable(
                    symbiotic_core::DiagnosticCode::HttpUnavailable,
                ));
            }
            self.inner.classify(request).await
        }
    }

    #[cfg(feature = "queue")]
    fn queue_config(cache: Option<PathBuf>) -> ModelQueueConfig {
        ModelQueueConfig {
            max_in_flight: 1,
            lease_seconds: 60,
            logical_retry_attempts: 2,
            retry_attempts: 2,
            retry_jitter_seconds: 0,
            request_timeout_seconds: Some(10),
            requests_per_minute: None,
            input_units_per_minute: None,
            response_cache_dir: cache,
            ..ModelQueueConfig::default()
        }
    }

    #[cfg(feature = "queue")]
    #[tokio::test(start_paused = true)]
    async fn queued_classifier_reuses_exact_response_cache_and_traces() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let trace_sink = Arc::new(InMemoryTraceSink::default());
        let provider = QueuedClassifierProvider::new(
            CountingClassifier {
                inner: StaticClassifierProvider::new([ClassifierAnswer::noul("goal", 0.7)]),
                calls: calls.clone(),
                failures: 0,
            },
            Arc::new(
                SqliteQueue::in_memory()
                    .unwrap()
                    .with_clock(crate::test_clock::clock()),
            ),
            "worker",
            queue_config(Some(dir.path().join("cache"))),
        )
        .with_spend_ledger(crate::test_spend::ledger(), None)
        .with_trace_sink(trace_sink.clone());

        let first = provider
            .classify(request(vec![goal_question()]))
            .await
            .unwrap();
        let second = provider
            .classify(request(vec![goal_question()]))
            .await
            .unwrap();
        assert_eq!(first.noul("goal"), Some(0.7));
        assert_eq!(second.noul("goal"), Some(0.7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let records = trace_sink.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].cache.response_cache, CacheStatus::Miss);
        assert_eq!(records[1].cache.response_cache, CacheStatus::Hit);
        assert!(records[0].queue_item_id.is_some());
        assert_eq!(
            records[0].model.queue_id().0,
            "classify:static:static-classifier-v1"
        );
    }

    #[cfg(feature = "queue")]
    #[tokio::test(start_paused = true)]
    async fn queued_classifier_retries_retryable_failures() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn ClassifierProvider> = Arc::new(CountingClassifier {
            inner: StaticClassifierProvider::new([ClassifierAnswer::noul("goal", 0.7)]),
            calls: calls.clone(),
            failures: 1,
        });
        // An `Arc<dyn ClassifierProvider>` is itself a provider and can be queued.
        let queued = QueuedClassifierProvider::new(
            provider,
            Arc::new(
                SqliteQueue::in_memory()
                    .unwrap()
                    .with_clock(crate::test_clock::clock()),
            ),
            "worker",
            queue_config(None),
        )
        .with_spend_ledger(crate::test_spend::ledger(), None);
        let response = queued
            .classify(request(vec![goal_question()]))
            .await
            .unwrap();
        assert_eq!(response.noul("goal"), Some(0.7));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[cfg(feature = "queue")]
    #[tokio::test(start_paused = true)]
    async fn queued_classifier_traces_a_terminal_failure_with_its_typed_class() {
        let trace_sink = Arc::new(InMemoryTraceSink::default());
        let provider = QueuedClassifierProvider::new(
            CountingClassifier {
                inner: StaticClassifierProvider::new([ClassifierAnswer::noul("goal", 0.7)]),
                calls: Arc::new(AtomicUsize::new(0)),
                failures: 1,
            },
            Arc::new(
                SqliteQueue::in_memory()
                    .unwrap()
                    .with_clock(crate::test_clock::clock()),
            ),
            "worker",
            ModelQueueConfig {
                logical_retry_attempts: 1,
                retry_attempts: 1,
                ..queue_config(None)
            },
        )
        .with_spend_ledger(crate::test_spend::ledger(), None)
        .with_trace_sink(trace_sink.clone());
        assert!(matches!(
            provider.classify(request(vec![goal_question()])).await,
            Err(ModelError::Unavailable(_))
        ));
        let traces = trace_sink.records();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].outcome, InvocationOutcome::Failed);
        assert_eq!(traces[0].error_class, Some(FailureClass::Unavailable));
    }

    #[cfg(feature = "queue")]
    #[test]
    fn classify_requests_carry_an_input_budget() {
        let small = request(vec![goal_question()]).input_budget_units().unwrap();
        let mut large = request(vec![goal_question()]);
        large
            .state
            .insert("message".to_string(), serde_json::json!("y".repeat(4_000)));
        // About one unit per four characters of state and questions.
        assert!(small > 0 && large.input_budget_units().unwrap() >= small + 990);
    }
}
