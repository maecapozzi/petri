//! Native run-wide inactivity and active-runtime enforcement.
//!
//! The watchdog counts only structurally live execution. Queue time before the
//! first attempt is excluded. A global pause stops both clocks. Questions stop
//! both clocks only when every running firing is waiting for a person; an
//! independent sibling keeps the clocks running.
//!
//! Inactivity is refreshed by non-empty log output, artifacts, Petri's
//! structurally identified model events (including Pebble's typed tool
//! lifecycle), or an explicit host call to [`RunWatchdog::record_activity`].
//! Coordinator bookkeeping, retries, heartbeats, and arbitrary custom
//! payloads are not activity. Embedding hosts use that hook for other native
//! tool events after classifying their typed event, without interpreting
//! model prose. In particular, run-management polling, gather/wait
//! bookkeeping, and heartbeats must not call it.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use engine::{EngineState, Event, EventRecord};
use ir::StepEvent;
use serde::{Deserialize, Serialize};
use steps::{Answer, Question, QuestionExpired};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use crate::{
    CancelReason, CoordinatorEvent, CoordinatorHandle, CoordinatorRecord, CoordinatorState,
    ExecutionId, ExecutionObserver, RunTimeoutQuestion, RunTimeoutState, RunTimeoutWork,
};

/// An inactivity expiration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StallTimeout {
    pub stall_timeout_ms: u64,
    pub idle_ms:          u64,
}

/// An active-runtime expiration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaximumActiveDuration {
    pub maximum_active_duration_ms: u64,
    pub active_duration_ms:         u64,
}

/// The timeout that stopped a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunTimeout {
    Stall(StallTimeout),
    MaximumActiveDuration(MaximumActiveDuration),
}

struct State {
    durable:          RunTimeoutState,
    paused:           bool,
    redispatch_ready: bool,
    last_tick:        Instant,
    active_remainder: Duration,
    idle_remainder:   Duration,
    tripped:          Option<RunTimeout>,
}

impl State {
    fn questions_cover_running(&self) -> bool {
        !self.durable.running.is_empty()
            && self.durable.running.iter().all(|work| {
                self.durable.questions.iter().any(|question| {
                    question.execution == work.execution && question.firing == work.firing
                })
            })
    }

    fn idle_clock_runs(&self) -> bool {
        self.durable.started
            && self.redispatch_ready
            && !self.paused
            && !self.questions_cover_running()
    }

    fn active_clock_runs(&self) -> bool {
        self.idle_clock_runs() && !self.durable.running.is_empty()
    }

    fn settle(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_tick);
        if self.active_clock_runs() {
            let elapsed = elapsed.saturating_add(self.active_remainder);
            self.durable.active_duration_ms = self
                .durable
                .active_duration_ms
                .saturating_add(millis(elapsed));
            self.active_remainder =
                Duration::from_nanos(u64::from(elapsed.subsec_nanos() % 1_000_000));
        }
        if self.idle_clock_runs() {
            let elapsed = elapsed.saturating_add(self.idle_remainder);
            self.durable.idle_duration_ms = self
                .durable
                .idle_duration_ms
                .saturating_add(millis(elapsed));
            self.idle_remainder =
                Duration::from_nanos(u64::from(elapsed.subsec_nanos() % 1_000_000));
        }
        self.last_tick = now;
    }
}

struct Inner {
    stall_timeout:       Option<Duration>,
    max_active_duration: Option<Duration>,
    state:               Mutex<State>,
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The native run watchdog.
///
/// Attach a clone as an [`ExecutionObserver`], then call [`start`](Self::start)
/// once the coordinator handle exists. On resume, the observer restores the
/// newest [`RunTimeoutState`] from [`CoordinatorState`] before work starts.
#[derive(Clone)]
pub struct RunWatchdog {
    inner: Arc<Inner>,
    stop:  CancellationToken,
}

/// Compatibility name for callers that only configure inactivity.
pub type StallWatchdog = RunWatchdog;

impl RunWatchdog {
    /// An inactivity-only watchdog, preserving the previous constructor.
    pub fn new(stall_timeout: Duration) -> Self {
        Self::with_limits(Some(stall_timeout), None)
    }

    pub fn with_limits(
        stall_timeout: Option<Duration>,
        max_active_duration: Option<Duration>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                stall_timeout,
                max_active_duration,
                state: Mutex::new(State {
                    durable:          RunTimeoutState::default(),
                    paused:           false,
                    redispatch_ready: true,
                    last_tick:        Instant::now(),
                    active_remainder: Duration::ZERO,
                    idle_remainder:   Duration::ZERO,
                    tripped:          None,
                }),
            }),
            stop:  CancellationToken::new(),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.inner.stall_timeout.unwrap_or_default()
    }

    pub fn max_active_duration(&self) -> Option<Duration> {
        self.inner.max_active_duration
    }

    pub fn state(&self) -> RunTimeoutState {
        let mut state = self.inner.state();
        state.settle(Instant::now());
        state.durable.clone()
    }

    /// Restore accounting supplied by an embedding host before `start`.
    ///
    /// This is useful when the host persists coordinator events in its own
    /// store. Missing accounting must be passed as `None`; that restores zero
    /// rather than deriving elapsed time from timestamps. Restored clocks stay
    /// parked until the observer receives `on_execution_resumed`, after the
    /// host has completed recovery setup and is ready to dispatch execution.
    pub fn restore(&self, restored: Option<&RunTimeoutState>, paused: bool) {
        let mut state = self.inner.state();
        state.durable = restored.cloned().unwrap_or_default();
        state.paused = paused;
        state.redispatch_ready = false;
        state.last_tick = Instant::now();
        state.active_remainder = Duration::ZERO;
        state.idle_remainder = Duration::ZERO;
    }

    /// Record structurally known model/tool activity from an embedding host.
    ///
    /// Call this for typed model execution/streaming or real tool work. Do
    /// not call it for observation/status polling, gather/wait bookkeeping,
    /// heartbeats, or empty retries.
    pub fn record_activity(&self) {
        let mut state = self.inner.state();
        state.settle(Instant::now());
        state.durable.idle_duration_ms = 0;
        state.idle_remainder = Duration::ZERO;
    }

    pub fn is_blocked(&self) -> bool {
        self.inner.state().questions_cover_running()
    }

    pub fn start(&self, handle: CoordinatorHandle) -> WatchdogTask {
        {
            let mut state = self.inner.state();
            state.last_tick = Instant::now();
        }
        let inner = self.inner.clone();
        let stop = self.stop.clone();
        let task = tokio::spawn(async move {
            monitor(inner, stop, handle).await;
        });
        WatchdogTask {
            stop: self.stop.clone(),
            task: Some(task),
        }
    }

    pub fn tripped(&self) -> Option<StallTimeout> {
        match self.inner.state().tripped.clone() {
            Some(RunTimeout::Stall(timeout)) => Some(timeout),
            Some(RunTimeout::MaximumActiveDuration(_)) | None => None,
        }
    }

    pub fn timeout_reason(&self) -> Option<RunTimeout> {
        self.inner.state().tripped.clone()
    }

    fn set_paused(&self, paused: bool) {
        let mut state = self.inner.state();
        state.settle(Instant::now());
        state.paused = paused;
    }
}

/// The running monitor. Stop it after the run; dropping it aborts the task.
pub struct WatchdogTask {
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl WatchdogTask {
    pub async fn stop(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for WatchdogTask {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn monitor(inner: Arc<Inner>, stop: CancellationToken, handle: CoordinatorHandle) {
    let resolution = checkpoint_resolution(inner.stall_timeout, inner.max_active_duration);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = sleep(resolution) => {}
        }
        let (durable, timeout) = {
            let mut state = inner.state();
            state.settle(Instant::now());
            let durable = state.durable.clone();
            let stall = inner.stall_timeout.and_then(|limit| {
                let configured = millis(limit);
                (state.durable.idle_duration_ms >= configured).then(|| {
                    RunTimeout::Stall(StallTimeout {
                        stall_timeout_ms: configured,
                        idle_ms:          state.durable.idle_duration_ms,
                    })
                })
            });
            let maximum = inner.max_active_duration.and_then(|limit| {
                let configured = millis(limit);
                (state.durable.active_duration_ms >= configured).then(|| {
                    RunTimeout::MaximumActiveDuration(MaximumActiveDuration {
                        maximum_active_duration_ms: configured,
                        active_duration_ms:         state.durable.active_duration_ms,
                    })
                })
            });
            let timeout = maximum.or(stall);
            if timeout.is_some() {
                state.tripped.clone_from(&timeout);
            }
            (durable, timeout)
        };
        match checkpoint_or_stop(&stop, handle.checkpoint_timeout(durable)).await {
            Checkpoint::Recorded => {}
            Checkpoint::Stopped => return,
            Checkpoint::Unavailable => {
                tracing::warn!(
                    timeout = timeout.is_some(),
                    "run timeout accounting could not be checkpointed"
                );
                if timeout.is_none() {
                    return;
                }
            }
        }
        match timeout {
            Some(RunTimeout::Stall(timeout)) => {
                handle.cancel_root_for(CancelReason::StallTimeout {
                    stall_timeout_ms: timeout.stall_timeout_ms,
                    idle_ms:          timeout.idle_ms,
                });
                return;
            }
            Some(RunTimeout::MaximumActiveDuration(timeout)) => {
                handle.cancel_root_for(CancelReason::MaximumActiveDuration {
                    maximum_active_duration_ms: timeout.maximum_active_duration_ms,
                    active_duration_ms:         timeout.active_duration_ms,
                });
                return;
            }
            None => {}
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Checkpoint {
    Recorded,
    Stopped,
    Unavailable,
}

async fn checkpoint_or_stop(
    stop: &CancellationToken,
    checkpoint: impl Future<Output = bool>,
) -> Checkpoint {
    tokio::select! {
        biased;
        () = stop.cancelled() => Checkpoint::Stopped,
        recorded = checkpoint => {
            if recorded {
                Checkpoint::Recorded
            } else {
                Checkpoint::Unavailable
            }
        }
    }
}

fn checkpoint_resolution(stall: Option<Duration>, maximum: Option<Duration>) -> Duration {
    stall
        .into_iter()
        .chain(maximum)
        .min()
        .map_or(Duration::from_secs(1), |budget| {
            (budget / 4).clamp(Duration::from_millis(1), Duration::from_secs(1))
        })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn meaningful_progress(event: &StepEvent) -> bool {
    match event {
        StepEvent::Log { line, .. } => !line.is_empty(),
        StepEvent::Artifact { .. } => true,
        StepEvent::Custom(value) => petri_model_activity(value),
    }
}

fn petri_model_activity(value: &serde_json::Value) -> bool {
    if value.get("kind").and_then(serde_json::Value::as_str) != Some("pebble") {
        return false;
    }
    let Some(event) = value
        .get("event")
        .and_then(|envelope| envelope.get("event"))
        .and_then(serde_json::Value::as_object)
    else {
        return false;
    };
    event.contains_key("LlmRequestStarted")
        || event.contains_key("LlmFirstOutput")
        || event.contains_key("ToolCallStarted")
        || event.contains_key("ToolCallCompleted")
        || event.contains_key("AssistantMessage")
        || non_empty_string(event, "TextDelta", "delta")
        || non_empty_string(event, "ReasoningDelta", "delta")
        || event
            .get("AssistantOutputReplace")
            .is_some_and(|replacement| {
                replacement
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|text| !text.is_empty())
                    || replacement
                        .get("reasoning")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|reasoning| !reasoning.is_empty())
            })
}

fn non_empty_string(
    event: &serde_json::Map<String, serde_json::Value>,
    variant: &str,
    field: &str,
) -> bool {
    event
        .get(variant)
        .and_then(|payload| payload.get(field))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

impl ExecutionObserver for RunWatchdog {
    fn on_engine_record(
        &self,
        execution: ExecutionId,
        record: &EventRecord,
        _recorded_at: u64,
        _state: &EngineState,
    ) {
        let now = Instant::now();
        let mut state = self.inner.state();
        state.settle(now);
        match &record.event {
            Event::StepStarted { firing, .. } => {
                state.durable.started = true;
                state.redispatch_ready = true;
                state.durable.running.insert(RunTimeoutWork {
                    execution,
                    firing: *firing,
                });
            }
            Event::StepProgressRecorded { firing, ev } => {
                if let Some(question) = Question::from_event(ev) {
                    state.durable.questions.insert(RunTimeoutQuestion {
                        execution,
                        firing: *firing,
                        question: question.id,
                    });
                } else if let Some(expired) = QuestionExpired::from_event(ev) {
                    state.durable.questions.retain(|question| {
                        question.execution != execution || question.question != expired.question
                    });
                } else if meaningful_progress(ev) {
                    state.durable.idle_duration_ms = 0;
                    state.idle_remainder = Duration::ZERO;
                }
            }
            Event::ControlRequested {
                ctl: ir::Control::Deliver(value),
                ..
            } => {
                if let Some(question) = Answer::from_value(value).and_then(|answer| answer.question)
                {
                    let before = state.durable.questions.len();
                    state.durable.questions.retain(|pending| {
                        pending.execution != execution || pending.question != question
                    });
                    if state.durable.questions.len() != before {
                        state.durable.idle_duration_ms = 0;
                        state.idle_remainder = Duration::ZERO;
                    }
                }
            }
            Event::StepFinished { firing, .. } => {
                state.durable.running.remove(&RunTimeoutWork {
                    execution,
                    firing: *firing,
                });
                state.durable.questions.retain(|question| {
                    question.execution != execution || question.firing != *firing
                });
            }
            _ => {}
        }
    }

    fn on_lifecycle(&self, record: &CoordinatorRecord) {
        match &record.body {
            CoordinatorEvent::RunPaused => self.set_paused(true),
            CoordinatorEvent::RunUnpaused => self.set_paused(false),
            CoordinatorEvent::RunTimeoutUpdated { .. }
            | CoordinatorEvent::RunStarted { .. }
            | CoordinatorEvent::GraphRegistered { .. }
            | CoordinatorEvent::InvocationDeclared { .. }
            | CoordinatorEvent::ExecutionDeclared { .. }
            | CoordinatorEvent::ExecutionFinished { .. }
            | CoordinatorEvent::InvocationFinished { .. }
            | CoordinatorEvent::InvocationCancelRequested { .. }
            | CoordinatorEvent::RunNoteRecorded { .. }
            | CoordinatorEvent::ScopeReleased { .. }
            | CoordinatorEvent::RunFinished { .. } => {}
        }
    }

    fn on_resumed(&self, state: &CoordinatorState) {
        self.restore(state.timeout.as_ref(), state.paused);
        let finished: BTreeSet<_> = state
            .executions
            .iter()
            .filter_map(|(execution, state)| state.exit.is_some().then_some(*execution))
            .collect();
        let mut own = self.inner.state();
        own.durable
            .running
            .retain(|work| !finished.contains(&work.execution));
        own.durable
            .questions
            .retain(|question| !finished.contains(&question.execution));
    }

    fn on_execution_resumed(&self, execution: ExecutionId, engine: &EngineState) {
        let now = Instant::now();
        let mut state = self.inner.state();
        state.settle(now);
        state
            .durable
            .running
            .retain(|work| work.execution != execution);
        state
            .durable
            .questions
            .retain(|question| question.execution != execution);

        state
            .durable
            .running
            .extend(
                engine
                    .live_firings()
                    .filter(|firing| firing.started)
                    .map(|firing| RunTimeoutWork {
                        execution,
                        firing: firing.id,
                    }),
            );
        state
            .durable
            .questions
            .extend(pending_questions(execution, engine));
        state.durable.started |= !state.durable.running.is_empty();
        state.redispatch_ready = true;
        state.last_tick = now;
    }
}

fn pending_questions(
    execution: ExecutionId,
    engine: &EngineState,
) -> impl Iterator<Item = RunTimeoutQuestion> {
    let running: BTreeSet<_> = engine
        .live_firings()
        .filter(|firing| firing.started)
        .map(|firing| firing.id)
        .collect();
    let mut questions = BTreeSet::new();
    for record in engine.log.records() {
        match &record.event {
            Event::StepProgressRecorded { firing, ev } => {
                if let Some(question) = Question::from_event(ev) {
                    questions.insert(RunTimeoutQuestion {
                        execution,
                        firing: *firing,
                        question: question.id,
                    });
                } else if let Some(expired) = QuestionExpired::from_event(ev) {
                    questions.retain(|question: &RunTimeoutQuestion| {
                        question.question != expired.question
                    });
                }
            }
            Event::ControlRequested {
                ctl: ir::Control::Deliver(value),
                ..
            } => {
                if let Some(question) = Answer::from_value(value).and_then(|answer| answer.question)
                {
                    questions.retain(|pending| pending.question != question);
                }
            }
            Event::StepFinished { firing, .. } => {
                questions.retain(|question| question.firing != *firing);
            }
            _ => {}
        }
    }
    questions.retain(|question| running.contains(&question.firing));
    questions.into_iter()
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use tokio::task::yield_now;
    use tokio::time::{advance, timeout};

    use super::*;

    #[test]
    fn structural_question_parking_requires_every_running_firing() {
        let watchdog =
            RunWatchdog::with_limits(Some(Duration::from_secs(10)), Some(Duration::from_secs(90)));
        let mut state = watchdog.inner.state();
        state.durable.started = true;
        state.durable.running.insert(RunTimeoutWork {
            execution: ExecutionId::new(0),
            firing:    ir::FiringId::new(1),
        });
        state.durable.running.insert(RunTimeoutWork {
            execution: ExecutionId::new(1),
            firing:    ir::FiringId::new(2),
        });
        state.durable.questions.insert(RunTimeoutQuestion {
            execution: ExecutionId::new(0),
            firing:    ir::FiringId::new(1),
            question:  "gate".into(),
        });
        assert!(!state.questions_cover_running());
        state.durable.questions.insert(RunTimeoutQuestion {
            execution: ExecutionId::new(1),
            firing:    ir::FiringId::new(2),
            question:  "other".into(),
        });
        assert!(state.questions_cover_running());
    }

    #[tokio::test(start_paused = true)]
    async fn progress_resets_only_inactivity() {
        let watchdog =
            RunWatchdog::with_limits(Some(Duration::from_secs(10)), Some(Duration::from_secs(90)));
        {
            let mut state = watchdog.inner.state();
            state.durable.started = true;
            state.durable.running.insert(RunTimeoutWork {
                execution: ExecutionId::new(0),
                firing:    ir::FiringId::new(1),
            });
        }
        advance(Duration::from_secs(5)).await;
        watchdog.record_activity();
        let state = watchdog.state();
        assert!(state.active_duration_ms >= 5_000);
        assert_eq!(state.idle_duration_ms, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn restored_work_does_not_count_recovery_setup() {
        let watchdog =
            RunWatchdog::with_limits(Some(Duration::from_secs(10)), Some(Duration::from_secs(90)));
        let restored = RunTimeoutState {
            started: true,
            running: BTreeSet::from([
                RunTimeoutWork {
                    execution: ExecutionId::new(0),
                    firing:    ir::FiringId::new(1),
                },
                RunTimeoutWork {
                    execution: ExecutionId::new(1),
                    firing:    ir::FiringId::new(2),
                },
            ]),
            questions: BTreeSet::from([RunTimeoutQuestion {
                execution: ExecutionId::new(0),
                firing:    ir::FiringId::new(1),
                question:  "gate".into(),
            }]),
            ..RunTimeoutState::default()
        };
        watchdog.restore(Some(&restored), false);

        advance(Duration::from_secs(30)).await;

        let state = watchdog.state();
        assert_eq!(state.active_duration_ms, 0);
        assert_eq!(state.idle_duration_ms, 0);
        assert_eq!(state.running, restored.running);
        assert_eq!(state.questions, restored.questions);

        {
            let mut state = watchdog.inner.state();
            state.redispatch_ready = true;
        }
        advance(Duration::from_secs(5)).await;
        let state = watchdog.state();
        assert_eq!(state.active_duration_ms, 5_000);
        assert_eq!(state.idle_duration_ms, 5_000);
    }

    #[test]
    fn sub_millisecond_activity_cannot_discard_active_runtime() {
        let watchdog = RunWatchdog::with_limits(None, Some(Duration::from_millis(10)));
        let mut state = watchdog.inner.state();
        state.durable.started = true;
        state.durable.running.insert(RunTimeoutWork {
            execution: ExecutionId::new(0),
            firing:    ir::FiringId::new(1),
        });
        let mut now = state.last_tick;
        for _ in 0..20 {
            now += Duration::from_micros(500);
            state.settle(now);
            state.durable.idle_duration_ms = 0;
            state.idle_remainder = Duration::ZERO;
        }
        assert_eq!(state.durable.active_duration_ms, 10);
    }

    #[test]
    fn pause_and_all_waiting_questions_exclude_both_clocks() {
        let watchdog =
            RunWatchdog::with_limits(Some(Duration::from_secs(10)), Some(Duration::from_secs(90)));
        let mut state = watchdog.inner.state();
        state.durable.started = true;
        state.durable.running.insert(RunTimeoutWork {
            execution: ExecutionId::new(0),
            firing:    ir::FiringId::new(1),
        });
        state.durable.questions.insert(RunTimeoutQuestion {
            execution: ExecutionId::new(0),
            firing:    ir::FiringId::new(1),
            question:  "gate".into(),
        });
        let mut now = state.last_tick + Duration::from_secs(30);
        state.settle(now);
        assert_eq!(state.durable.active_duration_ms, 0);
        assert_eq!(state.durable.idle_duration_ms, 0);

        state.durable.running.insert(RunTimeoutWork {
            execution: ExecutionId::new(1),
            firing:    ir::FiringId::new(2),
        });
        now += Duration::from_secs(5);
        state.settle(now);
        assert_eq!(state.durable.active_duration_ms, 5_000);
        assert_eq!(state.durable.idle_duration_ms, 5_000);

        state.paused = true;
        now += Duration::from_secs(30);
        state.settle(now);
        assert_eq!(state.durable.active_duration_ms, 5_000);
        assert_eq!(state.durable.idle_duration_ms, 5_000);
    }

    #[test]
    fn polling_heartbeat_and_empty_output_are_not_meaningful_activity() {
        assert!(!meaningful_progress(&StepEvent::Custom(
            serde_json::json!({"kind": "heartbeat"})
        )));
        assert!(!meaningful_progress(&StepEvent::Custom(
            serde_json::json!({
                "kind": "pebble",
                "event": {"event": {"SessionStarted": {}}}
            })
        )));
        assert!(meaningful_progress(&StepEvent::Custom(serde_json::json!({
            "kind": "pebble",
            "event": {"event": {"AssistantMessage": {"text": "working"}}}
        }))));
        assert!(meaningful_progress(&StepEvent::Custom(serde_json::json!({
            "kind": "pebble",
            "event": {"event": {"TextDelta": {"delta": "work"}}}
        }))));
        for variant in ["ToolCallStarted", "ToolCallCompleted"] {
            assert!(meaningful_progress(&StepEvent::Custom(serde_json::json!({
                "kind": "pebble",
                "event": {"event": {(variant): {"tool_name": "shell"}}}
            }))));
        }
        assert!(!meaningful_progress(&StepEvent::Custom(
            serde_json::json!({
                "kind": "pebble",
                "event": {"event": {"TextDelta": {"delta": ""}}}
            })
        )));
        assert!(!meaningful_progress(&StepEvent::Log {
            stream: ir::LogStream::Stdout,
            line:   String::new(),
        }));
        assert!(meaningful_progress(&StepEvent::Log {
            stream: ir::LogStream::Stdout,
            line:   "tool output".into(),
        }));
        assert!(meaningful_progress(&StepEvent::Artifact {
            name: "result".into(),
            uri:  "memory://result".into(),
        }));
    }

    #[test]
    fn old_runs_restore_without_inventing_elapsed_time() {
        let watchdog =
            RunWatchdog::with_limits(Some(Duration::from_secs(10)), Some(Duration::from_secs(90)));
        watchdog.restore(None, false);
        assert_eq!(watchdog.state(), RunTimeoutState::default());
    }

    #[tokio::test]
    async fn stop_does_not_wait_for_a_checkpoint_acknowledgement() {
        let stop = CancellationToken::new();
        let task = tokio::spawn({
            let stop = stop.clone();
            async move {
                assert_eq!(
                    checkpoint_or_stop(&stop, pending()).await,
                    Checkpoint::Stopped
                );
            }
        });
        yield_now().await;
        let watchdog = WatchdogTask {
            stop,
            task: Some(task),
        };
        timeout(Duration::from_millis(100), watchdog.stop())
            .await
            .expect("stop is bounded while checkpoint acknowledgement is withheld");
    }
}
