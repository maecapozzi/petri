//! Readiness item 6 on the standalone host: the failure circuit breaker
//! (`loop_restart_signature_limit`), node visit totals across `loop_restart`,
//! the stall watchdog (`stall_timeout`), and the control service (pause at
//! admission, steering into a live stage). Agents are Fabro's simulated
//! stubs; commands and human gates are real.

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep as sleep_thread;
use std::time::{Duration, Instant};

use petri::attractor::{
    AGENT_KIND, BranchStep, CommandStep, FanInStep, ForkStep, HumanStep, StageStep, StubStep,
    WAIT_KIND, WORKFLOW_KIND,
};
use petri::engine::{EngineState, Event, EventRecord, RunError};
use petri::execution::controls::{ControlError, ControlService};
use petri::execution::events::{
    CollectingSink, EventProjector, EventSource, ViewEvent, replay_run_dir,
};
use petri::execution::host::{self, HostRun};
use petri::execution::inspect::inspect_run_dir;
use petri::execution::watchdog::{RunTimeout, RunWatchdog, StallWatchdog, WatchdogTask};
use petri::execution::{
    CallSite, CancelReason, CoordinatorEvent, CoordinatorInvocationClient, CoordinatorRecord,
    ExecutionId, ExecutionObserver, GraphDigest, InterviewDispatcher, InterviewReply,
    InterviewRequest, Interviewer, InvocationClient as _, InvocationRequest, SandboxMode,
    SecretBindings,
};
use petri::executor::Retention;
use petri::frontend::fabro::Fabro;
use petri::frontend::{CompileInputs, Lowered};
use petri::ir::{
    Control, FiringId, Graph, GraphBuilder, Outcome, RunStatus, Scope, ScopeId, Status, StepEvent,
    StepRef, Value,
};
use petri::steps::{Answer, Question, Step, StepCtx};
use petri::{RunOptions, Runtime, driver};
use serde::Deserialize;
use serde_json::json;
use testkit::RunDir;
use tokio::sync::{Notify, oneshot};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

fn runtime(dir: &RunDir) -> Runtime {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_secs(2);
    options.retention = Retention::Never;
    options.echo = false;
    let mut registry = Runtime::standard().registry().clone();
    for kind in [&AGENT_KIND, &WAIT_KIND, &WORKFLOW_KIND] {
        registry.register_runner(Arc::new(StubStep::new((*kind).clone())));
    }
    registry.register(CommandStep);
    registry.register(ForkStep);
    registry.register(BranchStep);
    registry.register(FanInStep);
    registry.register(HumanStep);
    registry.register(StageStep);
    Runtime::standard()
        .frontend(Fabro::new())
        .steps(registry)
        .options(options)
}

fn lower(rt: &Runtime, dir: &RunDir, text: &str) -> Lowered {
    let path = dir.path().join("wf.fabro");
    fs::write(&path, text).expect("write the workflow");
    let lowered = rt
        .check(&path, None, None, &CompileInputs::new())
        .expect("loads");
    assert!(
        lowered.graph.is_some(),
        "the workflow lowers: {:?}",
        lowered.diagnostics
    );
    lowered
}

/// Script a stub node's calls.
fn script(graph: &mut Graph, node: &str, calls: &Value) {
    let node = graph
        .body
        .nodes
        .iter_mut()
        .find(|n| n.name == node)
        .expect("the node");
    let Value::Object(config) = &mut node.step.config else {
        panic!("an object config");
    };
    config.insert("simulate".into(), json!({ "calls": calls.clone() }));
}

fn firings_of(report: &driver::ExecutionReport, node: &str) -> usize {
    report
        .state
        .history()
        .iter()
        .filter(|r| r.name == node)
        .count()
}

#[derive(Default)]
struct ResumeTimeoutFixture {
    root_question:  Notify,
    child_question: Notify,
    active_child:   Notify,
}

#[derive(Deserialize)]
struct ResumeChildren {
    question: GraphDigest,
    active:   GraphDigest,
}

struct ResumeParentStep;

#[async_trait::async_trait]
impl Step for ResumeParentStep {
    const NAME: &'static str = "test/resume-parent";
    type Config = ResumeChildren;

    async fn run(&self, config: ResumeChildren, ctx: StepCtx) -> Outcome {
        let client = match ctx.require_capability::<CoordinatorInvocationClient>() {
            Ok(client) => client,
            Err(error) => return error.into(),
        };
        let start = |slot: &str, graph| {
            client.start_or_attach(InvocationRequest::new(
                CallSite {
                    firing:  ctx.firing,
                    attempt: ctx.attempt,
                    slot:    slot.into(),
                },
                graph,
                BTreeMap::new(),
                SecretBindings::None,
                SandboxMode::Isolated,
            ))
        };
        let mut question = match start("question", config.question).await {
            Ok(handle) => handle,
            Err(error) => return Outcome::failure(error.to_string()),
        };
        let mut active = match start("active", config.active).await {
            Ok(handle) => handle,
            Err(error) => return Outcome::failure(error.to_string()),
        };
        if ctx
            .logs
            .send_acked(Question::new("root-wait", "Waiting").to_event())
            .await
            .is_err()
        {
            return Outcome::failure("could not record the parent question");
        }
        let fixture = match ctx.require_capability::<Arc<ResumeTimeoutFixture>>() {
            Ok(fixture) => fixture,
            Err(error) => return error.into(),
        };
        fixture.root_question.notify_one();
        let (question, active) = tokio::join!(question.result(), active.result());
        if question.status == RunStatus::Cancelled || active.status == RunStatus::Cancelled {
            Outcome::cancelled()
        } else {
            Outcome::success(serde_json::Value::Null)
        }
    }
}

struct ResumeQuestionStep;

#[async_trait::async_trait]
impl Step for ResumeQuestionStep {
    const NAME: &'static str = "test/resume-question";
    type Config = ();

    async fn run(&self, (): (), mut ctx: StepCtx) -> Outcome {
        if ctx
            .logs
            .send_acked(Question::new("child-wait", "Waiting").to_event())
            .await
            .is_err()
        {
            return Outcome::failure("could not record the child question");
        }
        let fixture = match ctx.require_capability::<Arc<ResumeTimeoutFixture>>() {
            Ok(fixture) => fixture,
            Err(error) => return error.into(),
        };
        fixture.child_question.notify_one();
        let _ = ctx.control.recv().await;
        Outcome::cancelled()
    }
}

struct ResumeActiveStep;

#[async_trait::async_trait]
impl Step for ResumeActiveStep {
    const NAME: &'static str = "test/resume-active";
    type Config = ();

    async fn run(&self, (): (), mut ctx: StepCtx) -> Outcome {
        let fixture = match ctx.require_capability::<Arc<ResumeTimeoutFixture>>() {
            Ok(fixture) => fixture,
            Err(error) => return error.into(),
        };
        fixture.active_child.notify_one();
        let _ = ctx.control.recv().await;
        Outcome::cancelled()
    }
}

fn resume_timeout_runtime(dir: &RunDir, fixture: Arc<ResumeTimeoutFixture>) -> Runtime {
    runtime(dir)
        .step(ResumeParentStep)
        .step(ResumeQuestionStep)
        .step(ResumeActiveStep)
        .capability(fixture)
}

fn digest(graph: &Graph) -> GraphDigest {
    GraphDigest::of(
        serde_json::to_string(graph)
            .expect("a graph encodes")
            .as_bytes(),
    )
}

/// The reason the breaker blocked a route, if it did.
fn blocked_reason(state: &EngineState) -> Option<String> {
    state.errors().iter().find_map(|error| match error {
        RunError::RoutingBlocked { reason, .. } => Some(reason.to_string()),
        _ => None,
    })
}

// ── Circuit breaker ─────────────────────────────────────────────────────────

const RESTART_LOOP: &str = r#"digraph G {
    graph [loop_restart_signature_limit=3]
    start [shape=Mdiamond]
    exit [shape=Msquare]
    work [prompt="x", max_visits=10]
    check [shape=diamond]
    start -> work -> check
    check -> exit [condition="context.done=true"]
    check -> work [loop_restart=true]
}"#;

/// Three repeats of one deterministic failure signature fail the run, and
/// the count survives the restart chain: each repeat is in its own
/// execution.
#[tokio::test]
async fn a_repeated_deterministic_failure_trips_the_breaker_across_restarts() {
    let dir = RunDir::new("breaker-trip");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART_LOOP);
    let mut graph = lowered.graph.expect("lowers");
    assert_eq!(
        graph
            .policy
            .loop_restart_signature_limit
            .map(NonZeroU32::get),
        Some(3)
    );
    script(
        &mut graph,
        "work",
        &json!([{
            "outcome": "failed",
            "failure_reason": "the tests failed on line 12",
            "context_updates": { "done": "false" }
        }]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    assert_eq!(
        firings_of(&report, "work"),
        1,
        "the last execution ran work once"
    );
    let reason = blocked_reason(&report.state).expect("the route was blocked");
    assert!(
        reason.contains("deterministic failure cycle detected"),
        "{reason}"
    );
    assert!(reason.contains("repeated 3 times (limit 3)"), "{reason}");
    // Three executions ran (two restarts), one `work` each.
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 3, "{document:?}");
}

/// Success does not clear a signature's count.
#[tokio::test]
async fn a_success_between_failures_does_not_clear_the_count() {
    let dir = RunDir::new("breaker-no-clear");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART_LOOP);
    let mut graph = lowered.graph.expect("lowers");
    let fail = json!({
        "outcome": "failed",
        "failure_reason": "the tests failed",
        "context_updates": { "done": "false" }
    });
    script(
        &mut graph,
        "work",
        &json!([fail, fail, { "context_updates": { "done": "false" } }, fail]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 4, "fail, fail, success, fail");
}

/// A `loop_restart` edge taken by the failing node itself: a deterministic
/// failure may not restart; a transient one may.
#[tokio::test]
async fn a_restart_edge_admits_only_transient_failures() {
    const SELF_RESTART: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [prompt="x", max_visits=10]
        start -> work
        work -> work [loop_restart=true, condition="outcome=failed"]
        work -> exit
    }"#;
    let dir = RunDir::new("breaker-restart-deterministic");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, SELF_RESTART);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{ "outcome": "failed", "failure_reason": "syntax error" }, {}]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    let reason = blocked_reason(&report.state).expect("blocked");
    assert!(
        reason.contains("loop_restart blocked: failure_class=deterministic"),
        "{reason}"
    );

    let dir = RunDir::new("breaker-restart-transient");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, SELF_RESTART);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{ "outcome": "failed", "failure_reason": "connection refused by the registry" }, {}]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 2, "one restart, then success");
}

/// Command output whose line numbers and names read as transient hints.
const DIAGNOSTIC: &str = "FAILED tests/test_retry.py::test_timeout at src/domain/order.py:502:5";

/// Real command diagnostics stay deterministic on a normal loop edge, where
/// repeats must trip the breaker.
#[tokio::test]
async fn command_diagnostics_cannot_bypass_the_breaker() {
    let report = run_failing_command("breaker-command-loop", false).await;
    assert_eq!(
        firings_of(&report, "work"),
        3,
        "{:?}",
        report.state.errors()
    );
    let reason = blocked_reason(&report.state).expect("the breaker blocks the route");
    assert!(
        reason.contains("deterministic failure cycle detected"),
        "{reason}"
    );
    assert!(reason.contains("repeated 3 times (limit 3)"), "{reason}");
}

/// Real command diagnostics stay deterministic on a transient-only restart.
#[tokio::test]
async fn command_diagnostics_cannot_take_a_restart_edge() {
    let report = run_failing_command("breaker-command-restart", true).await;
    assert_eq!(
        firings_of(&report, "work"),
        1,
        "{:?}",
        report.state.errors()
    );
    let reason = blocked_reason(&report.state).expect("the breaker blocks the route");
    assert!(
        reason.contains("loop_restart blocked: failure_class=deterministic"),
        "{reason}"
    );
}

/// Runs a command that prints [`DIAGNOSTIC`] and exits 1 on a self-loop, and
/// checks the diagnostic survives in the failure reason and
/// `command.output`.
async fn run_failing_command(name: &str, restart: bool) -> driver::ExecutionReport {
    let dir = RunDir::new(name);
    let rt = runtime(&dir);
    let workflow = format!(
        r#"digraph G {{
                graph [loop_restart_signature_limit=3]
                start [shape=Mdiamond]
                exit [shape=Msquare]
                work [shape=parallelogram, max_visits=4,
                      script="echo '{DIAGNOSTIC}' >&2; exit 1"]
                start -> work
                work -> work [condition="outcome=failed", loop_restart={restart}]
                work -> exit
            }}"#
    );
    let graph = lower(&rt, &dir, &workflow).graph.expect("lowers");
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    let info = report
        .state
        .history()
        .iter()
        .find(|r| r.name == "work")
        .and_then(|r| r.outcome.status.failure_info())
        .expect("command failure");
    assert_eq!(info.class.as_str(), "exit_status:1");
    assert!(info.message.contains(DIAGNOSTIC), "{}", info.message);
    assert_eq!(
        report.state.run_context().get("command.output"),
        Some(&json!(format!("{DIAGNOSTIC}\n")))
    );
    report
}

/// Node visit totals survive a `loop_restart` while the context is
/// replaced: `max_visits=2` is reached on the third execution's visit, not
/// reset by the restart.
#[tokio::test]
async fn node_visit_totals_survive_a_restart_while_context_resets() {
    const RESTART: &str = r#"digraph G {
        graph [loop_restart_signature_limit=10]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [prompt="x", max_visits=2]
        check [shape=diamond]
        start -> work -> check
        check -> exit [condition="context.done=true"]
        check -> work [loop_restart=true]
    }"#;
    let dir = RunDir::new("breaker-visits");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{ "context_updates": { "done": "false", "seen": "yes" } }]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    assert!(
        report
            .state
            .errors()
            .iter()
            .any(|error| matches!(error, RunError::BudgetExceeded { max_firings: 2, .. })),
        "{:?}",
        report.state.errors()
    );
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 3);
    // The final execution started with an empty context: the restart
    // replaced it, and `seen` was written again by nothing.
    assert!(
        report.state.run_context().get("seen").is_none(),
        "{:?}",
        report.state.run_context().kv
    );
}

/// Both signature maps are restored on resume: a run crashed after the third
/// failure was recorded but before its route was resolved still blocks the
/// route when resumed.
#[tokio::test]
async fn the_breaker_state_is_restored_on_resume() {
    let dir = RunDir::new("breaker-resume");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART_LOOP);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{
            "outcome": "failed",
            "failure_reason": "the tests failed",
            "context_updates": { "done": "false" }
        }]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);

    // Crash the last execution right after `work`'s final StepFinished:
    // the block decision is not in the log any more.
    let executions = dir.path().join("executions");
    let mut dirs: Vec<_> = fs::read_dir(&executions)
        .expect("executions")
        .map(|e| e.expect("entry").path())
        .collect();
    dirs.sort();
    let events = dirs.last().expect("an execution").join("events.jsonl");
    let text = fs::read_to_string(&events).expect("events");
    let lines: Vec<&str> = text.lines().collect();
    let cut = lines
        .iter()
        .position(|line| line.contains("\"step.finished\"") && line.contains("failure"))
        .expect("the failure record")
        + 1;
    fs::write(&events, format!("{}\n", lines[..cut].join("\n"))).expect("truncate");
    // The coordinator log must also forget the exit and the run's finish.
    let coordinator = dir.path().join("coordinator.jsonl");
    let text = fs::read_to_string(&coordinator).expect("coordinator log");
    let kept: Vec<&str> = text
        .lines()
        .filter(|line| {
            !(line.contains("\"run.finished\"")
                || line.contains("\"invocation.finished\"")
                || line.contains("\"scope.released\"")
                || (line.contains("\"execution.finished\"") && line.contains("terminal")))
        })
        .collect();
    fs::write(&coordinator, format!("{}\n", kept.join("\n"))).expect("rewrite");

    let resumed = host::resume(&rt).await.expect("resumes");
    assert_eq!(resumed.status, RunStatus::Failed);
    let reason = blocked_reason(&resumed.state).expect("the restored count blocks the route");
    assert!(reason.contains("repeated 3 times"), "{reason}");
}

// ── Stall watchdog ──────────────────────────────────────────────────────────

/// A run with no execution activity for the stall budget is cancelled.
#[tokio::test]
async fn an_idle_run_is_cancelled_by_the_watchdog() {
    const IDLE: &str = r#"digraph G {
        graph [stall_timeout="400ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        long [shape=parallelogram, script="sleep 20"]
        start -> long -> exit
    }"#;
    let dir = RunDir::new("watchdog-idle");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, IDLE);
    let graph = lowered.graph.expect("lowers");
    assert_eq!(graph.policy.stall_timeout, Some(Duration::from_millis(400)));
    let watchdog = StallWatchdog::new(graph.policy.stall_timeout.expect("a budget"));
    let sink = Arc::new(CollectingSink::default());
    let projector = EventProjector::new(sink.clone());
    let host_run = HostRun::new(graph)
        .observe(Arc::new(watchdog.clone()))
        .observe(projector.clone() as Arc<dyn ExecutionObserver>);
    let mut task = None;
    let started = Instant::now();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        task = Some(watchdog.start(handle));
    })
    .await
    .expect("the run completes");
    task.expect("started").stop().await;
    let receipt = projector.shutdown().await;
    assert!(receipt.is_clean(), "{receipt:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(
        report.status,
        RunStatus::Cancelled,
        "exit={:?}, errors={:?}, history={:?}, releases={:?}",
        report.exit,
        report.state.errors(),
        report.state.history(),
        report.releases
    );
    let stall = watchdog.tripped().expect("the watchdog fired");
    assert_eq!(stall.stall_timeout_ms, 400);
    assert!(stall.idle_ms >= 400);
    // The stall is a public event, derived from the durable cancel record
    // that carries it as the reason.
    let events = sink.events();
    let stalls: Vec<_> = events
        .iter()
        .filter(|e| matches!(e.view(), Some(ViewEvent::RunStalled { .. })))
        .collect();
    assert_eq!(stalls.len(), 1, "{events:#?}");
    assert!(matches!(
        stalls[0].view(),
        Some(ViewEvent::RunStalled {
            stall_timeout_ms: 400,
            idle_ms
        }) if *idle_ms >= 400
    ));
    assert!(
        events.iter().any(|e| matches!(
            e.coordinator(),
            Some(CoordinatorEvent::InvocationCancelRequested {
                reason: Some(CancelReason::StallTimeout {
                    stall_timeout_ms: 400,
                    ..
                }),
                ..
            })
        )),
        "the cancel names its reason: {events:#?}"
    );
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    assert!(
        replayed
            .iter()
            .any(|e| matches!(e.view(), Some(ViewEvent::RunStalled { .. }))),
        "the stall survives replay"
    );
}

struct ActivityAfterTimeoutDecision {
    watchdog: RunWatchdog,
    observed: AtomicBool,
}

impl ExecutionObserver for ActivityAfterTimeoutDecision {
    fn on_engine_record(&self, _: ExecutionId, _: &EventRecord, _: u64, _: &EngineState) {}

    fn on_lifecycle(&self, record: &CoordinatorRecord) {
        if let CoordinatorEvent::RunTimeoutUpdated { state } = &record.body
            && state.idle_duration_ms >= 100
            && !self.observed.swap(true, Ordering::SeqCst)
        {
            // The watchdog made its timeout decision before asking the
            // coordinator to append this checkpoint. Activity here is real,
            // but it is after that decision's linearization point.
            self.watchdog.record_activity();
        }
    }
}

/// Activity can race between a timeout decision and its durable checkpoint.
/// The checkpoint observer provides that controlled ordering: the decision
/// remains valid and cancellation follows, while the activity resets the next
/// idle interval rather than retroactively revoking the completed decision.
#[tokio::test]
async fn activity_after_the_timeout_decision_does_not_revoke_it() {
    const IDLE: &str = r#"digraph G {
        graph [stall_timeout="100ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        long [shape=parallelogram, script="sleep 20"]
        start -> long -> exit
    }"#;
    let dir = RunDir::new("watchdog-decision-activity-race");
    let rt = runtime(&dir);
    let graph = lower(&rt, &dir, IDLE).graph.expect("lowers");
    let watchdog = RunWatchdog::with_limits(Some(Duration::from_millis(100)), None);
    let race = Arc::new(ActivityAfterTimeoutDecision {
        watchdog: watchdog.clone(),
        observed: AtomicBool::new(false),
    });
    let mut task = None;
    let report = timeout(
        Duration::from_secs(10),
        host::run_configured(
            &rt,
            HostRun::new(graph)
                .observe(race.clone())
                .observe(Arc::new(watchdog.clone())),
            |handle, _| task = Some(watchdog.start(handle)),
        ),
    )
    .await
    .expect("the timeout race and cleanup are bounded")
    .expect("the run completes");
    task.expect("started").stop().await;

    assert!(
        race.observed.load(Ordering::SeqCst),
        "activity was injected from the timeout checkpoint callback"
    );
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(matches!(
        watchdog.timeout_reason(),
        Some(RunTimeout::Stall(timeout))
            if timeout.stall_timeout_ms == 100 && timeout.idle_ms >= 100
    ));
}

struct HeartbeatOnly;

#[async_trait::async_trait]
impl Step for HeartbeatOnly {
    const NAME: &'static str = "test/heartbeat-only";
    type Config = ();

    async fn run(&self, (): (), mut ctx: StepCtx) -> Outcome {
        loop {
            tokio::select! {
                () = sleep(Duration::from_millis(20)) => {
                    let _ = ctx
                        .logs
                        .send(StepEvent::Custom(json!({"kind": "heartbeat"})))
                        .await;
                }
                control = ctx.control.recv() => match control {
                    Some(Control::Cancel | Control::Kill) | None => {
                        return Outcome::cancelled();
                    }
                    Some(_) => {}
                }
            }
        }
    }
}

struct ToolActivityOnly;

#[async_trait::async_trait]
impl Step for ToolActivityOnly {
    const NAME: &'static str = "test/tool-activity-only";
    type Config = ();

    async fn run(&self, (): (), mut ctx: StepCtx) -> Outcome {
        let started = Instant::now();
        loop {
            tokio::select! {
                () = sleep(Duration::from_millis(50)) => {
                    let _ = ctx
                        .logs
                        .send(StepEvent::Custom(json!({
                            "kind": "pebble",
                            "event": {
                                "event": {
                                    "ToolCallStarted": {
                                        "tool_name": "shell"
                                    }
                                }
                            }
                        })))
                        .await;
                    if started.elapsed() >= Duration::from_millis(700) {
                        return Outcome::success(Value::Null);
                    }
                }
                control = ctx.control.recv() => match control {
                    Some(Control::Cancel | Control::Kill) | None => {
                        return Outcome::cancelled();
                    }
                    Some(_) => {}
                }
            }
        }
    }
}

/// Typed heartbeat/status traffic is durable but not meaningful execution
/// progress, so it cannot keep an otherwise idle run alive.
#[tokio::test]
async fn heartbeat_only_traffic_still_expires_as_idle() {
    let dir = RunDir::new("watchdog-heartbeat-only");
    let rt = runtime(&dir).step(HeartbeatOnly);
    let mut builder = GraphBuilder::bare();
    builder.graph_mut().policy.stall_timeout = Some(Duration::from_millis(300));
    builder.graph_mut().policy.max_active_duration = None;
    let scope = builder.add_scope(Scope::new(ScopeId::new(0)));
    builder.add_step("poll", scope, HeartbeatOnly::NAME);
    let watchdog = RunWatchdog::with_limits(Some(Duration::from_millis(300)), None);
    let mut task = None;
    let report = timeout(
        Duration::from_secs(10),
        host::run_configured(
            &rt,
            HostRun::new(builder.build()).observe(Arc::new(watchdog.clone())),
            |handle, _| task = Some(watchdog.start(handle)),
        ),
    )
    .await
    .expect("idle timeout and cleanup are bounded")
    .expect("the run completes");
    task.expect("started").stop().await;

    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(matches!(
        watchdog.timeout_reason(),
        Some(RunTimeout::Stall(timeout))
            if timeout.stall_timeout_ms == 300 && timeout.idle_ms >= 300
    ));
}

/// Pebble's typed tool lifecycle is real execution activity even when a tool
/// is otherwise silent, so it refreshes inactivity structurally.
#[tokio::test]
async fn typed_tool_activity_refreshes_inactivity() {
    let dir = RunDir::new("watchdog-tool-activity");
    let rt = runtime(&dir).step(ToolActivityOnly);
    let mut builder = GraphBuilder::bare();
    builder.graph_mut().policy.stall_timeout = Some(Duration::from_millis(300));
    builder.graph_mut().policy.max_active_duration = None;
    let scope = builder.add_scope(Scope::new(ScopeId::new(0)));
    builder.add_step("tool", scope, ToolActivityOnly::NAME);
    let watchdog = RunWatchdog::with_limits(Some(Duration::from_millis(300)), None);
    let mut task = None;
    let report = timeout(
        Duration::from_secs(10),
        host::run_configured(
            &rt,
            HostRun::new(builder.build()).observe(Arc::new(watchdog.clone())),
            |handle, _| task = Some(watchdog.start(handle)),
        ),
    )
    .await
    .expect("the active tool run is bounded")
    .expect("the run completes");
    task.expect("started").stop().await;

    assert_eq!(report.status, RunStatus::Success);
    assert_eq!(watchdog.timeout_reason(), None);
    assert!(watchdog.state().active_duration_ms >= 600);
}

/// Meaningful output keeps inactivity fresh, but cannot reset the independent
/// maximum active-runtime backstop. Cancellation still follows the native
/// coordinator path and is replayable.
#[tokio::test]
async fn continuous_progress_is_cancelled_at_the_maximum_active_duration() {
    const BUSY: &str = r#"digraph G {
        graph [stall_timeout="100ms", max_active_duration="300ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        long [shape=parallelogram, script="while true; do echo working; sleep 0.02; done"]
        start -> long -> exit
    }"#;
    let dir = RunDir::new("watchdog-maximum-active");
    let rt = runtime(&dir);
    let graph = lower(&rt, &dir, BUSY).graph.expect("lowers");
    let watchdog =
        RunWatchdog::with_limits(graph.policy.stall_timeout, graph.policy.max_active_duration);
    let sink = Arc::new(CollectingSink::default());
    let projector = EventProjector::new(sink.clone());
    let host_run = HostRun::new(graph)
        .observe(Arc::new(watchdog.clone()))
        .observe(projector.clone() as Arc<dyn ExecutionObserver>);
    let mut task = None;
    let report = host::run_configured(&rt, host_run, |handle, _| {
        task = Some(watchdog.start(handle));
    })
    .await
    .expect("the run completes");
    task.expect("started").stop().await;
    let receipt = projector.shutdown().await;
    assert!(receipt.is_clean(), "{receipt:?}");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(matches!(
        watchdog.timeout_reason(),
        Some(RunTimeout::MaximumActiveDuration(timeout))
            if timeout.maximum_active_duration_ms == 300
                && timeout.active_duration_ms >= 300
    ));

    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event.coordinator(),
        Some(CoordinatorEvent::InvocationCancelRequested {
            reason: Some(CancelReason::MaximumActiveDuration {
                maximum_active_duration_ms: 300,
                active_duration_ms,
            }),
            ..
        }) if *active_duration_ms >= 300
    )));
    assert!(events.iter().any(|event| matches!(
        event.view(),
        Some(ViewEvent::RunMaximumActiveDurationExceeded {
            maximum_active_duration_ms: 300,
            active_duration_ms,
        }) if *active_duration_ms >= 300
    )));
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    assert!(replayed.iter().any(|event| matches!(
        event.view(),
        Some(ViewEvent::RunMaximumActiveDurationExceeded { .. })
    )));
}

/// A resumed live firing is reconstructed from its engine log even when the
/// latest timeout checkpoint was written after that firing finished. Its
/// accumulated active time is retained, so redispatch cannot buy a fresh
/// maximum-duration budget.
#[tokio::test]
async fn active_runtime_and_live_work_are_restored_on_resume() {
    const FINITE: &str = r#"digraph G {
        graph [stall_timeout="100ms", max_active_duration="900ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        long [shape=parallelogram, script="i=0; while [ $i -lt 30 ]; do echo working; sleep 0.02; i=$((i+1)); done"]
        start -> long -> exit
    }"#;
    let dir = RunDir::new("watchdog-resume-active");
    let rt = runtime(&dir);
    let mut options = rt.run_options().clone();
    options.retention = Retention::Always;
    let rt = rt.options(options);
    let graph = lower(&rt, &dir, FINITE).graph.expect("lowers");
    let first =
        RunWatchdog::with_limits(graph.policy.stall_timeout, graph.policy.max_active_duration);
    let mut first_task = None;
    let report = host::run_configured(
        &rt,
        HostRun::new(graph).observe(Arc::new(first.clone())),
        |handle, _| first_task = Some(first.start(handle)),
    )
    .await
    .expect("the first lifetime completes");
    first_task.expect("started").stop().await;
    assert_eq!(report.status, RunStatus::Success);
    assert!(first.state().active_duration_ms >= 500);

    let events = fs::read_dir(dir.path().join("executions"))
        .unwrap_or_else(|error| {
            let entries: Vec<_> = fs::read_dir(dir.path())
                .expect("run directory")
                .map(|entry| entry.expect("run entry").file_name())
                .collect();
            panic!("executions: {error}; run entries: {entries:?}")
        })
        .next()
        .expect("one execution")
        .expect("execution entry")
        .path()
        .join("events.jsonl");
    let text = fs::read_to_string(&events).expect("engine log");
    let lines: Vec<&str> = text.lines().collect();
    let cut = lines
        .iter()
        .position(|line| line.contains("\"step.finished\""))
        .expect("the command's finish record");
    fs::write(&events, format!("{}\n", lines[..cut].join("\n"))).expect("truncate engine log");

    let coordinator = dir.path().join("coordinator.jsonl");
    let text = fs::read_to_string(&coordinator).expect("coordinator log");
    let lines: Vec<&str> = text.lines().collect();
    let cut = lines
        .iter()
        .position(|line| line.contains("\"execution.finished\""))
        .expect("the execution finish record");
    fs::write(&coordinator, format!("{}\n", lines[..cut].join("\n")))
        .expect("truncate coordinator log");

    let resumed = RunWatchdog::with_limits(
        Some(Duration::from_millis(100)),
        Some(Duration::from_millis(900)),
    );
    let mut resumed_task = None;
    let started = Instant::now();
    let report = host::resume_configured(
        &rt,
        Vec::new(),
        vec![Arc::new(resumed.clone())],
        |handle, _| resumed_task = Some(resumed.start(handle)),
    )
    .await
    .expect("the run resumes");
    resumed_task.expect("started").stop().await;
    assert_eq!(
        report.status,
        RunStatus::Cancelled,
        "exit={:?}, errors={:?}, history={:?}, releases={:?}",
        report.exit,
        report.state.errors(),
        report.state.history(),
        report.releases
    );
    assert!(
        started.elapsed() < Duration::from_millis(550),
        "resume reset the active budget: {:?}",
        started.elapsed()
    );
    assert!(matches!(
        resumed.timeout_reason(),
        Some(RunTimeout::MaximumActiveDuration(timeout))
            if timeout.maximum_active_duration_ms == 900
                && timeout.active_duration_ms >= 900
    ));
}

struct DelayActiveRedispatch {
    watchdog: RunWatchdog,
    delayed:  AtomicBool,
    parked:   AtomicBool,
}

impl ExecutionObserver for DelayActiveRedispatch {
    fn on_engine_record(&self, _: ExecutionId, _: &EventRecord, _: u64, _: &EngineState) {}

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}

    fn on_execution_resumed(&self, _: ExecutionId, state: &EngineState) {
        let has_question = state.log.records().iter().any(|record| {
            matches!(
                &record.event,
                Event::StepProgressRecorded { ev, .. }
                    if Question::from_event(ev).is_some()
            )
        });
        if !has_question && !self.delayed.swap(true, Ordering::SeqCst) {
            sleep_thread(Duration::from_millis(400));
            self.parked
                .store(self.watchdog.timeout_reason().is_none(), Ordering::SeqCst);
        }
    }
}

/// Recovery preparation for one execution cannot make another restored
/// execution's pending question consume either timeout budget. This exercises
/// the coordinator's actual resume callbacks and delays the active sibling
/// immediately before its watchdog callback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_questions_stay_parked_during_sibling_recovery() {
    let dir = RunDir::new("watchdog-resume-question-sibling");
    let fixture = Arc::new(ResumeTimeoutFixture::default());

    let mut question = GraphBuilder::bare();
    let scope = question.add_scope(Scope::new(ScopeId::new(0)));
    question.add_step("question", scope, ResumeQuestionStep::NAME);
    let question = question.build();

    let mut active = GraphBuilder::bare();
    let scope = active.add_scope(Scope::new(ScopeId::new(0)));
    active.add_step("active", scope, ResumeActiveStep::NAME);
    let active = active.build();

    let mut parent = GraphBuilder::bare();
    let scope = parent.add_scope(Scope::new(ScopeId::new(0)));
    parent.add_node(
        "parent",
        scope,
        StepRef::new(
            ResumeParentStep::NAME,
            json!({
                "question": digest(&question),
                "active": digest(&active),
            }),
        ),
    );
    let parent = parent.build();

    let initial = RunWatchdog::with_limits(None, None);
    let (handle_tx, handle_rx) = oneshot::channel();
    let first_rt = resume_timeout_runtime(&dir, fixture.clone());
    let first_watchdog = initial.clone();
    let first = tokio::spawn(async move {
        host::run_configured(
            &first_rt,
            HostRun::new(parent)
                .with_children(vec![question, active])
                .observe(Arc::new(first_watchdog)),
            |handle, _| {
                let _ = handle_tx.send(handle);
            },
        )
        .await
    });
    let handle = handle_rx.await.expect("the coordinator handle");
    fixture.root_question.notified().await;
    fixture.child_question.notified().await;
    fixture.active_child.notified().await;

    let state = initial.state();
    assert_eq!(state.running.len(), 3, "parent and both children are live");
    assert_eq!(
        state.questions.len(),
        2,
        "the parent and question child are parked"
    );
    assert!(handle.checkpoint_timeout(state).await);
    first.abort();
    let _ = first.await;

    let resumed = RunWatchdog::with_limits(None, Some(Duration::from_millis(150)));
    let delay = Arc::new(DelayActiveRedispatch {
        watchdog: resumed.clone(),
        delayed:  AtomicBool::new(false),
        parked:   AtomicBool::new(false),
    });
    let rt = resume_timeout_runtime(&dir, fixture);
    let mut task = None;
    let report = timeout(
        Duration::from_secs(5),
        host::resume_configured(
            &rt,
            Vec::new(),
            vec![delay.clone(), Arc::new(resumed.clone())],
            |handle, _| task = Some(resumed.start(handle)),
        ),
    )
    .await
    .expect("resume and timeout cleanup are bounded")
    .expect("the run resumes");
    task.expect("watchdog started").stop().await;

    assert!(delay.delayed.load(Ordering::SeqCst));
    assert!(
        delay.parked.load(Ordering::SeqCst),
        "the restored questions parked both clocks during sibling recovery"
    );
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(matches!(
        resumed.timeout_reason(),
        Some(RunTimeout::MaximumActiveDuration(timeout))
            if timeout.maximum_active_duration_ms == 150
                && timeout.active_duration_ms >= 150
    ));
}

/// A pause parks the watchdog: a run paused far longer than its stall
/// budget, with nothing running, is not a stall, and it finishes once
/// unpaused. The watchdog is stopped before the unpause: what the step
/// then takes to launch its sandbox is wall-clock time under test load,
/// not idleness, and the budget is not for measuring it. An unpause
/// restarting the full budget is the watchdog's own unit test.
#[tokio::test]
async fn a_pause_parks_the_watchdog() {
    const IDLE_WHEN_PAUSED: &str = r#"digraph G {
        graph [stall_timeout="400ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a"]
        start -> a -> exit
    }"#;
    let dir = RunDir::new("watchdog-paused");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let graph = lower(&rt, &dir, IDLE_WHEN_PAUSED).graph.expect("lowers");
    let watchdog = StallWatchdog::new(graph.policy.stall_timeout.expect("a budget"));
    let host_run = HostRun::new(graph)
        .observe(Arc::new(controls.clone()))
        .observe(Arc::new(watchdog.clone()));
    controls.pause();
    let paused = controls.clone();
    let monitor = watchdog.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        paused.wire(handle.clone());
        let task = monitor.start(handle);
        let paused = paused.clone();
        tokio::spawn(async move {
            // Three budgets paused, with nothing running.
            sleep(Duration::from_millis(1200)).await;
            task.stop().await;
            paused.unpause().await;
        });
    })
    .await
    .expect("the run completes");
    assert!(watchdog.tripped().is_none(), "a paused run is not stalled");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
}

/// A pending question parks the watchdog: a gate that waits far longer than
/// the stall budget is not a stall, and the run continues once answered.
/// The answer stops the watchdog first, for the same reason the pause test
/// does: the step's sandbox launch is not idleness.
#[tokio::test]
async fn a_pending_question_parks_the_watchdog() {
    const GATED: &str = r#"digraph G {
        graph [stall_timeout="1s"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        gate [shape=hexagon, label="Go?"]
        go [shape=parallelogram, script="echo go"]
        start -> gate
        gate -> go [label="[Y] Yes"]
        go -> exit
    }"#;
    /// Answers after three budgets, stopping the watchdog it is handed first.
    struct Slow(Mutex<Option<WatchdogTask>>);
    #[async_trait::async_trait]
    impl Interviewer for Slow {
        async fn reply(&self, _: InterviewRequest, cancel: CancellationToken) -> InterviewReply {
            tokio::select! {
                () = sleep(Duration::from_secs(3)) => {
                    let task = self.0.lock().expect("the watchdog task").take();
                    if let Some(task) = task {
                        task.stop().await;
                    }
                    InterviewReply::Answered(Answer::choice("Y"))
                }
                () = cancel.cancelled() => InterviewReply::Cancelled,
            }
        }
    }
    let dir = RunDir::new("watchdog-parked");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, GATED);
    let graph = lowered.graph.expect("lowers");
    let watchdog = StallWatchdog::new(graph.policy.stall_timeout.expect("a budget"));
    let slow = Arc::new(Slow(Mutex::new(None)));
    let dispatcher = InterviewDispatcher::new(slow.clone());
    let host_run = HostRun::new(graph)
        .observe(Arc::new(watchdog.clone()))
        .observe(Arc::new(dispatcher.clone()));
    let report = host::run_configured(&rt, host_run, |handle, secrets| {
        dispatcher.wire(handle.clone(), secrets);
        *slow.0.lock().expect("the watchdog task") = Some(watchdog.start(handle));
    })
    .await
    .expect("the run completes");
    let receipt = dispatcher.shutdown().await;
    assert!(
        watchdog.tripped().is_none(),
        "a blocked run never stalls: {:?}",
        watchdog.tripped()
    );
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(receipt.is_clean(), "{:?}", receipt.errors);
}

/// A question parks only its own branch. An independently running sibling
/// keeps consuming the maximum active-runtime budget, and timeout
/// cancellation joins both branches and the pending interview.
#[tokio::test]
async fn an_active_sibling_keeps_a_pending_question_bounded() {
    const PARALLEL: &str = r#"digraph G {
        graph [stall_timeout="0s", max_active_duration="400ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        fork [shape=component]
        gate [shape=hexagon, label="Go?"]
        long [shape=parallelogram, script="sleep 20"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> gate
        fork -> long
        gate -> merge [label="[Y] Yes"]
        long -> merge
        merge -> exit
    }"#;

    struct WaitForCancellation;

    #[async_trait::async_trait]
    impl Interviewer for WaitForCancellation {
        async fn reply(&self, _: InterviewRequest, cancel: CancellationToken) -> InterviewReply {
            cancel.cancelled().await;
            InterviewReply::Cancelled
        }
    }

    let dir = RunDir::new("watchdog-question-with-active-sibling");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, PARALLEL);
    let graph = lowered.graph.expect("lowers");
    let watchdog =
        RunWatchdog::with_limits(graph.policy.stall_timeout, graph.policy.max_active_duration);
    let dispatcher = InterviewDispatcher::new(Arc::new(WaitForCancellation));
    let host_run = HostRun::new(graph)
        .with_children(lowered.children)
        .observe(Arc::new(watchdog.clone()))
        .observe(Arc::new(dispatcher.clone()));
    let mut task = None;
    let report = timeout(
        Duration::from_secs(10),
        host::run_configured(&rt, host_run, |handle, secrets| {
            dispatcher.wire(handle.clone(), secrets);
            task = Some(watchdog.start(handle));
        }),
    )
    .await
    .expect("timeout cancellation and native cleanup are bounded")
    .expect("the run completes");
    task.expect("started").stop().await;
    let receipt = dispatcher.shutdown().await;

    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(matches!(
        watchdog.timeout_reason(),
        Some(RunTimeout::MaximumActiveDuration(timeout))
            if timeout.maximum_active_duration_ms == 400
                && timeout.active_duration_ms >= 400
    ));
    assert!(receipt.is_clean(), "{receipt:?}");
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert!(
        document.complete,
        "all child work is terminal before the host returns: {document:?}"
    );
}

// ── Controls ────────────────────────────────────────────────────────────────

/// Which nodes started, in order.
#[derive(Default)]
struct Starts(Mutex<Vec<String>>);

impl ExecutionObserver for Starts {
    fn on_engine_record(
        &self,
        _: ExecutionId,
        record: &EventRecord,
        _recorded_at: u64,
        state: &EngineState,
    ) {
        if let Event::StepStarted { firing, .. } = &record.event
            && let Some(name) = state
                .firing_node(*firing)
                .and_then(|id| state.graph().node(id))
                .map(|node| node.name.to_string())
        {
            self.0.lock().expect("not poisoned").push(name);
        }
    }

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}
}

/// A paused run admits no new attempt; unpausing releases them, and the
/// held firing kept its identity (one visit, one attempt).
#[tokio::test]
async fn pause_holds_admission_and_unpause_releases_it() {
    const TWO: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a"]
        b [shape=parallelogram, script="echo b"]
        start -> a -> b -> exit
    }"#;
    let dir = RunDir::new("controls-pause");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, &dir, TWO);
    let starts = Arc::new(Starts::default());
    let sink = Arc::new(CollectingSink::default());
    let projector = EventProjector::new(sink.clone());
    let host_run = HostRun::new(lowered.graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(projector.clone() as Arc<dyn ExecutionObserver>)
        .observe(starts.clone());
    controls.pause();
    let paused = controls.clone();
    let seen = starts.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        paused.wire(handle);
        tokio::spawn(async move {
            sleep(Duration::from_millis(700)).await;
            assert!(
                seen.0.lock().expect("not poisoned").is_empty(),
                "nothing started while paused"
            );
            assert!(paused.is_paused());
            paused.unpause().await;
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let started = starts.0.lock().expect("not poisoned").clone();
    assert_eq!(started, ["start", "a", "b", "exit"]);
    for record in report.state.history() {
        assert_eq!(record.attempt.raw(), 1, "{}", record.name);
    }
    let receipt = projector.shutdown().await;
    assert!(receipt.is_clean(), "{receipt:?}");
    // The pause and the unpause are durable coordinator records, in order,
    // before the first attempt started.
    let events = sink.events();
    let notices: Vec<_> = events
        .iter()
        .filter(|e| {
            matches!(
                e.coordinator(),
                Some(CoordinatorEvent::RunPaused | CoordinatorEvent::RunUnpaused)
            )
        })
        .collect();
    assert_eq!(notices.len(), 2, "{events:#?}");
    assert!(matches!(
        notices[0].coordinator(),
        Some(CoordinatorEvent::RunPaused)
    ));
    assert!(matches!(
        notices[1].coordinator(),
        Some(CoordinatorEvent::RunUnpaused)
    ));
    assert!(
        notices
            .iter()
            .all(|e| e.id.source == EventSource::Coordinator && e.subject.is_none()),
        "{notices:#?}"
    );
    assert!(notices[0].id.seq < notices[1].id.seq);
    let first_attempt = events
        .iter()
        .position(|e| matches!(e.engine(), Some(Event::StepStarted { .. })))
        .expect("an attempt started");
    let unpaused = events
        .iter()
        .position(|e| matches!(e.coordinator(), Some(CoordinatorEvent::RunUnpaused)))
        .expect("unpaused");
    assert!(unpaused < first_attempt, "{events:#?}");
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    let durable: Vec<_> = replayed
        .iter()
        .filter(|e| {
            matches!(
                e.coordinator(),
                Some(CoordinatorEvent::RunPaused | CoordinatorEvent::RunUnpaused)
            )
        })
        .map(|e| e.coordinator().cloned())
        .collect();
    assert_eq!(
        durable,
        [
            Some(CoordinatorEvent::RunPaused),
            Some(CoordinatorEvent::RunUnpaused)
        ],
        "replay carries both controls"
    );
}

/// Cancellation stays responsive during a pause: the held firing settles as
/// cancelled and no attempt ever starts.
#[tokio::test]
async fn a_paused_run_can_still_be_cancelled() {
    const ONE: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > ran.txt"]
        start -> a -> exit
    }"#;
    let dir = RunDir::new("controls-pause-cancel");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, &dir, ONE);
    let host_run = HostRun::new(lowered.graph.expect("lowers")).observe(Arc::new(controls.clone()));
    controls.pause();
    let paused = controls.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        paused.wire(handle);
        tokio::spawn(async move {
            sleep(Duration::from_millis(300)).await;
            paused.cancel().expect("the run is live");
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(
        !report
            .state
            .history()
            .iter()
            .any(|r| r.name == "a" && r.attempt.raw() > 0 && r.outcome.status == Status::Success),
        "`a` never ran"
    );
    assert!(!dir.workspace().join("ran.txt").exists());
}

/// A steer reaches the named stage's live firing and never answers its
/// question; the answer still comes from the interviewer.
#[tokio::test]
async fn a_steer_reaches_the_stage_and_does_not_answer_its_question() {
    const GATE: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        gate [shape=hexagon, label="Go?"]
        go [shape=parallelogram, script="echo go"]
        start -> gate
        gate -> go [label="[Y] Yes"]
        go -> exit
    }"#;
    struct AfterSteer {
        controls: ControlService,
        steered:  Mutex<Option<Result<(), ControlError>>>,
    }
    #[async_trait::async_trait]
    impl Interviewer for AfterSteer {
        async fn reply(&self, request: InterviewRequest, _: CancellationToken) -> InterviewReply {
            // Steer the stage while its question is open, then answer.
            let result = self.controls.steer(&request.node, "hurry up").await;
            *self.steered.lock().expect("not poisoned") = Some(result);
            sleep(Duration::from_millis(200)).await;
            InterviewReply::Answered(Answer::choice("Y"))
        }
    }
    let dir = RunDir::new("controls-steer");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, &dir, GATE);
    let interviewer = Arc::new(AfterSteer {
        controls: controls.clone(),
        steered:  Mutex::new(None),
    });
    let dispatcher = InterviewDispatcher::new(interviewer.clone());
    let host_run = HostRun::new(lowered.graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(Arc::new(dispatcher.clone()));
    let report = host::run_configured(&rt, host_run, |handle, secrets| {
        dispatcher.wire(handle.clone(), secrets);
        controls.wire(handle);
    })
    .await
    .expect("the run completes");
    let receipt = dispatcher.shutdown().await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        *interviewer.steered.lock().expect("not poisoned"),
        Some(Ok(()))
    );
    assert!(receipt.is_clean(), "{:?}", receipt.errors);
    assert_eq!(receipt.questions.len(), 1);
    let deliveries = report
        .state
        .log
        .events()
        .filter(|e| matches!(e, Event::ControlRequested { .. }))
        .count();
    assert_eq!(deliveries, 2, "the steer and the answer");
    assert_eq!(
        controls.steer("gate", "too late").await,
        Err(ControlError::NoSuchStage("gate".into()))
    );
    let _: BTreeMap<_, _> = controls.stages();
}

// ── Durable pause across resume ─────────────────────────────────────────────

const PAUSE_TWO: &str = r#"digraph G {
    start [shape=Mdiamond]
    exit [shape=Msquare]
    a [shape=parallelogram, script="echo a > a.txt"]
    b [shape=parallelogram, script="echo b > b.txt"]
    start -> a -> b -> exit
}"#;

/// Pause the run the moment `node` starts, and say when it has finished.
struct PauseOnStart {
    node:     &'static str,
    controls: ControlService,
    finished: Arc<Notify>,
}

impl ExecutionObserver for PauseOnStart {
    fn on_engine_record(
        &self,
        _: ExecutionId,
        record: &EventRecord,
        _recorded_at: u64,
        state: &EngineState,
    ) {
        let named = |firing: &FiringId| {
            state
                .firing_node(*firing)
                .and_then(|id| state.graph().node(id))
                .is_some_and(|node| node.name == self.node)
        };
        match &record.event {
            Event::StepStarted { firing, .. } if named(firing) => self.controls.pause(),
            Event::StepFinished { firing, .. } if named(firing) => self.finished.notify_one(),
            _ => {}
        }
    }

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}
}

/// Run [`PAUSE_TWO`] until `a` has finished under a pause taken as `a`
/// started, then drop the run mid-flight: the crash. Returns the nodes that
/// were admitted before it.
async fn crash_while_paused(dir: &RunDir) -> Vec<String> {
    let controls = ControlService::new();
    let rt = runtime(dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, dir, PAUSE_TWO);
    let finished = Arc::new(Notify::new());
    let starts = Arc::new(Starts::default());
    let host_run = HostRun::new(lowered.graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(Arc::new(PauseOnStart {
            node:     "a",
            controls: controls.clone(),
            finished: finished.clone(),
        }))
        .observe(starts.clone());
    let wired = controls.clone();
    let run = Box::pin(host::run_configured(&rt, host_run, |handle, _| {
        wired.wire(handle);
    }));
    tokio::select! {
        report = run => panic!(
            "the run finished while paused: {:?}",
            report.map(|report| report.status)
        ),
        () = async {
            finished.notified().await;
            // `b` reaches admission and is held; the pause record is durable.
            sleep(Duration::from_millis(400)).await;
        } => {}
    }
    assert!(controls.is_paused());
    let started = starts.0.lock().expect("not poisoned").clone();
    assert_eq!(started, ["start", "a"], "`b` was held at admission");
    started
}

/// What a resumed run admitted during its first `wait`, and whether its
/// control service came back paused.
#[derive(Clone, Default)]
struct ResumeObservation {
    admitted: Vec<String>,
    paused:   bool,
}

/// Every pause and unpause the coordinator records, in order.
#[derive(Default)]
struct PauseRecords(Mutex<Vec<bool>>);

impl ExecutionObserver for PauseRecords {
    fn on_engine_record(&self, _: ExecutionId, _: &EventRecord, _: u64, _: &EngineState) {}

    fn on_lifecycle(&self, record: &CoordinatorRecord) {
        let paused = match record.body {
            CoordinatorEvent::RunPaused => true,
            CoordinatorEvent::RunUnpaused => false,
            _ => return,
        };
        self.0.lock().expect("not poisoned").push(paused);
    }
}

/// A pause and an unpause asked for back to back are recorded in that
/// order, so the run's record ends unpaused, as its gate does.
#[tokio::test]
async fn a_pause_and_an_immediate_unpause_are_recorded_in_order() {
    const SLOW: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="sleep 0.5"]
        start -> a -> exit
    }"#;
    let dir = RunDir::new("controls-pause-order");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let records = Arc::new(PauseRecords::default());
    let host_run = HostRun::new(lower(&rt, &dir, SLOW).graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(records.clone());
    let service = controls.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        service.wire(handle);
        let service = service.clone();
        tokio::spawn(async move {
            service.pause();
            service.unpause().await;
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(report.status, RunStatus::Success);
    assert!(!controls.is_paused());
    assert_eq!(
        *records.0.lock().expect("not poisoned"),
        [true, false],
        "the pause, then the unpause"
    );
}

/// A pause survives the crash: the resumed run admits nothing until an
/// unpause arrives through the new control service, then finishes.
#[tokio::test]
async fn a_pause_survives_resume_and_holds_admission_until_unpaused() {
    let dir = RunDir::new("controls-pause-resume");
    crash_while_paused(&dir).await;

    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let starts = Arc::new(Starts::default());
    let observed = Arc::new(Mutex::new(ResumeObservation::default()));
    let releaser = controls.clone();
    let seen = starts.clone();
    let record = observed.clone();
    let report = host::resume_configured(
        &rt,
        Vec::new(),
        vec![Arc::new(controls.clone()), starts.clone()],
        |handle, _| {
            releaser.wire(handle);
            tokio::spawn(async move {
                sleep(Duration::from_millis(700)).await;
                *record.lock().expect("not poisoned") = ResumeObservation {
                    admitted: seen.0.lock().expect("not poisoned").clone(),
                    paused:   releaser.is_paused(),
                };
                releaser.unpause().await;
            });
        },
    )
    .await
    .expect("resumes");
    let observed = observed.lock().expect("not poisoned").clone();
    assert!(
        observed.paused,
        "the resumed control service starts paused: the last recorded control was a pause"
    );
    assert_eq!(
        observed.admitted,
        Vec::<String>::new(),
        "nothing was admitted while the resumed run waited for the unpause"
    );
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let started = starts.0.lock().expect("not poisoned").clone();
    assert_eq!(started, ["b", "exit"], "the resume continued at `b`");
    for record in report.state.history() {
        assert_eq!(record.attempt.raw(), 1, "{}", record.name);
    }
    // Both controls are durable coordinator records: the pause from the
    // first process, the unpause from the second.
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    let controls_seen: Vec<_> = replayed
        .iter()
        .filter(|e| {
            matches!(
                e.coordinator(),
                Some(CoordinatorEvent::RunPaused | CoordinatorEvent::RunUnpaused)
            )
        })
        .collect();
    assert_eq!(controls_seen.len(), 2, "{replayed:#?}");
    assert!(matches!(
        controls_seen[0].coordinator(),
        Some(CoordinatorEvent::RunPaused)
    ));
    assert!(matches!(
        controls_seen[1].coordinator(),
        Some(CoordinatorEvent::RunUnpaused)
    ));
    assert!(
        controls_seen
            .iter()
            .all(|e| e.id.source == EventSource::Coordinator),
        "{controls_seen:#?}"
    );
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert!(!inspection.paused);
}

/// A resumed run that came back paused can still be cancelled: the held
/// firing settles as cancelled and no attempt starts.
#[tokio::test]
async fn a_paused_resumed_run_can_be_cancelled() {
    let dir = RunDir::new("controls-pause-resume-cancel");
    crash_while_paused(&dir).await;
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert!(inspection.paused, "the run directory records the pause");

    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let starts = Arc::new(Starts::default());
    let canceller = controls.clone();
    let report = host::resume_configured(
        &rt,
        Vec::new(),
        vec![Arc::new(controls.clone()), starts.clone()],
        |handle, _| {
            canceller.wire(handle);
            tokio::spawn(async move {
                sleep(Duration::from_millis(300)).await;
                assert!(canceller.is_paused());
                canceller.cancel().expect("the run is live");
            });
        },
    )
    .await
    .expect("resumes");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert_eq!(
        starts.0.lock().expect("not poisoned").clone(),
        Vec::<String>::new(),
        "no attempt started"
    );
    assert!(!dir.workspace().join("b.txt").exists());
}
