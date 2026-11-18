use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use executor::{AcquireContext, MapSecrets, NoProgress, ScopeSpec, ServiceSpec};
use ir::{ContainerOptions, RegistryCredentials, RuntimeSpec, RuntimeTarget, ScopeId};
use sandbox_driver::{
    Capabilities, Error, EventContext, Isolation, ProviderKind, ResourceKind, Resources, Sandbox,
    SandboxFilter, SandboxId, SandboxKind, SandboxProvider, SandboxSource, SandboxSpec,
    SnapshotFilter, SnapshotId, SnapshotProvider, SnapshotSpec, SnapshotState, SnapshotStatus,
};
use sandbox_driver_daytona_config::{DaytonaProviderConfig, DockerExecutionTarget};
use testkit::RunDir;

use crate::snapshots::RunnerSnapshot;
use crate::{
    FixedProvider, MemoryLedger, RunIdentity, SandboxBackend, SandboxExecutor, SandboxOptions,
    build_daytona_spec,
};

#[derive(Default)]
struct Snapshots {
    status: Mutex<Option<SnapshotStatus>>,
}

#[async_trait]
impl SnapshotProvider for Snapshots {
    async fn create(
        &self,
        spec: &SnapshotSpec,
        _: Option<EventContext>,
    ) -> sandbox_driver::Result<SnapshotId> {
        let id = SnapshotId::try_new(spec.name.as_ref().unwrap()).unwrap();
        let mut status = SnapshotStatus::new(id.clone(), SnapshotState::Active);
        status.sandbox_kind = spec.sandbox_kind;
        let mut resources = spec.resources;
        resources.disk_mb.get_or_insert(3 * 1024);
        status.resources = Some(resources);
        status.regions = spec.region.iter().cloned().collect();
        *self.status.lock().unwrap() = Some(status);
        Ok(id)
    }

    async fn get(&self, id: &SnapshotId) -> sandbox_driver::Result<SnapshotStatus> {
        self.status
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::NotFound {
                resource: ResourceKind::Snapshot,
                id:       id.as_str().to_owned(),
            })
    }

    async fn list(&self, _: &SnapshotFilter) -> sandbox_driver::Result<Vec<SnapshotStatus>> {
        panic!("runner snapshot preparation does not list snapshots")
    }

    async fn delete(&self, _: &SnapshotId, _: Option<EventContext>) -> sandbox_driver::Result<()> {
        panic!("runner snapshots are shared")
    }
}

struct DaytonaProvider {
    kind:         ProviderKind,
    capabilities: Capabilities,
    snapshots:    Snapshots,
}

impl DaytonaProvider {
    fn new() -> Self {
        Self {
            kind:         ProviderKind::try_new("daytona").unwrap(),
            capabilities: Capabilities::minimal(Isolation::Container),
            snapshots:    Snapshots::default(),
        }
    }
}

#[async_trait]
impl SandboxProvider for DaytonaProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn create(
        &self,
        _: &SandboxSpec,
        _: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        panic!("the test builds a spec without creating a sandbox")
    }

    async fn attach(
        &self,
        _: &SandboxId,
        _: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        panic!("the test does not attach a sandbox")
    }

    async fn list(
        &self,
        _: &SandboxFilter,
    ) -> sandbox_driver::Result<Vec<sandbox_driver::SandboxStatus>> {
        panic!("the test does not list sandboxes")
    }

    fn snapshots(&self) -> Option<&dyn SnapshotProvider> {
        Some(&self.snapshots)
    }
}

fn snapshot() -> RunnerSnapshot {
    RunnerSnapshot::new(
        "pinned-runner",
        Resources::default(),
        SandboxKind::VirtualMachine,
        None,
    )
}

#[test]
fn container_offering_supports_process_and_nested_container_jobs() {
    let snapshot = RunnerSnapshot::new(
        "pinned-runner",
        Resources::default(),
        SandboxKind::Container,
        None,
    );
    for container_job in [false, true] {
        let mut scope = ScopeSpec::new(ScopeId::new(1), "job");
        if container_job {
            scope.runtime.target = RuntimeTarget::Container {
                image:       "alpine:3.20".into(),
                options:     ContainerOptions::default(),
                credentials: None,
            };
        }
        let spec = build_daytona_spec(&scope, &AcquireContext::bare(), &snapshot).unwrap();
        assert_eq!(spec.sandbox_kind, Some(SandboxKind::Container));
        let config: DaytonaProviderConfig = serde_json::from_value(spec.provider_config).unwrap();
        assert_eq!(
            config.docker.unwrap().target,
            if container_job {
                DockerExecutionTarget::Container
            } else {
                DockerExecutionTarget::VirtualMachine
            }
        );
    }
}

#[test]
fn a_process_target_uses_the_vm_and_disables_automatic_stops_and_deletion() {
    let scope = ScopeSpec::new(ScopeId::new(1), "job");
    let spec = build_daytona_spec(&scope, &AcquireContext::bare(), &snapshot()).unwrap();
    spec.validate().unwrap();
    assert!(matches!(&spec.source, SandboxSource::Snapshot { id } if id == &snapshot().id));
    assert_eq!(spec.sandbox_kind, Some(SandboxKind::VirtualMachine));
    assert_eq!(
        spec.resources,
        Resources::default(),
        "the snapshot owns resource allocation"
    );
    assert_eq!(spec.timers.auto_stop_after_idle, Some(Duration::ZERO));
    assert_eq!(spec.timers.auto_pause_after_idle, Some(Duration::ZERO));
    assert_eq!(spec.timers.auto_delete_after_stop, Some(Duration::ZERO));
    assert_eq!(spec.timers.ttl, Some(Duration::ZERO));
    assert_eq!(spec.public, Some(false));
    let config: DaytonaProviderConfig = serde_json::from_value(spec.provider_config).unwrap();
    let docker = config.docker.unwrap();
    assert_eq!(docker.target, DockerExecutionTarget::VirtualMachine);
    assert!(docker.image.is_empty());
    assert!(docker.options.binds.is_empty());
}

#[test]
fn container_jobs_keep_options_services_and_credentials_inside_the_vm() {
    let mut scope = ScopeSpec::new(ScopeId::new(1), "job");
    scope.env.insert("SCOPE_VALUE".into(), "scope".into());
    scope.runtime = RuntimeSpec {
        target:       RuntimeTarget::Container {
            image:       "private/job:pin".into(),
            options:     ContainerOptions {
                user: Some("1000".into()),
                privileged: true,
                env: vec![("SCOPE_VALUE".into(), "container".into())],
                ..Default::default()
            },
            credentials: Some(RegistryCredentials {
                username:        "user".into(),
                password_secret: "REGISTRY_PASSWORD".into(),
            }),
        },
        requirements: Vec::new(),
    };
    scope.services.push(ServiceSpec::new("db", "postgres:16"));
    let ctx = AcquireContext::new(
        Arc::new(MapSecrets::from_pairs(&[(
            "REGISTRY_PASSWORD",
            "private-password",
        )])),
        Arc::new(NoProgress),
    );
    let spec = build_daytona_spec(&scope, &ctx, &snapshot()).unwrap();
    assert_eq!(spec.env["SCOPE_VALUE"], "container");
    assert_eq!(
        spec.user.as_deref(),
        Some("root"),
        "VM bootstrap keeps its own user"
    );
    assert!(!format!("{spec:?}").contains("private-password"));
    let config: DaytonaProviderConfig = serde_json::from_value(spec.provider_config).unwrap();
    let docker = config.docker.unwrap();
    assert_eq!(docker.target, DockerExecutionTarget::Container);
    assert_eq!(docker.image, "private/job:pin");
    assert_eq!(docker.user.as_deref(), Some("1000"));
    assert!(docker.options.privileged);
    assert!(!docker.options.host_network);
    assert!(docker.options.extra_hosts.is_empty());
    assert!(docker.options.binds.is_empty());
    assert_eq!(docker.options.sidecars[0].name, "db");
    assert_eq!(
        docker.options.registry_auth.unwrap().password,
        "private-password"
    );
}

#[tokio::test]
async fn docker_host_network_does_not_reach_daytonas_nested_docker() {
    let dir = RunDir::new("sandbox-daytona-host-network-exclusion");
    let provider = Arc::new(DaytonaProvider::new());
    let executor = SandboxExecutor::new(
        Arc::new(FixedProvider::new(provider.clone())),
        Arc::new(MemoryLedger::default()),
        Arc::new(RunIdentity::for_run_dir(dir.path().to_path_buf())),
        executor::Retention::Never,
        None,
        SandboxOptions {
            backend: SandboxBackend::Daytona,
            docker_host_network: true,
            ..Default::default()
        },
    );
    let scope =
        ScopeSpec::new(ScopeId::new(1), "job").with_runtime(RuntimeSpec::container("alpine:3.20"));
    let spec = executor
        .build_spec(
            &scope,
            &[],
            "daytona-host-network-exclusion",
            &AcquireContext::bare(),
            &*provider,
        )
        .await
        .unwrap();
    let config: DaytonaProviderConfig = serde_json::from_value(spec.provider_config).unwrap();
    assert!(!config.docker.unwrap().options.host_network);
}

#[test]
fn a_vm_process_target_with_sidecars_fails_before_snapshot_preparation() {
    let scope = ScopeSpec::new(ScopeId::new(1), "job")
        .with_services(vec![ServiceSpec::new("db", "postgres:16")]);
    assert!(build_daytona_spec(&scope, &AcquireContext::bare(), &snapshot()).is_err());
}
