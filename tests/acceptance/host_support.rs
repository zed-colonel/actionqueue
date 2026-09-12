#![allow(dead_code)]
use actionqueue_core::{bounded::OpaqueRef, causal::ControlMutationContext, control::*};
pub fn host(scope: ControlScope) -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope,
        attribution: ControlMutationContext::new(OpaqueRef::new("fixture-host").unwrap()),
    }
}
#[cfg(feature = "platform")]
pub fn tenant<W: actionqueue_storage::wal::writer::WalWriter>(
    a: &mut actionqueue_storage::mutation::StorageMutationAuthority<
        W,
        actionqueue_storage::recovery::reducer::ReplayReducer,
    >,
    tenant: actionqueue_core::ids::TenantId,
) -> Result<HostControlContext, ControlError> {
    use actionqueue_core::{actor::*, ids::*, mutation::*, platform::*};
    use actionqueue_runtime::control::execute_mutation;
    if a.projection().get_tenant(&tenant).is_none() {
        return Err(ControlError::Scope);
    }
    let actor: ActorId = tenant.to_string().parse().unwrap();
    if a.projection().get_actor(&actor).is_none() {
        let seq = a.projection().latest_sequence() + 1;
        let _ = execute_mutation(
            a,
            &host(ControlScope::ProvisionTenant(tenant)),
            MutationCommand::ActorRegister(ActorRegisterCommand::new(
                seq,
                ActorRegistration::new(
                    actor,
                    "fixture-actor",
                    ExecutorTraits::new(vec!["compute".into()]).unwrap(),
                    u64::MAX / 3,
                )
                .with_tenant(tenant),
                0,
            )),
        )?;
        let seq = a.projection().latest_sequence() + 1;
        let _ = execute_mutation(
            a,
            &host(ControlScope::Store),
            MutationCommand::RoleAssign(RoleAssignCommand::new(
                seq,
                actor,
                Role::Operator,
                tenant,
                0,
            )),
        )?;
        for action in [
            QueueAction::AdmitTask,
            QueueAction::AdmitSignal,
            QueueAction::InspectTask,
            QueueAction::InspectSignal,
            QueueAction::InspectWait,
            QueueAction::ResolveWait,
            QueueAction::CancelWait,
            QueueAction::CancelTask,
            QueueAction::CancelRun,
            QueueAction::RetainSignal,
            QueueAction::ManageBudget,
            QueueAction::ManageSubscription,
            QueueAction::AppendLedger,
        ] {
            let seq = a.projection().latest_sequence() + 1;
            let _ = execute_mutation(
                a,
                &host(ControlScope::Store),
                MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
                    seq,
                    actor,
                    action.permission(),
                    tenant,
                    0,
                )),
            )?;
        }
    }
    Ok(HostControlContext { actor_id: Some(actor), ..host(ControlScope::Tenant(tenant)) })
}
#[cfg(feature = "platform")]
pub fn bind_engine_tenant<
    H: actionqueue_executor_local::handler::ExecutorHandler + 'static,
    C: actionqueue_core::time::clock::Clock,
>(
    boot: &mut actionqueue_runtime::engine::BootstrappedEngine<H, C>,
    tenant: actionqueue_core::ids::TenantId,
    actor: Option<actionqueue_core::ids::ActorId>,
) {
    use actionqueue_core::{actor::*, ids::*, platform::*};
    let actor = actor.unwrap_or_else(|| tenant.to_string().parse::<ActorId>().unwrap());
    if boot.projection().get_actor(&actor).is_none() {
        boot.set_control_context(Some(host(ControlScope::ProvisionTenant(tenant))));
        boot.register_actor(
            ActorRegistration::new(
                actor,
                "fixture-control",
                ExecutorTraits::new(vec!["compute".into()]).unwrap(),
                u64::MAX / 3,
            )
            .with_tenant(tenant),
        )
        .unwrap();
    }
    boot.set_control_context(Some(host(ControlScope::Store)));
    if boot.projection().get_role_assignment(actor, tenant).is_none() {
        boot.assign_role(actor, Role::Operator, tenant).unwrap();
    }
    for action in [QueueAction::AppendLedger, QueueAction::AdmitTask] {
        if !boot.projection().capability_grants().any(|g| {
            g.actor_id == actor
                && g.tenant_id == tenant
                && g.capability == action.permission()
                && g.revoked_at.is_none()
        }) {
            boot.grant_capability(actor, action.permission(), tenant).unwrap();
        }
    }
    boot.set_control_context(Some(HostControlContext {
        actor_id: Some(actor),
        ..host(ControlScope::Tenant(tenant))
    }));
}
