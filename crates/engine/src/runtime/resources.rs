use super::*;

impl WorkflowApplication {
    pub fn capabilities(
        &self,
        access: &AccessContext,
    ) -> Result<ApplicationCapabilities, ForgeError> {
        self.authorize(access, Permission::Read, false)?;
        let c = &self.shared.composition;
        let store = c.store.capabilities();
        Ok(ApplicationCapabilities {
            protocol_version: PROTOCOL_VERSION,
            workflow_format: WORKFLOW_FORMAT.into(),
            checkpoint_format: store.checkpoint_format,
            durable: store.durable,
            artifact_transfers: c.artifacts.host_access()
                && (!store.durable || c.coordinated_artifacts()),
            artifacts_durable: c.artifacts.durable(),
            limits: c.limits.clone(),
        })
    }

    fn authorize_artifacts(&self, access: &AccessContext, writing: bool) -> Result<(), ForgeError> {
        self.authorize(
            access,
            if writing {
                Permission::Start
            } else {
                Permission::Read
            },
            writing,
        )?;
        if !access.permits_resource("artifacts") {
            return Err(ForgeError::new(
                "access.denied",
                "Artifact access is not granted",
            ));
        }
        let c = &self.shared.composition;
        if !c.artifacts.host_access()
            || (c.store.capabilities().durable && !c.coordinated_artifacts())
        {
            return Err(ForgeError::new(
                "capability.unsupported",
                "Host artifact transfers require compatible providers",
            ));
        }
        Ok(())
    }

    pub async fn write_artifact(
        &self,
        access: AccessContext,
        content: ByteStream,
        media_type: &str,
    ) -> Result<ArtifactRef, ForgeError> {
        self.authorize_artifacts(&access, true)?;
        let c = &self.shared.composition;
        c.artifacts
            .write_for_host(&c.id, &access.scope, content, media_type)
            .await
    }

    pub async fn read_artifact(
        &self,
        access: AccessContext,
        reference: &ArtifactRef,
    ) -> Result<ByteStream, ForgeError> {
        self.authorize_artifacts(&access, false)?;
        if reference.scope != access.scope {
            return Err(ForgeError::new(
                "access.denied",
                "Artifact belongs to another scope",
            ));
        }
        let c = &self.shared.composition;
        c.artifacts.read_for_host(&c.id, reference).await
    }
}
