use super::*;

/// Invokes one authorized attempt. It cannot select successors or commit run state.
pub(super) async fn invoke(
    operation: Arc<dyn Operation>,
    context: OperationContext,
    command: Invocation,
) -> Result<Value, ForgeError> {
    let cancellation = context.cancellation.clone();
    let deadline = context.deadline_at_ms;
    let called =
        AssertUnwindSafe(async move { operation.execute(context, command).await }).catch_unwind();
    let result = tokio::select! {
        _=cancellation.cancelled()=>Err(ForgeError::new("operation.cancelled","Operation was cancelled")),
        result=tokio::time::timeout(Duration::from_millis(deadline.saturating_sub(now_ms())),called)=>match result {
            Err(_)=>Err(ForgeError::new("operation.timeout","Operation reached its deadline")),
            Ok(Err(_))=>Err(ForgeError::new("operation.failed","Operation panicked")),
            Ok(Ok(Err(mut error)))=>{
                error.message=error.message.chars().take(1024).collect();
                error.code=error.code.chars().filter(|c|c.is_ascii_alphanumeric() || matches!(c,'.'|'_'|'-')).take(128).collect();
                let mut diagnostic=Diagnostic::new("operation.failed",error.message.clone());
                diagnostic.operation_error=Some(error);diagnostic.retryable=Some(false);
                Err(diagnostic.into())
            }
            Ok(Ok(Ok(output)))=>Ok(output.value),
        }
    };
    cancellation.cancel();
    result
}
