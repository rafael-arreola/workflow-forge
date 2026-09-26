use super::*;

pub(super) struct ReadPage {
    pub descriptor: OperationDescriptor,
}
impl Operation for ReadPage {
    fn descriptor(&self) -> &OperationDescriptor {
        &self.descriptor
    }
    fn execute<'a>(&'a self, ctx: OperationContext, invocation: Invocation) -> OperationFuture<'a> {
        Box::pin(async move {
            let state: Cursor = decode(invocation.input)?;
            let size = invocation.config["batch_size"]
                .as_u64()
                .filter(|n| (1..=MAX_BATCH as u64).contains(n))
                .ok_or_else(|| invalid("Batch size must be between 1 and 100"))?
                as usize;
            let bytes = read_bytes(&ctx, &state.source, MAX_SOURCE_BYTES).await?;
            std::str::from_utf8(&bytes).map_err(|_| invalid("CSV must use UTF-8"))?;
            let mut reader = csv::ReaderBuilder::new()
                .flexible(true)
                .from_reader(bytes.as_slice());
            let headers = reader
                .headers()
                .map_err(|_| invalid("CSV header is unreadable"))?;
            if !headers.iter().eq(["sku", "quantity"]) {
                return Err(invalid("CSV header must be sku,quantity"));
            }
            let mut rows = Vec::with_capacity(size);
            let mut total = 0;
            for (index, record) in reader.records().enumerate() {
                if index >= MAX_ROWS {
                    return Err(invalid("CSV exceeds 10000 records"));
                }
                let record = record.map_err(|_| invalid("CSV record is unreadable"))?;
                total += 1;
                if index < state.cursor || rows.len() == size {
                    continue;
                }
                let sku = record.get(0).unwrap_or_default().trim().to_owned();
                let mut quantity = record.get(1).and_then(|s| s.trim().parse::<u64>().ok());
                let issue = if record.len() != 2 {
                    quantity = None;
                    Some("Expected two columns".into())
                } else if sku.is_empty() || sku.chars().count() > 64 {
                    Some("SKU must contain between 1 and 64 characters".into())
                } else if quantity.is_none() {
                    Some("Quantity must be a nonnegative integer".into())
                } else {
                    None
                };
                rows.push(Row {
                    index,
                    line: record.position().map_or(index as u64 + 2, |p| p.line()),
                    sku,
                    quantity,
                    issue,
                });
            }
            if state.cursor > total {
                return Err(invalid("Cursor is beyond the end of the source"));
            }
            output(&Page {
                cursor: state.cursor,
                next_cursor: state.cursor + rows.len(),
                total,
                rows,
            })
        })
    }
}
