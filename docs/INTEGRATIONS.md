# Official outbound integrations

The `integrations` feature exposes factory functions for HTTP/JSON, file reads and CSV parsing.
The host explicitly creates trusted profiles and registers the resulting `OperationBundle`.
Registering a module does not execute its operations. Clients/configuration are shared;
per-invocation data is not. No connector controls workflow successors.

## Profile identity

HTTP exports `forge.http.<name>` contract 2. File profiles export `forge.files.<name>.read`
contract 1. Implementation revisions derive from adapter version and the serialized profile's
SHA-256. Changing URL, method, root, limits or secret reference changes that revision; rotating
the secret value does not. Obtain exact revisions from descriptors/catalogs rather than guessing
them. Preserve old profiles when retained runs require their revisions.

## HTTP/JSON

`HttpJsonProfile` fixes name, absolute HTTP(S) URL, method, optional bearer secret reference,
timeout and byte budgets. Configured URLs reject userinfo, query and fragments. Invocation input
contains a string-map `query` and optional JSON `body` for writes; it cannot change the endpoint,
method, proxy or headers. GET has no body. Query fields and request JSON are bounded before dispatch.

Defaults are 10 seconds, 1 MiB request JSON and 4 MiB response. Redirects, implicit proxies and
client retries are disabled. Response size counts actual received bytes. Valid JSON responses
return `{status, body}`, including 4xx/5xx; 204 returns a null body. The workflow decides status
semantics. Response headers are not returned, and errors omit remote bodies, URLs and secrets.

GET is Read/Safe; POST/PUT/PATCH/DELETE are Write/Unsafe. Before dispatch, failure is NotApplied.
After dispatching a write, timeout, disconnect or an invalid response is Unknown. The generic
adapter does not infer idempotency from a header and does not provide a business inspector.
A destination-specific adapter can expose Keyed repetition/reconciliation when justified.

Bearer credentials resolve through `OperationContext::secret` and a declared `secret:<name>`
resource. The host controls allowed destinations. General scope permissions apply to operations
without secrets; there is no additional per-endpoint ACL hidden in this module.

## Files

`FileReadProfile` fixes an absolute root, name, media type and byte limit (4 MiB by default).
Input `{path}` is relative, at most 1,024 bytes, without `..` or absolute paths. A `cap-std`
directory handle resolves within the configured root, including symbolic-link boundaries.
Only regular files are accepted. Bounded chunks stream to the artifact port; output is an
`ArtifactRef`. Diagnostics do not expose paths/content. The operation is Read/Safe and requires
`artifacts` access.

A repeated read may see changed source content; a confirmed published artifact preserves its
bytes. There is no arbitrary file-write operation or promise to interrupt an OS syscall.
Hosts must use local regular files they control. Compiled modules are not sandboxed.

## CSV batches

`csv_operations(CsvOptions)` exports `forge.csv.read_batch`. Input is `{source, offset?, size?}`
with a same-scope artifact. Output contains `headers`, rows `{index, line, fields, valid_columns}`
and nullable `next_offset`. Offset counts data records from zero; line is the parser's physical
line number, including quoted/multiline fields. The module does not interpret domain quantities,
dates or inventory rules. Column mismatch is recorded per row; invalid UTF-8, headers or budgets
reject parsing.

Defaults: 4 MiB source, 10,000 records, 128 columns, 16 KiB per field, batch size 100 and maximum
batch size 1,000. The current parser rereads the bounded source for each batch; it is not a
constant-memory parser for unlimited input. The engine limits concurrency. Access uses the
operation context, never execution-store tables. Durable runs need coordinated artifact storage.

See [inventory proof of concept](../examples/host/examples/v2_inventory.rs) for composition with
a separate business module and [module tests](../crates/modules/tests) for local connector fixtures.
