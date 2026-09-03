# Changelog

All notable changes to `atl-server` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this crate
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This file starts at 0.25.0. Earlier history is in the commit log.

## [0.25.0]

### `atl-core` 0.23.2 -> 0.28.0

The server had been three JCS fixes behind the library it issues receipts
with. Two API changes came with the jump.

`canonicalize` and `canonicalize_and_hash` are fallible now
(`AtlResult<_>`), because a value that has no canonical form has no hash
either, and inventing one produces a `metadata_hash` no other implementation
can reproduce. Every caller here propagates the refusal; none of them
`unwrap` it and none substitute a default.

`Receipt` is read-only: its fields are private, read through `entry()`,
`proof()`, `anchors()`, `super_proof()`, `spec_version()` and
`upgrade_url()`, and it is built with `ReceiptBuilder` rather than a struct
literal. Both construction sites in `receipt::generator` moved over.
`ReceiptBuilder::build` takes a `SourceTextCheck`; see below for what this
server is entitled to assert there.

### RFC 8785 Section 3.1 is checked on the ingress bytes

> JSON objects MUST NOT exhibit duplicate property names
>
> — RFC 8785 Section 3.1

The server validated nothing at all about client metadata. It hashed
whatever `serde_json` handed back, and `serde_json` resolves
`{"x":1,"x":2}` by keeping the last occurrence — so by the time
`req.metadata` existed the ambiguity was already gone and no check placed
there could have seen it. RFC 8259 Section 4 makes the surviving occurrence
unpredictable across parsers, so two conformant readers could canonicalize
two different objects out of one request body and the log would commit to
whichever `serde_json` happened to pick.

**The check therefore runs on the raw text, before the typed parse**, using
`atl_core::check_unique_property_names`, on every route that can put a
metadata document into the log:

* `POST /v1/anchor` (JSON) — over the whole request body, in
  `api::handlers::anchor::anchor_json`, before `serde_json::from_str`.
  Scoping it to the `metadata` subtree would mean finding that subtree in
  raw text with an ad-hoc scanner whose idea of where it starts could
  disagree with `serde_json`'s; and a body that states `"metadata"` twice is
  ambiguous in exactly the way the constraint exists to prevent.
* gRPC `AnchorEntry` and `AnchorBatch` — over `metadata_json`, which *is*
  the raw text, so the scope there is exactly the metadata document.

Refusal is `400 Bad Request` with error code `DUPLICATE_PROPERTY_NAME`,
carrying the RFC 6901 JSON Pointer of the offending object; the gRPC paths
answer `INVALID_ARGUMENT`. This is a client defect, not an operator one, and
it must not page anybody.

`POST /v1/anchor` with `multipart/form-data` still answers `501`: no
multipart ingress exists, so there is nothing to check on that route yet.
The requirement is written down where the handler would be implemented.

### Canonicalization refusals reach the client as 4xx

`ServerError::Core` maps to `500`, so propagating an `AtlError` with `?`
would have turned a client's un-canonicalizable document into a server
fault. Two variants were added instead, both `400`:

* `DuplicatePropertyName { location, reason }` — `DUPLICATE_PROPERTY_NAME`
* `NotCanonicalizable(String)` — `NOT_CANONICALIZABLE`

`api::streaming::hash_payload`, `hash_json_payload` and `hash_metadata`
return `ServerResult<[u8; 32]>` and map the refusal to the second of these.

### Large integers are anchored, not refused

RFC 8785 Appendix B notes (1) and (2) put the safe-integer range outside the
canonicalizer's concern — "how numbers are used in applications does not
affect the JCS algorithm" — and the RFC's own Table 1 normalizes 2\*\*68 to
`295147905179352830000` rather than rejecting it. Nothing here refuses a
number, and a test walks the whole circle for a nanosecond timestamp to keep
it that way: raw client text `{"ts":1756812345678901234}` -> the hash the
leaf commits -> the issued receipt as bytes -> a verifier's independent
recomputation from those bytes.

### The `SourceTextCheck` escape hatch, and what backs it

`ReceiptBuilder::build` is given
`SourceTextCheck::assume_duplicate_property_names_already_rejected()` at
both issuance sites. Two independent facts back the assertion, and they are
recorded on `receipt::generator::issuance_provenance`:

1. The receipt is assembled in memory and serialized by this server. Its
   properties are distinct struct fields and its `metadata` is a
   `serde_json::Value`, whose `Map` cannot hold one key twice — so no text
   `serde_json` produces from it can repeat a name.
2. The metadata document itself was checked as text at ingress, on every
   route listed above. Storage does not reintroduce the hazard: the column
   is written from `Value::to_string()` and read back with `from_str`.

Fact 1 alone would justify the marker. Fact 2 is what makes the
`metadata_hash` a commitment to a document the client unambiguously sent.
Entries written before this release are covered by fact 1 only.
