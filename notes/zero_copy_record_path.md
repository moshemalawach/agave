# Project E — Zero-copy transaction recording on the PoH path

## TL;DR

Stage 1 (deep study) is done. The optimisation is **viable in principle** but
**not tractable in a single session**: the `Vec<VersionedTransaction>` carried
by `Entry` is the type that flows through the leader's working-bank channel,
the broadcast/coalesce path, the TPU entry notifier, and ultimately into
`wincode::serialize(component)` inside `Shredder::make_merkle_shreds_from_component`.
Touching that type to carry pre-encoded wire bytes is a consensus-adjacent
change spanning `entry`, `poh`, `core`, and `turbine` and well above the 500
LOC ceiling the brief sets.

What is committed in this session: just this notes file. Code experiments
were performed and discarded; nothing else is staged.

## Execution path (with type signatures)

```
core/src/banking_stage/consumer.rs:353
  processing_results.iter()
      .zip(batch.sanitized_transactions())             // [RuntimeTransaction<ResolvedTransactionView<SharedBytes>>]
      .filter_map(|(res, tx)| res.was_processed().then(|| tx.to_versioned_transaction()))
      .collect_vec()                                    // -> Vec<VersionedTransaction>   <-- 6 allocs / tx

self.transaction_recorder.record_transactions(bank_id, processed_transactions)
  // poh/src/transaction_recorder.rs:52
  // fn record_transactions(&self, bank_id, transactions: Vec<VersionedTransaction>) -> RecordTransactionsSummary
  hash_transactions(&transactions)                      // entry::entry::hash_transactions
                                                        //   = MerkleTree over flat-mapped signatures
  self.record(bank_id, vec![hash], vec![transactions])
  -> record_sender.try_send(Record::new(mixins, transaction_batches, bank_id))

poh/src/poh_recorder.rs:328
  fn record(&mut self, bank_id, mixins, transaction_batches: Vec<Vec<VersionedTransaction>>)
    poh.record_batches(&mixins, &mut self.entries)      // -> hash chain advance, fills self.entries: Vec<PohEntry>
    for (entry, transactions) in self.entries.drain(..).zip(transaction_batches) {
        working_bank_sender.send((bank, (Entry { num_hashes, hash, transactions }.into(), tick_height)))
    }
  // working_bank_sender: Sender<(Arc<Bank>, (EntryOrMarker, u64))>
  // EntryOrMarker = Entry(Entry) | Marker(VersionedBlockMarker)

core/src/tpu_entry_notifier.rs:77
  // Reads only entry.num_hashes / entry.hash / entry.transactions.len()
  // Forwards (bank, (entry_or_marker, tick_height)) onwards to broadcast_entry_sender.

turbine/src/broadcast_stage/broadcast_utils.rs:72
  fn recv_slot_components(...) -> ReceiveResults {
      component: BlockComponent::EntryBatch(entries: Vec<Entry>)  // coalesces ENTRY_COALESCE_DURATION
      ...
      serialized_size(&entries)                                   // wincode::serialized_size, walks struct
  }

turbine/src/broadcast_stage/standard_broadcast_run.rs:178
  fn component_to_shreds(&mut self, keypair, component: &BlockComponent, ...)
    -> Shredder::make_merkle_shreds_from_component(...)

ledger/src/shredder.rs:67
  pub fn make_merkle_shreds_from_component(...)
      let bytes = wincode::serialize(component).unwrap();         // <-- the second & final encode
      Self::make_shreds_from_data_slice(&self, keypair, &bytes, ...)
```

The redundant work in `to_versioned_transaction`
(`runtime-transaction/src/runtime_transaction/transaction_view.rs:186`):

| Allocation                                   | Size                            |
|----------------------------------------------|---------------------------------|
| `static_account_keys: Vec<Pubkey>`           | up to 256 * 32 B               |
| `instructions: Vec<CompiledInstruction>`     | one per ix                     |
| `CompiledInstruction.accounts: Vec<u8>`      | one per ix                     |
| `CompiledInstruction.data: Vec<u8>`          | one per ix                     |
| `address_table_lookups[i].writable_indexes`  | one per ATL                    |
| `address_table_lookups[i].readonly_indexes`  | one per ATL                    |
| `signatures: Vec<Signature>`                 | one per tx                     |
| (outer `Vec<VersionedTransaction>` collect)  | one per batch                  |

For a typical 1-ix legacy transfer this is ~6 fresh `Vec`s; for a v0 tx with
2 ixs and 1 ATL it's ~10. Multiply by the per-second tx rate at a hot leader
to get the back-of-envelope 5-10% banking saving.

## What every downstream consumer actually needs

| Site                                                       | Reads              | Needs structured form? |
|------------------------------------------------------------|--------------------|-----------------------|
| `transaction_recorder::hash_transactions`                  | signatures only    | No                    |
| `core/src/tpu_entry_notifier.rs:77-95`                     | `transactions.len()` | No                  |
| `turbine/.../broadcast_utils.rs::recv_slot_components`     | `wincode::serialized_size(&entries)`         | No (can be tracked alongside) |
| `turbine/.../standard_broadcast_run.rs::component_to_shreds` | wincode bytes (via `Shredder::make_merkle_shreds_from_component`) | No |
| `turbine/.../broadcast_duplicates_run.rs:128-129`          | `entry.transactions[0].message.recent_blockhash()`  | **Yes** (test/duplicate-attack path only — gated to non-prod scenarios) |
| `core/src/banking_stage.rs:999`                            | `entry.transactions.is_empty()` (test) | No |
| Geyser / status sender                                     | Operates on `SanitizedTransaction` from the committer, not on the recorded `VersionedTransaction`. | No (independent path) |

In other words: **on the production leader path, no consumer requires the
materialised `VersionedTransaction`**. The structure is built only to be
re-serialised back to wire bytes a few hops later.

## Wire bytes are what wincode would write — but only for Legacy and V0

`SanitizedTransactionView::data()`
(`transaction-view/src/transaction_view.rs:177`) returns the full serialised
transaction the validator parsed off the wire. The validator obtains it by:

```
SanitizedTransactionView::try_new_sanitized(packet_data, true)
  -> TransactionFrame::try_new(data) // parses bincode/wincode layout in-place
```

For Legacy and V0 transactions, the wire layout matches the canonical
bincode/wincode layout of `VersionedTransaction { signatures, message }`:
`[short_vec_len(sig_count)] [sigs...] [message_bytes]`. No maps, no
`f32`/`f64`, no `bool` rewrite paths, no `Option<T>` discriminant
ambiguities. So for any Legacy or V0 tx that successfully sanitised,
`wincode::serialize(&view.to_versioned_transaction()) == view.data()`.

**V1 (SIMD-0385) transactions break this equivalence** — corrected during
review. The V1 wire format puts signatures at the *end* of the packet
(see `transaction-view/src/transaction_frame.rs:93-186`):
`[version_byte] [header] [config_mask] [lifetime] [num_ix] [num_addr]
[addresses] [config_values] [instruction_payloads] [signatures]`.
Re-serialising via `wincode::serialize(&VersionedTransaction)` would emit
signatures first (because the struct field order is `{ signatures,
message }` and there is no custom `SchemaWrite` for V1 that reorders them).
There is therefore **no zero-copy path for V1 transactions** without
either (a) extending wincode's `SchemaWrite` for `v1::Message` to emit
signatures at the end (a consensus-format change well outside the scope
of this project), or (b) gating the zero-copy path on
`tx.version() != V1` and falling back to materialise-and-reserialise for
V1.

Option (b) is acceptable in practice because V1 is **pre-mainnet** —
it's not yet a production traffic class, so the gating cost is zero
today and small even after activation. But the gate must be explicit:
the next session must NOT include V1 in the byte-equivalence test
population without first confirming wincode's V1 emission. The currently
written Phase 1 plan ("legacy 1-sig, legacy multi-sig, v0 ATL,
v0 multi-ATL, v1 with config") would fail on the V1 case.

### Pre-existing test breakage in `runtime-transaction`

I attempted to add a unit test verifying byte equality for legacy and v0
transactions; it failed to compile because `Hash::new_unique()` is not
visible to the test target. Diagnosis (corrected from an earlier
mis-framing):
`runtime-transaction/Cargo.toml:28` only declares `solana-hash` in
`[dependencies]`, not in `[dev-dependencies]`. The production dep does
not enable the `atomic` feature, and within the isolated crate graph
nothing else turns it on, so `Hash::new_unique()` (gated on
`feature = "atomic"`) is unavailable in test compilation.

The fix is to add to `[dev-dependencies]` in
`runtime-transaction/Cargo.toml`:

```toml
solana-hash = { workspace = true, features = ["atomic"] }
```

This unifies the feature for tests without changing the production dep.
Orthogonal to Project E.

## Why this is more than 500 LOC, by section

To pipe wire bytes end-to-end, the in-memory shape that crosses the channel
has to change. The minimum coupling looks like this:

1. **`entry::Entry`** (`entry/src/entry.rs:99-112`). The wincode schema for
   `transactions` is `WincodeVec<VersionedTransaction, MaxDataShredsLen>`.
   Either the field type changes to a new enum
   `EntryTransactions = Materialised(Vec<VersionedTransaction>) | Wire(Vec<RawTxBytes>)`
   with a custom `SchemaWrite` that emits the same bytes on either branch, or
   we add a sibling `RawEntry` type and the channel becomes
   `enum WireOrEntry { Wire(RawEntry), Built(Entry) }`. Either way this is
   the consensus-critical entry serialisation surface and demands the
   byte-equivalence test the brief flagged.

2. **`poh::poh_recorder::Record`** (`poh/src/poh_recorder.rs:80-98`) carries
   `transaction_batches: Vec<Vec<VersionedTransaction>>`. Needs to grow a
   variant or sibling type that holds raw byte slices + signatures (signatures
   are needed for the `hash_transactions` step that runs *before* the record
   is sent).

3. **`poh::transaction_recorder::TransactionRecorder::record_transactions`**
   (`poh/src/transaction_recorder.rs:52`). Either a new `record_transactions_zero_copy`
   accepting `&[&SanitizedTransactionView<D>]` plus filter indices, or a new
   trait that abstracts "thing-with-signatures-and-wire-bytes". Either way
   `consumer.rs:353-380` is rewritten.

4. **`turbine` broadcast/coalesce path**
   (`broadcast_utils.rs::recv_slot_components`, `standard_broadcast_run.rs::component_to_shreds`).
   `BlockComponent::EntryBatch(Vec<Entry>)` either grows a wire variant or its
   `SchemaWrite` impl learns to dispatch on the new entry shape. The
   serialised-size accounting at `broadcast_utils.rs:148` and `:201` becomes
   an O(1) sum-of-known-sizes instead of a struct walk.

5. **`turbine/.../broadcast_duplicates_run.rs:128-129`** reads
   `entry.transactions[0].message.recent_blockhash()`. The duplicate-broadcast
   flow is a behind-cfg test/attack feature, but its presence means a wire-bytes
   variant can't simply omit access — it has to either fall back to parsing or
   we accept that this code only works in the `Materialised` branch.

6. **`core::tpu_entry_notifier`** (`tpu_entry_notifier.rs:77-95`) only needs
   `transactions.len()`; trivial accessor on the new shape.

7. **The leader-side `SharedBytes` lifetime story.** The
   `RuntimeTransaction<ResolvedTransactionView<SharedBytes>>` that
   `consumer.rs` holds has a lifetime tied to the `TransactionBatch` /
   `BankingPacketBatch`. Today the `Vec<VersionedTransaction>` we collect
   detaches from those bytes (it owns its own copy). For zero-copy we must
   either:
   - clone the `SharedBytes` (a single `Arc::clone` per tx, much cheaper than
     6 `Vec` allocations), and the entry/channel hold those `SharedBytes`
     until the shredder has copied them, or
   - copy each tx's wire bytes into a single arena buffer that the entry
     points into.
   The first option is simpler and the more obvious starting design.

That's at minimum: `entry/src/entry.rs` (consensus-critical), one new module
in `entry/src` for the wire variant, `poh/src/poh_recorder.rs`,
`poh/src/transaction_recorder.rs`, `core/src/banking_stage/consumer.rs`,
`core/src/tpu_entry_notifier.rs`, `turbine/src/broadcast_stage/broadcast_utils.rs`,
`turbine/src/broadcast_stage/standard_broadcast_run.rs`, plus test rewrites in
all of those. Realistic floor ≈ 800-1200 LOC plus test work.

## Concrete next-session plan

Phased so each phase ships independently and can be tested:

### Phase 1 — `SharedBytesEntry` plumbing without changing `Entry`'s on-wire layout

Add a new internal struct in `entry/src`:

```rust
// entry/src/shared_bytes_entry.rs
pub struct SharedBytesEntry {
    pub num_hashes: u64,
    pub hash: Hash,
    pub transactions: Vec<SharedTxBytes>,   // (signatures: SmallVec<[Signature; 2]>, wire: SharedBytes)
}
```

Add an extension method on `Entry`:

```rust
impl SharedBytesEntry {
    pub fn into_entry(self) -> Entry { ... }   // existing slow path
    pub fn write_wincode(&self, w: impl Writer) -> WriteResult<()> { ... }  // emits identical bytes
}
```

Plumb it through `Record::transaction_batches` as a sibling variant
(`enum RecordPayload { Materialised(Vec<Vec<VT>>), Wire(Vec<Vec<SharedTxBytes>>) }`),
flow it through `WorkingBankEntryOrMarker` as an enum, terminate it at
`Shredder::make_merkle_shreds_from_component` by extending `BlockComponent`
to a wire variant whose `SchemaWrite::write` emits the entry-batch bytes
directly without going through `VersionedTransaction`'s schema.

**Invariant test (non-negotiable, and the critical test for the project)**:
for a representative population of transactions (legacy 1-sig, legacy multi-sig,
v0 with ATL, v0 multi-ATL — **V1 is excluded; see below**), build both an
old-style `BlockComponent::EntryBatch(Vec<Entry>)` and a new-style
`BlockComponent::WireEntryBatch(Vec<SharedBytesEntry>)`, and assert
`wincode::serialize(old) == wincode::serialize(new)` byte-for-byte.

**V1 must be gated, not included.** As corrected during review, V1's
wire format places signatures at the end of the packet, while wincode's
default emission for `VersionedTransaction { signatures, message }`
places them first. Until/unless wincode learns a custom `SchemaWrite`
for `v1::Message` that mirrors the wire layout, the zero-copy path
must check `tx.version() != V1` and fall back to the materialised path
for V1 transactions. V1 is pre-mainnet, so this gate is benign.

### Phase 2 — Wire `consumer.rs` to the new path

Replace the `Vec<VersionedTransaction>` collect with a
`Vec<SharedTxBytes>` collect. The fast path becomes:

```rust
let processed_transactions: Vec<SharedTxBytes> = processing_results
    .iter()
    .zip(batch.sanitized_transactions())
    .filter_map(|(res, tx)| {
        res.was_processed().then(|| SharedTxBytes {
            signatures: tx.transaction.signatures().into(),
            wire: tx.transaction.inner_data().clone(),  // Arc<[u8]> bump
        })
    })
    .collect();
```

This drops 6 small `Vec`s per tx down to one `Arc::clone`.

### Phase 3 — Drop the `Materialised` channel variant once Phase 2 is stable

Once everything except `broadcast_duplicates_run.rs` no longer constructs the
materialised path, gate the `Materialised` variant behind a test-only feature
(or have `broadcast_duplicates_run.rs` parse-on-demand, which is fine because
that path is already off the critical perf path).

### Phase 4 — Fold `to_versioned_transaction` callers that don't need it

There are `to_versioned_transaction` calls in
`scheduler/transaction_state_container.rs:507/524/543` for re-packetisation
on retry. Those are not on the leader-execute hot path but are still 6
allocations each; switching them to `SharedTxBytes` is a smaller follow-up.

## Open questions for next session

1. **Does `SharedBytes` (the `D: TransactionData` impl actually used by
   banking) outlive the broadcast?** Resolved during review:
   `SharedBytes = Arc<Vec<u8>>` (confirmed at
   `core/src/banking_stage/transaction_scheduler/transaction_state_container.rs:251`).
   `Arc::clone` is safe and the Phase 2 collect-into-`SharedTxBytes`
   design is tractable as written. No mmap or recycled-pool surprise.

2. **Is wincode's `serialized_size(&entries)` cheaper than the current
   struct walk if we already know the per-tx wire size?** Likely yes, by a
   lot, since the wire size is just `tx.data().len()`. Coalescer at
   `broadcast_utils.rs:148` will see a noticeable win independent of the
   record-path savings.

3. **Does anything in the snapshot/replay path read `Entry::transactions`
   from a leader's locally cached entries?** Quick search shows no — the
   leader's tick cache and working-bank channel are write-only on the leader
   side; replay reads `Entry` after deserialising shreds, which is the same
   shape regardless. Worth a confirmation pass before Phase 1.

4. **Cost-model and QoS**: `qos_service.rs` consumes
   `&[impl TransactionWithMeta]` which is the `RuntimeTransaction<ResolvedTransactionView>`,
   not the materialised `VersionedTransaction`. Independent of this change.

## Pre-existing issue noticed during this study

See the corrected diagnosis under "Pre-existing test breakage in
`runtime-transaction`" above. Summary: `runtime-transaction/Cargo.toml:28`
declares `solana-hash` only in `[dependencies]`, not `[dev-dependencies]`,
without the `atomic` feature. As a result every test in the crate that
uses `Hash::new_unique()` fails to compile under
`cargo test -p solana-runtime-transaction --lib`. The tests pass in CI
because some other workspace member unifies the feature on, but isolated
crate testing is broken. One-line fix
(add `solana-hash = { workspace = true, features = ["atomic"] }` to
`[dev-dependencies]`); outside the scope of Project E.

## Path-mapping nits caught in review

- The `record_sender.try_send(Record { … })` call described in the
  execution-path table actually lives in `poh/src/poh_recorder.rs`
  (the `record()` method), not in `transaction_recorder.rs`.
  `transaction_recorder.rs:52` is the entry point that calls
  `self.record(...)`, which in turn forwards to the channel from inside
  `poh_recorder`. Don't go looking for `record_sender.try_send` in
  `transaction_recorder.rs`.
- `Shredder::make_merkle_shreds_from_component`'s `wincode::serialize(component)`
  call is at `ledger/src/shredder.rs:79` (the `:67` reference earlier in
  this document points at the function's `#[allow]` attribute line).
