## 0.3.0

- **Breaking:** require `ndk ^0.10.0` and `broadcast_queue_shim_for_ndk
  ^0.6.0`.
- **Breaking:** `add`, `remove` and `consolidate` called without `relays:` no
  longer throw when the author's NIP-65 is unknown. The event is applied
  locally and queued for `RelaySet.outbox(pubkey)`, which the outbox worker
  resolves later, offline included. An author without NIP-65 ends up with a
  `failed` entry in the queue. The outbox needs a `relayListFn`, which
  `OfflineBroadcast.withNdk` provides.

## 0.2.0

- Add `AppendOnlyLists.clearLocalAccountData({pubkey})` and
  `clearAllLocalData()` for a purely local reset: wipe the sembast projection
  (state, tombstones, cached plaintext), the pubkey's 1990/1991 events and
  matching kind 5 deletions in the NDK cache, and the fetched-range bookmarks
  for those lists so the next read re-syncs from relays. Active `watchList`
  streams are closed; the outbox is left untouched and nothing is published.
- Attribute every queued broadcast to its author (`pubkey:` on
  `outbox.broadcast`), so a caller owning a dedicated outbox can clear an
  account's pending sends on logout. Requires
  `broadcast_queue_shim_for_ndk ^0.4.0`.

## 0.1.1

- Remove subscription `name`.

## 0.1.0

Initial release.

- Pure OR-Set CRDT core: `AppendOnlyListEntry`, `EntryStat`,
  `AppendOnlyListEvent`, `AppendOnlyListState` (parse and fold kinds
  1990/1991).
- Event builder/parser with NIP-44 self-encryption for private entries.
- `Filter` helpers (`listFilter`, `deletionFilter`) for relay queries.
- Sembast-backed cleartext `ProjectionStore` for offline reads that survive
  restarts.
- `AppendOnlyLists` usecase wiring an injected `Ndk`, `OfflineBroadcast`
  queue, and `ProjectionStore`: `getList`, `watchList`, `add`, `remove`,
  `consolidate`, `decryptPending`.
- `consolidate` splits both the fresh Add(s) and the NIP-09 deletion(s)
  into chunks that fit under `maxEventBytes` (default 32 KB) so relays
  don't reject oversized events on lists with many entries or long
  history.
- NIP-09 deletions emitted by other devices of the same author are
  honored on incoming sync (tombstone set + cache trim + re-fold).
- Persistent decryption cache (third sembast store): cleartext NIP-44
  payloads are written to disk keyed by event id, so re-folding works
  without the signer once a private event has been decoded once.
