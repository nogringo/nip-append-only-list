/// Local-first Dart implementation of the Nostr append-only lists NIP.
///
/// The NIP defines two regular event kinds - `kind:1990` (Add) and
/// `kind:1991` (Remove) - that complement NIP-51 by storing list membership
/// as an OR-Set CRDT instead of last-write-wins replaceable events. State
/// is computed client-side; concurrent edits from multiple devices converge
/// without coordination.
///
/// This library provides:
///
/// * A pure CRDT core ([AppendOnlyListState], [AppendOnlyListEntry],
///   [AppendOnlyListEvent]) for parsing and folding events independently
///   of NDK.
/// * Filter helpers ([listFilter], [deletionFilter]) for relay queries.
/// * A persistent cleartext projection store ([ProjectionStore]) backed by
///   sembast - keeps previously-decrypted private entries readable across
///   restarts even before a signer is connected.
/// * [AppendOnlyLists], a high-level usecase wiring NDK's cache + requests,
///   the projection, and an injected `OfflineBroadcast` queue for durable
///   write delivery.
library;

export 'src/append_only_lists.dart';
export 'src/entry.dart';
export 'src/event_codec.dart';
export 'src/filters.dart';
export 'src/kinds.dart';
export 'src/projection_store.dart';
export 'src/state.dart';
