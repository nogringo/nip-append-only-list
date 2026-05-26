import 'package:ndk/ndk.dart';

import 'entry.dart';
import 'event_codec.dart';
import 'kinds.dart';

/// Per-entry OR-Set timestamps.
///
/// An entry is a member of the resolved set iff [lastAddAt] is non-null and
/// no Remove arrived strictly later, i.e. `lastAddAt >= (lastRemoveAt ?? -∞)`.
class EntryStat {
  /// Highest `created_at` across Add events for this entry, or `null` if
  /// only Removes have been observed.
  final int? lastAddAt;

  /// Highest `created_at` across Remove events for this entry, or `null` if
  /// only Adds have been observed.
  final int? lastRemoveAt;

  /// Whether the **most recent Add** carried this entry as private. Tracks
  /// the latest publication style so consolidation re-emits with the right
  /// visibility. Ignored when the entry is currently removed.
  final bool privateOnLastAdd;

  const EntryStat({
    this.lastAddAt,
    this.lastRemoveAt,
    this.privateOnLastAdd = false,
  });

  /// True iff the entry is currently a member of the list.
  bool get isPresent =>
      lastAddAt != null &&
      (lastRemoveAt == null || lastAddAt! >= lastRemoveAt!);

  EntryStat applyAdd(int createdAt, {required bool private}) {
    final newer = lastAddAt == null || createdAt > lastAddAt!;
    return EntryStat(
      lastAddAt: newer ? createdAt : lastAddAt,
      lastRemoveAt: lastRemoveAt,
      privateOnLastAdd: newer ? private : privateOnLastAdd,
    );
  }

  EntryStat applyRemove(int createdAt) => EntryStat(
    lastAddAt: lastAddAt,
    lastRemoveAt: (lastRemoveAt == null || createdAt > lastRemoveAt!)
        ? createdAt
        : lastRemoveAt,
    privateOnLastAdd: privateOnLastAdd,
  );
}

/// Resolved state of an append-only list for a single (author, listName).
class AppendOnlyListState {
  final String listName;
  final String pubkey;

  /// Per-entry CRDT bookkeeping. Includes entries currently absent (Removes
  /// have arrived) so the OR-Set converges under further concurrent ops.
  final Map<AppendOnlyListEntry, EntryStat> stats;

  /// Event ids of append-only events whose NIP-44 content has not been
  /// decrypted yet - typically because no capable signer was available at
  /// ingestion time. `AppendOnlyLists.decryptPending` consumes this set:
  /// each event is reparsed with the signer, its private entries are
  /// folded in, and the id is removed on success.
  final Set<String> pendingDecryptionEventIds;

  const AppendOnlyListState({
    required this.listName,
    required this.pubkey,
    required this.stats,
    required this.pendingDecryptionEventIds,
  });

  factory AppendOnlyListState.empty({
    required String listName,
    required String pubkey,
  }) => AppendOnlyListState(
    listName: listName,
    pubkey: pubkey,
    stats: const {},
    pendingDecryptionEventIds: const {},
  );

  /// Currently-present entries, with their last-known privacy flag.
  Set<AppendOnlyListEntry> get entries {
    final out = <AppendOnlyListEntry>{};
    stats.forEach((entry, stat) {
      if (stat.isPresent) {
        out.add(entry.copyWith(private: stat.privateOnLastAdd));
      }
    });
    return out;
  }

  /// Pure CRDT fold over already-parsed [AppendOnlyListEvent]s.
  ///
  /// All events must share the same `listName` and `pubkey`; mismatching
  /// events are ignored.
  static AppendOnlyListState foldEvents(
    Iterable<AppendOnlyListEvent> events, {
    required String listName,
    required String pubkey,
  }) {
    final stats = <AppendOnlyListEntry, EntryStat>{};
    final pending = <String>{};

    for (final e in events) {
      if (e.listName != listName || e.pubkey != pubkey) continue;
      // Track undecrypted events by id (symmetric: add if encrypted with
      // no private entries decoded, remove if decoded successfully later).
      if (e.hasEncryptedContent && !e.entries.any((x) => x.private)) {
        pending.add(e.eventId);
      } else {
        pending.remove(e.eventId);
      }
      for (final entry in e.entries) {
        final key = entry.copyWith(private: false); // identity ignores private
        final cur = stats[key] ?? const EntryStat();
        stats[key] = e.op == AppendOnlyListOp.add
            ? cur.applyAdd(e.createdAt, private: entry.private)
            : cur.applyRemove(e.createdAt);
      }
    }

    return AppendOnlyListState(
      listName: listName,
      pubkey: pubkey,
      stats: stats,
      pendingDecryptionEventIds: pending,
    );
  }

  /// Convenience: parse + fold a list of raw nostr events.
  ///
  /// Events that aren't append-only kinds, or that target a different
  /// `(listName, pubkey)`, are dropped silently.
  ///
  /// [plaintextById] maps event ids to pre-decrypted NIP-44 plaintexts.
  /// Any event missing from the map keeps its private content opaque; its
  /// id ends up in `pendingDecryptionEventIds` so the consumer can retry
  /// decryption later (`AppendOnlyLists.decryptPending`).
  static AppendOnlyListState fromEvents(
    Iterable<Nip01Event> events, {
    required String listName,
    required String pubkey,
    Map<String, String>? plaintextById,
  }) {
    final parsed = <AppendOnlyListEvent>[];
    for (final ev in events) {
      final p = AppendOnlyListEvent.parse(ev, plaintext: plaintextById?[ev.id]);
      if (p != null) parsed.add(p);
    }
    return foldEvents(parsed, listName: listName, pubkey: pubkey);
  }
}
