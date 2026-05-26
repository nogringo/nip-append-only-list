import 'package:sembast/sembast.dart';

import 'entry.dart';
import 'state.dart';

/// Persistent cleartext projection of computed list state.
///
/// One record per `(pubkey, listName)` is stored in a sembast database the
/// caller owns. The record carries the OR-Set bookkeeping in cleartext, so
/// previously-decrypted private entries remain visible at the next start-up
/// even before a signer is connected.
///
/// Security caveat: this means cleartext for what was originally encrypted
/// content is written to local storage. The caller is responsible for
/// device-level protection (disk encryption, app sandbox).
class ProjectionStore {
  final Database _db;
  final StoreRef<String, Map<String, Object?>> _store;
  final StoreRef<String, Map<String, Object?>> _tombstoneStore;
  final StoreRef<String, String> _plaintextStore;

  /// Builds a [ProjectionStore] backed by [db].
  ///
  /// [storeName] picks the sembast store for projected state. Two companion
  /// stores live alongside it in the same database:
  ///   * `'${storeName}_tombstones'` - event ids that have been
  ///     NIP-09-deleted, filtered out at ingestion time even if a relay
  ///     redelivers them.
  ///   * `'${storeName}_plaintext'` - cleartext NIP-44 payloads keyed by
  ///     event id, so a previously-decrypted private event can be re-folded
  ///     without the signer being present (the foundation of "cleartext
  ///     survives across sessions" promised by this package).
  ProjectionStore(this._db, {String storeName = 'append_only_list_projection'})
    : _store = stringMapStoreFactory.store(storeName),
      _tombstoneStore = stringMapStoreFactory.store('${storeName}_tombstones'),
      _plaintextStore = StoreRef<String, String>('${storeName}_plaintext');

  String _key(String pubkey, String listName) => '$pubkey|$listName';

  /// Reads the persisted state for `(pubkey, listName)`, or returns an empty
  /// state if no projection exists yet.
  Future<AppendOnlyListState> load({
    required String pubkey,
    required String listName,
  }) async {
    final record = await _store.record(_key(pubkey, listName)).get(_db);
    if (record == null) {
      return AppendOnlyListState.empty(listName: listName, pubkey: pubkey);
    }
    return _decode(record, pubkey: pubkey, listName: listName);
  }

  /// Persists [state] for `(pubkey, listName)`.
  Future<void> save(AppendOnlyListState state) async {
    await _store
        .record(_key(state.pubkey, state.listName))
        .put(_db, _encode(state));
  }

  /// Deletes the projection for `(pubkey, listName)`. Used by consolidation
  /// when older events are superseded.
  Future<void> delete({
    required String pubkey,
    required String listName,
  }) async {
    await _store.record(_key(pubkey, listName)).delete(_db);
  }

  /// Loads the set of NIP-09-tombstoned event ids for `(pubkey, listName)`.
  Future<Set<String>> loadTombstones({
    required String pubkey,
    required String listName,
  }) async {
    final rec = await _tombstoneStore.record(_key(pubkey, listName)).get(_db);
    if (rec == null) return const <String>{};
    final ids = rec['ids'];
    if (ids is! List) return const <String>{};
    return ids.whereType<String>().toSet();
  }

  /// Marks [ids] as tombstoned for `(pubkey, listName)`. The set is merged
  /// with any existing tombstones so the operation is idempotent.
  Future<void> addTombstones({
    required String pubkey,
    required String listName,
    required Set<String> ids,
  }) async {
    if (ids.isEmpty) return;
    await _db.transaction((txn) async {
      final ref = _tombstoneStore.record(_key(pubkey, listName));
      final existing = await ref.get(txn);
      final merged = <String>{};
      final prior = existing?['ids'];
      if (prior is List) {
        merged.addAll(prior.whereType<String>());
      }
      merged.addAll(ids);
      await ref.put(txn, <String, Object?>{
        'ids': merged.toList(growable: false),
      });
    });
  }

  /// Returns the previously-decrypted NIP-44 plaintext for [eventId], or
  /// `null` if the package has never decrypted that event.
  Future<String?> loadDecryptedPlaintext(String eventId) =>
      _plaintextStore.record(eventId).get(_db);

  /// Batch variant of [loadDecryptedPlaintext]: looks up every cleartext
  /// known for [eventIds] in a single sembast query and returns them as a
  /// map keyed by event id. Event ids that have no cached plaintext are
  /// simply absent from the result.
  ///
  /// Used by `_replayCache` to avoid an O(n) wave of sequential sembast
  /// reads on cold start of a large list.
  Future<Map<String, String>> loadDecryptedPlaintexts(
    Iterable<String> eventIds,
  ) async {
    final ids = eventIds.toList(growable: false);
    if (ids.isEmpty) return const <String, String>{};
    final values = await _plaintextStore.records(ids).get(_db);
    final out = <String, String>{};
    for (var i = 0; i < ids.length; i++) {
      final v = values[i];
      if (v != null) out[ids[i]] = v;
    }
    return out;
  }

  /// Persists the decrypted plaintext of [eventId]. Calling this with the
  /// same `eventId` overwrites the previous value (the cleartext is
  /// deterministic, so this is idempotent in practice).
  Future<void> saveDecryptedPlaintext({
    required String eventId,
    required String plaintext,
  }) async {
    await _plaintextStore.record(eventId).put(_db, plaintext);
  }

  /// Drops the cleartext payloads for the given [eventIds]. Used when
  /// events get NIP-09-tombstoned so we don't keep dead cleartext on disk.
  Future<void> deleteDecryptedPlaintext(Iterable<String> eventIds) async {
    if (eventIds.isEmpty) return;
    await _db.transaction((txn) async {
      for (final id in eventIds) {
        await _plaintextStore.record(id).delete(txn);
      }
    });
  }

  /// Read-modify-write helper. The mutator receives the current state and
  /// must return the new state; the whole operation runs in a sembast
  /// transaction so concurrent updates remain consistent.
  Future<AppendOnlyListState> update({
    required String pubkey,
    required String listName,
    required AppendOnlyListState Function(AppendOnlyListState current) mutator,
  }) async {
    return _db.transaction((txn) async {
      final ref = _store.record(_key(pubkey, listName));
      final record = await ref.get(txn);
      final current = record == null
          ? AppendOnlyListState.empty(listName: listName, pubkey: pubkey)
          : _decode(record, pubkey: pubkey, listName: listName);
      final next = mutator(current);
      await ref.put(txn, _encode(next));
      return next;
    });
  }

  // ---- (de)serialization ---------------------------------------------------

  Map<String, Object?> _encode(AppendOnlyListState state) {
    final entries = <Map<String, Object?>>[];
    state.stats.forEach((entry, stat) {
      entries.add(<String, Object?>{
        't': entry.tag,
        'v': entry.value,
        'a': stat.lastAddAt,
        'r': stat.lastRemoveAt,
        'p': stat.privateOnLastAdd,
      });
    });
    return <String, Object?>{
      'entries': entries,
      'pending': state.pendingDecryptionEventIds.toList(growable: false),
    };
  }

  AppendOnlyListState _decode(
    Map<String, Object?> record, {
    required String pubkey,
    required String listName,
  }) {
    final stats = <AppendOnlyListEntry, EntryStat>{};
    final entries = record['entries'];
    if (entries is List) {
      for (final raw in entries) {
        if (raw is! Map) continue;
        stats[AppendOnlyListEntry(
          tag: raw['t'].toString(),
          value: raw['v'].toString(),
        )] = EntryStat(
          lastAddAt: raw['a'] as int?,
          lastRemoveAt: raw['r'] as int?,
          privateOnLastAdd: raw['p'] as bool? ?? false,
        );
      }
    }
    final pending = <String>{};
    final pendingRaw = record['pending'];
    if (pendingRaw is List) {
      for (final id in pendingRaw) {
        if (id is String) pending.add(id);
      }
    }
    return AppendOnlyListState(
      listName: listName,
      pubkey: pubkey,
      stats: stats,
      pendingDecryptionEventIds: pending,
    );
  }
}
