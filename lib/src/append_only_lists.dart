import 'dart:async';
import 'dart:math' as math;

import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';

import 'entry.dart';
import 'event_codec.dart';
import 'filters.dart';
import 'kinds.dart';
import 'projection_store.dart';
import 'state.dart';

/// Local-first usecase for NIP append-only lists (kinds 1990/1991).
///
/// Persistence wiring is owned by the caller:
///   * [ndk] - NDK instance whose `CacheManager` keeps raw events.
///   * [outbox] - `broadcast_queue_shim_for_ndk` queue for durable delivery.
///   * [projection] - sembast-backed cleartext store, mirrored from the
///     events for offline reads (incl. previously-decrypted private entries).
///
/// Reads work without a signer (private entries are read from the
/// projection if they were decrypted in a previous session). Writes always
/// need a signer.
class AppendOnlyLists {
  AppendOnlyLists({
    required Ndk ndk,
    required this.outbox,
    required this.projection,
    this.maxEventBytes = 32 * 1024,
  }) : _ndk = ndk,
       _cache = ndk.config.cache;

  /// Durable outgoing-event queue. Public so callers can inspect pending
  /// broadcasts, trigger manual retries, etc.
  final OfflineBroadcast outbox;

  /// Cleartext projection of computed list state. Public so callers can
  /// read or evict entries directly (useful for tests and for app-level
  /// cache management).
  final ProjectionStore projection;

  /// Soft byte cap used when [consolidate] batches the fresh Add(s) and
  /// the NIP-09 deletion(s) into multiple events. Pick a value that fits
  /// under your relays' per-event size limit (many public relays cap
  /// around 64-128 KB; the 32 KB default is conservative enough to be
  /// portable). The estimator is approximate, so a small safety margin
  /// is already baked in.
  final int maxEventBytes;

  final Ndk _ndk;
  final CacheManager _cache;

  /// Active stream controllers, keyed by `"$pubkey|$listName"`. Allows local
  /// writes to push deltas into existing `watchList` subscribers.
  final Map<String, StreamController<AppendOnlyListState>> _controllers = {};

  String _key(String pubkey, String listName) => '$pubkey|$listName';

  /// Returns [explicit] if non-null, otherwise the signer of the currently
  /// logged-in NDK account.
  EventSigner? _resolveSigner(EventSigner? explicit) =>
      explicit ?? _ndk.accounts.getLoggedAccount()?.signer;

  /// Same as [_resolveSigner] but throws when no signer can be resolved -
  /// used by write methods where signing is mandatory.
  EventSigner _requireSigner(EventSigner? explicit) {
    final s = _resolveSigner(explicit);
    if (s == null) {
      throw StateError(
        'No signer available: pass `signer:` explicitly, or log an account '
        'into the injected Ndk instance first.',
      );
    }
    return s;
  }

  /// Returns [explicit] if non-empty, otherwise resolves the author's NIP-65
  /// write relays via NDK. Throws when neither is available - the outbox
  /// shim requires a non-empty target list per event.
  Future<List<String>> _resolveRelays(
    List<String>? explicit,
    String pubkey,
  ) async {
    final found = await _resolveRelaysOrEmpty(explicit, pubkey);
    if (found.isEmpty) {
      throw StateError(
        'No relays available: pass `relays:` explicitly, or publish a NIP-65 '
        'relay list (kind 10002) for $pubkey first.',
      );
    }
    return found;
  }

  /// Resolves the NIP-44 plaintext for [event], using the persistent
  /// decryption cache first and the [signer] only as a last resort.
  ///
  /// Returns `null` when the event has no encrypted content, or when both
  /// the cache miss and the signer attempt fail. On a successful signer
  /// decryption, the plaintext is written to the cache so subsequent
  /// re-folds (even without a signer) can recover it.
  Future<String?> _resolvePlaintext(
    Nip01Event event,
    EventSigner? signer,
  ) async {
    if (event.content.isEmpty) return null;
    final cached = await projection.loadDecryptedPlaintext(event.id);
    if (cached != null) return cached;
    if (signer == null || !signer.canSign()) return null;
    try {
      final plaintext = await signer.decryptNip44(
        ciphertext: event.content,
        senderPubKey: signer.getPublicKey(),
      );
      if (plaintext != null && plaintext.isNotEmpty) {
        await projection.saveDecryptedPlaintext(
          eventId: event.id,
          plaintext: plaintext,
        );
        return plaintext;
      }
    } catch (_) {
      // Decryption failed (wrong key, malformed ciphertext, …). The event
      // stays in pendingDecryptionEventIds for a later retry.
    }
    return null;
  }

  /// Like [_resolveRelays] but returns an empty list instead of throwing -
  /// used by read methods, where missing relays just degrade to "projection
  /// only" rather than blocking the call.
  Future<List<String>> _resolveRelaysOrEmpty(
    List<String>? explicit,
    String pubkey,
  ) async {
    if (explicit != null && explicit.isNotEmpty) return explicit;
    final userRelayList = await _ndk.userRelayLists.getSingleUserRelayList(
      pubkey,
    );
    return userRelayList?.writeUrls.toList(growable: false) ?? const <String>[];
  }

  // ---------------------------------------------------------------- Reading

  /// Returns the current list state from the projection plus the raw event
  /// cache. Falls back to a remote query when the local view is empty or
  /// [forceRefresh] is set.
  ///
  /// [signer] is optional; if provided, encrypted content is decrypted and
  /// merged into the projection on the fly.
  Future<AppendOnlyListState> getList({
    required String pubkey,
    required String listName,
    EventSigner? signer,
    bool forceRefresh = false,
    Duration timeout = const Duration(seconds: 5),
    List<String>? relays,
  }) async {
    signer = _resolveSigner(signer);
    final resolvedRelays = await _resolveRelaysOrEmpty(relays, pubkey);
    var state = await projection.load(pubkey: pubkey, listName: listName);

    if (state.stats.isEmpty || forceRefresh) {
      state = await _replayCache(
        pubkey: pubkey,
        listName: listName,
        signer: signer,
      );
    }

    if (forceRefresh || state.stats.isEmpty) {
      state = await _syncFromRelays(
        pubkey: pubkey,
        listName: listName,
        signer: signer,
        timeout: timeout,
        explicitRelays: resolvedRelays,
      );
    } else {
      // Best-effort incremental sync in the background; don't block the
      // caller. Errors are swallowed - relay unreachability is expected.
      unawaited(
        _syncFromRelays(
          pubkey: pubkey,
          listName: listName,
          signer: signer,
          timeout: timeout,
          explicitRelays: resolvedRelays,
        ).then((_) {}, onError: (_) {}),
      );
    }

    return state;
  }

  /// Streams state updates as new events arrive.
  ///
  /// The stream emits an initial snapshot immediately, then re-emits on
  /// every relevant Add/Remove (local or remote). Cancel the subscription
  /// to stop the underlying NDK subscription.
  Stream<AppendOnlyListState> watchList({
    required String pubkey,
    required String listName,
    EventSigner? signer,
    List<String>? relays,
  }) {
    signer = _resolveSigner(signer);
    final key = _key(pubkey, listName);
    final existing = _controllers[key];
    if (existing != null && !existing.isClosed) {
      // Re-emit the latest known state immediately on a fresh listener.
      _emitInitial(existing, pubkey: pubkey, listName: listName);
      return existing.stream;
    }

    late final StreamController<AppendOnlyListState> controller;
    NdkResponse? sub;

    controller = StreamController<AppendOnlyListState>.broadcast(
      onListen: () async {
        // 1. Local snapshot from the projection / cache.
        await _emitInitial(
          controller,
          pubkey: pubkey,
          listName: listName,
          signer: signer,
        );
        // 2. Resolve relays (explicit > NIP-65 > skip network).
        final resolvedRelays = await _resolveRelaysOrEmpty(relays, pubkey);
        if (resolvedRelays.isEmpty) return;
        // 3. Paginated backfill - fills any historical gap the relay would
        //    otherwise truncate. `_syncFromRelays` uses `paginate: true`
        //    and `fetchedRanges` to avoid redundant work.
        try {
          await _syncFromRelays(
            pubkey: pubkey,
            listName: listName,
            signer: signer,
            timeout: const Duration(seconds: 10),
            explicitRelays: resolvedRelays,
          );
        } catch (_) {
          // Don't tear the stream down on transient relay errors.
        }
        // 4. Open a live subscription for new events arriving *after* now.
        //    A subscription doesn't paginate; pagination is handled by the
        //    backfill above, and the subscription only carries the tail.
        final now = DateTime.now().millisecondsSinceEpoch ~/ 1000;
        sub = _ndk.requests.subscription(
          filter: listFilter(pubkey: pubkey, listName: listName, since: now),
          explicitRelays: resolvedRelays,
          cacheWrite: true,
        );
        sub!.stream.listen((event) async {
          await _ingestRemoteEvent(
            event,
            pubkey: pubkey,
            listName: listName,
            signer: signer,
          );
        }, onError: (_) {});
      },
      onCancel: () async {
        if (sub != null) {
          await _ndk.requests.closeSubscription(sub!.requestId);
        }
        _controllers.remove(key);
      },
    );
    _controllers[key] = controller;
    return controller.stream;
  }

  // ---------------------------------------------------------------- Writing

  /// Publishes a kind 1990 Add event covering [entries] on list [listName].
  ///
  /// [signer] is optional - when omitted, the signer of the currently
  /// logged-in NDK account is used. Throws if neither is available.
  ///
  /// [relays] is optional - when null or empty, the author's NIP-65 write
  /// relays (resolved via NDK) are used. Throws if no NIP-65 list can be
  /// found for the author.
  ///
  /// The event is signed, persisted to the NDK cache and the projection
  /// before being enqueued for delivery, so [getList] / [watchList]
  /// observers see the new state immediately even when offline.
  Future<QueuedBroadcast> add({
    required String listName,
    required List<AppendOnlyListEntry> entries,
    List<String>? relays,
    EventSigner? signer,
  }) async {
    final eventSigner = _requireSigner(signer);
    return _emit(
      signer: eventSigner,
      op: AppendOnlyListOp.add,
      listName: listName,
      entries: entries,
      relays: await _resolveRelays(relays, eventSigner.getPublicKey()),
    );
  }

  /// Publishes a kind 1991 Remove event covering [entries] on list [listName].
  ///
  /// [signer] and [relays] are optional - see [add] for details.
  Future<QueuedBroadcast> remove({
    required String listName,
    required List<AppendOnlyListEntry> entries,
    List<String>? relays,
    EventSigner? signer,
  }) async {
    final eventSigner = _requireSigner(signer);
    return _emit(
      signer: eventSigner,
      op: AppendOnlyListOp.remove,
      listName: listName,
      entries: entries,
      relays: await _resolveRelays(relays, eventSigner.getPublicKey()),
    );
  }

  /// Retries NIP-44 decryption on every event still listed in
  /// [AppendOnlyListState.pendingDecryptionEventIds] for `(pubkey, listName)`.
  ///
  /// Typical use: call this when a signer becomes available (after login,
  /// app resume, etc.) to surface private entries that were ingested while
  /// the signer was offline.
  ///
  /// Returns the updated state. Events whose raw form is no longer in the
  /// NDK cache (e.g. purged by consolidation) are silently dropped from
  /// the pending set. Events that still fail to decrypt stay pending.
  Future<AppendOnlyListState> decryptPending({
    required String pubkey,
    required String listName,
    EventSigner? signer,
  }) async {
    final eventSigner = _requireSigner(signer);
    final current = await projection.load(pubkey: pubkey, listName: listName);
    if (current.pendingDecryptionEventIds.isEmpty) return current;

    // Bulk-load every pending raw event in one query, then resolve all of
    // them in parallel. NDK's signer implementations bound their own
    // concurrency (see relaystr/ndk#632), so the parallel decrypts stay
    // safe even on a remote bunker.
    final pendingIds = current.pendingDecryptionEventIds.toList(
      growable: false,
    );
    final pendingEvents = await _cache.loadEvents(ids: pendingIds);
    final eventById = {for (final ev in pendingEvents) ev.id: ev};

    final reparsed = <AppendOnlyListEvent>[];
    final stillPending = <String>{};
    final orphans = <String>{};

    await Future.wait(
      pendingIds.map((id) async {
        final raw = eventById[id];
        if (raw == null) {
          orphans.add(id);
          return;
        }
        final plaintext = await _resolvePlaintext(raw, eventSigner);
        final parsed = AppendOnlyListEvent.parse(raw, plaintext: plaintext);
        if (parsed == null) {
          orphans.add(id);
          return;
        }
        if (parsed.entries.any((e) => e.private)) {
          reparsed.add(parsed);
        } else {
          // Still couldn't decode - likely wrong signer for this content.
          stillPending.add(id);
        }
      }),
    );

    return projection.update(
      pubkey: pubkey,
      listName: listName,
      mutator: (state) {
        var next = state;
        for (final ev in reparsed) {
          next = _foldOne(next, ev);
        }
        // Replace the pending set with whatever the fold + retries left
        // behind: orphans dropped, successes removed by _foldOne, true
        // failures preserved.
        final survivingPending =
            Set<String>.from(next.pendingDecryptionEventIds)
              ..removeAll(orphans)
              ..addAll(stillPending);
        return AppendOnlyListState(
          listName: next.listName,
          pubkey: next.pubkey,
          stats: next.stats,
          pendingDecryptionEventIds: survivingPending,
        );
      },
    );
  }

  /// Issues a fresh Add capturing the current state of [listName] and
  /// NIP-09 deletes every superseded event for that list.
  ///
  /// [signer] and [relays] are optional - see [add] for details.
  ///
  /// Operates locally first (cache + projection rewritten) and enqueues both
  /// the new Add and the deletion through the outbox. The projection is
  /// refilled from the new Add only - the cleartext for previously-private
  /// entries is preserved because the new Add re-encrypts them.
  Future<void> consolidate({
    required String listName,
    List<String>? relays,
    EventSigner? signer,
  }) async {
    final eventSigner = _requireSigner(signer);
    final pubkey = eventSigner.getPublicKey();
    final resolvedRelays = await _resolveRelays(relays, pubkey);
    final state = await projection.load(pubkey: pubkey, listName: listName);
    final present = state.entries.toList();

    final supersededIds = await _superseededEventIds(
      pubkey: pubkey,
      listName: listName,
    );

    // 1. Emit fresh Add(s) carrying every currently-present entry,
    //    chunked so each event stays under [maxEventBytes]. OR-Set
    //    semantics make multi-Add splitting safe: the final state is the
    //    same regardless of how the entries are partitioned across events.
    if (present.isNotEmpty) {
      for (final chunk in _packAddBatches(present, listName)) {
        await _emit(
          signer: eventSigner,
          op: AppendOnlyListOp.add,
          listName: listName,
          entries: chunk,
          relays: resolvedRelays,
        );
      }
    }

    // 2. Delete the superseded events (NIP-09), also batched by byte cap.
    if (supersededIds.isNotEmpty) {
      for (final batch in _packDeletionBatches(supersededIds)) {
        final deletion = Nip01Event(
          pubKey: pubkey,
          kind: 5,
          tags: <List<String>>[
            for (final id in batch) <String>['e', id],
            <String>['k', '$kindAdd'],
            <String>['k', '$kindRemove'],
          ],
          content: 'append-only-list consolidation',
        );
        final signedDeletion = await eventSigner.sign(deletion);
        await _cache.saveEvent(signedDeletion);
        await outbox.broadcast(signedDeletion, relays: resolvedRelays);
      }
      // Trim local cache once every deletion has been queued for delivery.
      for (final id in supersededIds) {
        await _cache.removeEvent(id);
      }
      await projection.deleteDecryptedPlaintext(supersededIds);
    }
  }

  /// Splits [entries] into batches such that each resulting Add event
  /// stays under [maxEventBytes]. Uses a cheap byte estimator (no actual
  /// NIP-44 encryption) so packing is O(n).
  ///
  /// If a single entry's estimated size already exceeds the cap, it is
  /// emitted alone - the relay may still reject it, but the alternative
  /// (dropping it silently) is worse.
  List<List<AppendOnlyListEntry>> _packAddBatches(
    List<AppendOnlyListEntry> entries,
    String listName,
  ) {
    if (entries.isEmpty) return const [];
    final batches = <List<AppendOnlyListEntry>>[];
    var current = <AppendOnlyListEntry>[];
    for (final e in entries) {
      final candidate = [...current, e];
      if (_estimateAddBytes(candidate, listName) <= maxEventBytes ||
          current.isEmpty) {
        current = candidate;
      } else {
        batches.add(current);
        current = [e];
      }
    }
    if (current.isNotEmpty) batches.add(current);
    return batches;
  }

  /// Splits [ids] into batches such that each resulting kind 5 deletion
  /// event stays under [maxEventBytes]. Event ids are constant-size hex
  /// strings, so this is a fixed-arithmetic chunk.
  List<List<String>> _packDeletionBatches(Iterable<String> ids) {
    // Per `e` tag in JSON: `,["e","<64hex>"]` ≈ 72 bytes.
    // Baseline (id, pubkey, sig, kind, content, two k tags, wrapping): ≈ 320.
    const baseline = 320;
    const perETag = 72;
    final budget = maxEventBytes - baseline;
    final perBatch = budget ~/ perETag;
    if (perBatch <= 0) {
      // maxEventBytes is absurdly small; degrade to one e tag per event.
      return ids.map((id) => <String>[id]).toList(growable: false);
    }
    final all = ids.toList(growable: false);
    final batches = <List<String>>[];
    for (var i = 0; i < all.length; i += perBatch) {
      batches.add(all.sublist(i, math.min(i + perBatch, all.length)));
    }
    return batches;
  }

  /// Cheap byte estimator for an Add event carrying [entries] under the
  /// given [listName]. Does **not** perform NIP-44 encryption: it
  /// estimates the ciphertext size as `ceil(plaintext * 4/3) + 200` to
  /// cover NIP-44 v2's padding + nonce + mac + base64 overhead with a
  /// safety margin.
  int _estimateAddBytes(List<AppendOnlyListEntry> entries, String listName) {
    // Baseline: id(64) + pubkey(64) + sig(128) + created_at + kind +
    // JSON wrapping and quoting overhead. Round up generously.
    const baseline = 280;
    // d tag JSON: ,["d","<listName>"]
    var bytes = baseline + 9 + listName.length;

    var plaintextBytes = 0;
    var privateCount = 0;
    for (final e in entries) {
      if (e.private) {
        // [tag, value] JSON inside the content array.
        plaintextBytes += 8 + e.tag.length + e.value.length;
        privateCount++;
      } else {
        // Public tag JSON: ,["<tag>","<value>"]
        bytes += 8 + e.tag.length + e.value.length;
      }
    }
    if (privateCount > 0) {
      plaintextBytes += 2; // outer "[]"
      // NIP-44 v2 ciphertext (base64-encoded) is roughly plaintext * 4/3
      // after padding+nonce+mac. The +200 covers all the constant overhead
      // plus a generous margin.
      bytes += ((plaintextBytes * 4) / 3).ceil() + 200;
    }
    return bytes;
  }

  // ----------------------------------------------------------------- Closes

  /// Closes every active `watchList` stream. The injected [outbox] and
  /// [ndk] remain owned by the caller and are not disposed.
  Future<void> dispose() async {
    for (final c in _controllers.values) {
      await c.close();
    }
    _controllers.clear();
  }

  // ----------------------------------------------------------------- Private

  Future<QueuedBroadcast> _emit({
    required EventSigner signer,
    required AppendOnlyListOp op,
    required String listName,
    required List<AppendOnlyListEntry> entries,
    required List<String> relays,
  }) async {
    if (!signer.canSign()) {
      throw StateError('Signer cannot sign - required for append-only writes.');
    }
    final pubkey = signer.getPublicKey();
    // Force created_at to be strictly monotonic for local writes. The NIP
    // says a Remove must be *strictly* later than an Add to win, so two
    // sequential ops within the same wall-clock second would otherwise let
    // the Add stay present. The CRDT itself stays correct for concurrent
    // cross-device ops (those still resolve "Add wins on tie").
    final state = await projection.load(pubkey: pubkey, listName: listName);
    final maxKnown = _maxKnownCreatedAt(state);
    final wall = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final createdAt = wall > maxKnown ? wall : maxKnown + 1;
    final unsigned = await buildAppendOnlyEvent(
      op: op,
      listName: listName,
      entries: entries,
      pubkey: pubkey,
      signer: signer,
      createdAt: createdAt,
    );
    final signed = await signer.sign(unsigned);

    await _cache.saveEvent(signed);
    // Resolve plaintext via the cache/signer pipeline so the freshly-emitted
    // private content also lands in the persistent decryption store - any
    // future re-fold of this event will recover it without a signer.
    final plaintext = await _resolvePlaintext(signed, signer);
    final parsed = AppendOnlyListEvent.parse(signed, plaintext: plaintext);
    if (parsed != null) {
      await _applyToProjection(parsed);
    }
    return outbox.broadcast(signed, relays: relays);
  }

  Future<AppendOnlyListState> _replayCache({
    required String pubkey,
    required String listName,
    EventSigner? signer,
  }) async {
    final events = await _cache.loadEvents(
      pubKeys: [pubkey],
      kinds: appendOnlyKinds,
    );
    final tombstones = await projection.loadTombstones(
      pubkey: pubkey,
      listName: listName,
    );
    final filtered = events
        .where((e) => e.getDtag() == listName && !tombstones.contains(e.id))
        .toList(growable: false);

    // Step 1: batch-fetch every plaintext already known for these events
    // in a single sembast query. This is the common path: events seen in
    // previous sessions had their NIP-44 content cached at ingestion time,
    // so this single read resolves them all without ever touching the
    // signer.
    final encryptedIds = filtered
        .where((e) => e.content.isNotEmpty)
        .map((e) => e.id)
        .toList(growable: false);
    final plaintextById = Map<String, String>.from(
      await projection.loadDecryptedPlaintexts(encryptedIds),
    );

    // Step 2: for events the cache didn't cover, fall back to live
    // decryption with the signer. Only runs when the decryption cache is
    // partial (first-time ingest, cache wiped, etc.), so it's rare in
    // steady state.
    //
    // We fire all decryptions in parallel: NDK's signer implementations
    // (NIP-46 bunker, NIP-07, Amber) bound their own concurrency since
    // relaystr/ndk#632, so a `Future.wait` over hundreds of events stays
    // safe even on remote signers.
    if (signer != null && signer.canSign()) {
      final toResolve = filtered.where(
        (e) => e.content.isNotEmpty && !plaintextById.containsKey(e.id),
      );
      final pubkeyForDecrypt = signer.getPublicKey();
      await Future.wait(
        toResolve.map((ev) async {
          try {
            final pt = await signer.decryptNip44(
              ciphertext: ev.content,
              senderPubKey: pubkeyForDecrypt,
            );
            if (pt != null && pt.isNotEmpty) {
              plaintextById[ev.id] = pt;
              await projection.saveDecryptedPlaintext(
                eventId: ev.id,
                plaintext: pt,
              );
            }
          } catch (_) {
            // Decryption failed; the event stays in the pending set so a
            // later signer attempt can pick it up.
          }
        }),
      );
    }

    final state = AppendOnlyListState.fromEvents(
      filtered,
      pubkey: pubkey,
      listName: listName,
      plaintextById: plaintextById,
    );
    await projection.save(state);
    return state;
  }

  /// Remote sync with per-relay gap detection.
  ///
  /// Uses `ndk.fetchedRanges` to compute the time gaps each relay still
  /// owes us and issues one query per gap. After a successful query, the
  /// covered range is recorded so subsequent syncs skip work that has
  /// already been done - even across restarts (provided the configured
  /// `CacheManager` persists fetched-range records, as `SembastCacheManager`
  /// does).
  ///
  /// When [explicitRelays] is null or empty, the function returns
  /// immediately without touching the network - the caller is expected to
  /// have either passed a relay list explicitly or resolved one via NIP-65
  /// upstream. Reads silently degrade to "projection only" in this case.
  Future<AppendOnlyListState> _syncFromRelays({
    required String pubkey,
    required String listName,
    EventSigner? signer,
    required Duration timeout,
    Iterable<String>? explicitRelays,
  }) async {
    if (explicitRelays == null || explicitRelays.isEmpty) {
      return projection.load(pubkey: pubkey, listName: listName);
    }
    // Honor NIP-09 deletions first. If any apply, the affected raw events
    // are dropped from the NDK cache and the projection is rebuilt from
    // scratch, so the rest of the sync runs against a clean state.
    final hadDeletions = await _syncDeletions(
      pubkey: pubkey,
      listName: listName,
      timeout: timeout,
      explicitRelays: explicitRelays,
    );
    if (hadDeletions) {
      await _replayCache(pubkey: pubkey, listName: listName, signer: signer);
    }
    final baseFilter = listFilter(pubkey: pubkey, listName: listName);
    final until = DateTime.now().millisecondsSinceEpoch ~/ 1000;

    // ignore: experimental_member_use
    final perRelay = await _ndk.fetchedRanges.getOptimizedFilters(
      filter: baseFilter,
      since: 0,
      until: until,
      relayUrls: explicitRelays.toList(growable: false),
    );

    for (final entry in perRelay.entries) {
      final relayUrl = entry.key;
      for (final gapFilter in entry.value) {
        // Drop the gap's `until` before querying the relay. Reason: local
        // writes can use a `created_at` slightly in the future of the
        // current wall clock (the monotonic bump in `_emit` makes sequential
        // ops in the same second strictly orderable). A strict `until: now`
        // on the filter would let the relay drop those events on the floor.
        // We still record [since, wall_now] as covered in `fetchedRanges`,
        // so anything past wall_now is just re-fetched at the next sync.
        final queryFilter = gapFilter.clone()..until = null;
        final response = _ndk.requests.query(
          filter: queryFilter,
          timeout: timeout,
          explicitRelays: [relayUrl],
          // Paginate within each gap so we are not silently truncated by
          // the relay's per-filter cap.
          paginate: true,
        );
        var ok = true;
        try {
          await for (final event in response.stream) {
            await _ingestRemoteEvent(
              event,
              pubkey: pubkey,
              listName: listName,
              signer: signer,
            );
          }
        } catch (_) {
          ok = false;
        }
        if (ok) {
          // ignore: experimental_member_use
          await _ndk.fetchedRanges.addRange(
            filter: baseFilter,
            relayUrl: relayUrl,
            since: gapFilter.since ?? 0,
            until: gapFilter.until ?? until,
          );
        }
      }
    }

    return projection.load(pubkey: pubkey, listName: listName);
  }

  /// Pulls NIP-09 deletion events (kind 5 with `#k:["1990","1991"]`) for
  /// [pubkey], intersects them with cached 1990/1991 events belonging to
  /// [listName], persists the affected ids as tombstones and removes the
  /// events from the NDK cache.
  ///
  /// Returns `true` if at least one cached event was tombstoned, which
  /// signals the caller to re-fold the projection from the trimmed cache.
  Future<bool> _syncDeletions({
    required String pubkey,
    required String listName,
    required Duration timeout,
    required Iterable<String> explicitRelays,
  }) async {
    final base = deletionFilter(pubkey: pubkey);
    final until = DateTime.now().millisecondsSinceEpoch ~/ 1000;

    // ignore: experimental_member_use
    final perRelay = await _ndk.fetchedRanges.getOptimizedFilters(
      filter: base,
      since: 0,
      until: until,
      relayUrls: explicitRelays.toList(growable: false),
    );

    final deletions = <Nip01Event>[];
    for (final entry in perRelay.entries) {
      final relayUrl = entry.key;
      for (final gapFilter in entry.value) {
        // Same `until = null` trick as `_syncFromRelays`: don't let the
        // relay drop future-dated events on the floor.
        final queryFilter = gapFilter.clone()..until = null;
        final response = _ndk.requests.query(
          filter: queryFilter,
          timeout: timeout,
          explicitRelays: [relayUrl],
          paginate: true,
        );
        var ok = true;
        try {
          await for (final event in response.stream) {
            if (event.kind == 5 && event.pubKey == pubkey) {
              deletions.add(event);
            }
          }
        } catch (_) {
          ok = false;
        }
        if (ok) {
          // ignore: experimental_member_use
          await _ndk.fetchedRanges.addRange(
            filter: base,
            relayUrl: relayUrl,
            since: gapFilter.since ?? 0,
            until: gapFilter.until ?? until,
          );
        }
      }
    }

    if (deletions.isEmpty) return false;

    // Collect every targeted event id from every deletion. The `#k`
    // filter already restricted us to deletions whose `k` tag is 1990 or
    // 1991, so every collected id is, by author's own claim, a deletion
    // of an append-only event.
    final targets = <String>{};
    for (final del in deletions) {
      for (final tag in del.tags) {
        if (tag.length >= 2 && tag[0] == 'e') targets.add(tag[1]);
      }
    }
    if (targets.isEmpty) return false;

    // Persist every referenced id as a tombstone, *without* intersecting
    // with the local cache. Two reasons:
    //   1. A fresh device may receive the deletion before the deleted
    //      events themselves; tombstoning unconditionally lets us drop
    //      the revenants at ingestion (see `_ingestRemoteEvent`).
    //   2. The deletion query uses `fetchedRanges`, so we only see each
    //      deletion once - we can't rely on "retry next sync".
    // Ids targeting other lists of the same author are harmless dead
    // weight: those events would have been dropped by the d-tag check
    // in `_ingestRemoteEvent` anyway.
    await projection.addTombstones(
      pubkey: pubkey,
      listName: listName,
      ids: targets,
    );

    // Cache cleanup: only events we actually have, narrowed to our list.
    final cached = await _cache.loadEvents(
      pubKeys: [pubkey],
      kinds: appendOnlyKinds,
      ids: targets.toList(growable: false),
    );
    final toRemove = cached
        .where((e) => e.getDtag() == listName)
        .map((e) => e.id)
        .toList(growable: false);
    for (final id in toRemove) {
      await _cache.removeEvent(id);
    }
    // The raw events are gone - drop their cached plaintext too. Keeping
    // them would just be dead cleartext on disk.
    await projection.deleteDecryptedPlaintext(toRemove);
    // Re-fold only needed when something was actually evicted from the
    // cache - otherwise the projection didn't see those events anyway.
    return toRemove.isNotEmpty;
  }

  Future<void> _ingestRemoteEvent(
    Nip01Event event, {
    required String pubkey,
    required String listName,
    EventSigner? signer,
  }) async {
    if (event.pubKey != pubkey) return;
    if (!appendOnlyKinds.contains(event.kind)) return;
    if (event.getDtag() != listName) return;
    // Drop revenants: if this event id was previously NIP-09-deleted, we
    // ignore it even when a relay redelivers it (NIP-09 honoring is
    // best-effort on the relay side).
    final tombstones = await projection.loadTombstones(
      pubkey: pubkey,
      listName: listName,
    );
    if (tombstones.contains(event.id)) return;
    final plaintext = await _resolvePlaintext(event, signer);
    final parsed = AppendOnlyListEvent.parse(event, plaintext: plaintext);
    if (parsed == null) return;
    await _applyToProjection(parsed);
  }

  Future<void> _applyToProjection(AppendOnlyListEvent event) async {
    final next = await projection.update(
      pubkey: event.pubkey,
      listName: event.listName,
      mutator: (current) => _foldOne(current, event),
    );
    final controller = _controllers[_key(event.pubkey, event.listName)];
    if (controller != null && !controller.isClosed) {
      controller.add(next);
    }
  }

  AppendOnlyListState _foldOne(
    AppendOnlyListState current,
    AppendOnlyListEvent event,
  ) {
    final stats = Map<AppendOnlyListEntry, EntryStat>.from(current.stats);
    for (final entry in event.entries) {
      final key = entry.copyWith(private: false);
      final prev = stats[key] ?? const EntryStat();
      stats[key] = event.op == AppendOnlyListOp.add
          ? prev.applyAdd(event.createdAt, private: entry.private)
          : prev.applyRemove(event.createdAt);
    }
    final pending = Set<String>.from(current.pendingDecryptionEventIds);
    if (event.hasEncryptedContent && !event.entries.any((e) => e.private)) {
      pending.add(event.eventId);
    } else {
      pending.remove(event.eventId);
    }
    return AppendOnlyListState(
      listName: current.listName,
      pubkey: current.pubkey,
      stats: stats,
      pendingDecryptionEventIds: pending,
    );
  }

  /// Highest `created_at` ever folded into [state], derived on the fly
  /// from the per-entry stats. Used to keep local writes strictly
  /// monotonic across the same wall-clock second.
  int _maxKnownCreatedAt(AppendOnlyListState state) {
    var max = 0;
    for (final stat in state.stats.values) {
      final a = stat.lastAddAt ?? 0;
      final r = stat.lastRemoveAt ?? 0;
      if (a > max) max = a;
      if (r > max) max = r;
    }
    return max;
  }

  Future<void> _emitInitial(
    StreamController<AppendOnlyListState> controller, {
    required String pubkey,
    required String listName,
    EventSigner? signer,
  }) async {
    var state = await projection.load(pubkey: pubkey, listName: listName);
    if (state.stats.isEmpty) {
      state = await _replayCache(
        pubkey: pubkey,
        listName: listName,
        signer: signer,
      );
    }
    if (!controller.isClosed) controller.add(state);
  }

  Future<List<String>> _superseededEventIds({
    required String pubkey,
    required String listName,
  }) async {
    final events = await _cache.loadEvents(
      pubKeys: [pubkey],
      kinds: appendOnlyKinds,
    );
    return events
        .where((e) => e.getDtag() == listName)
        .map((e) => e.id)
        .toList(growable: false);
  }
}
