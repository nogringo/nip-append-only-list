import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/entities.dart' show ReadWriteMarker, UserRelayList;
import 'package:ndk/ndk.dart';
import 'package:nip_append_only_list/nip_append_only_list.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import 'mock_relay.dart';

void main() {
  group('AppendOnlyLists end-to-end against MockRelay', () {
    late MockRelay relay;
    late Ndk ndk;
    late OfflineBroadcast outbox;
    late AppendOnlyLists lists;
    late EventSigner signer;
    late String pubkey;

    setUp(() async {
      relay = MockRelay();
      await relay.start();

      ndk = ndkForRelay(relay);

      final outboxDb = await newDatabaseFactoryMemory().openDatabase(
        'outbox.db',
      );
      final projectionDb = await newDatabaseFactoryMemory().openDatabase(
        'projection.db',
      );

      outbox = OfflineBroadcast.withNdk(ndk, db: outboxDb);
      outbox.start();

      lists = AppendOnlyLists(
        ndk: ndk,
        outbox: outbox,
        projection: ProjectionStore(projectionDb),
      );

      signer = const Bip340EventSignerFactory().createWithNewKeyPair();
      pubkey = signer.getPublicKey();
      // Log the signer into NDK so calls can omit `signer:`.
      ndk.accounts.loginExternalSigner(signer: signer);
    });

    tearDown(() async {
      await lists.dispose();
      await outbox.dispose();
      await ndk.destroy();
      await relay.stop();
    });

    test(
      'add then remove converges to the expected set, locally and on relay',
      () async {
        final relayUrl = relay.url;

        await lists.add(
          listName: 'fruits',
          entries: const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
            AppendOnlyListEntry(tag: 't', value: 'banana'),
            AppendOnlyListEntry(tag: 't', value: 'cherry'),
          ],
          relays: [relayUrl],
        );

        // Wait for the outbox to deliver before continuing.
        await _waitForRelayCount(relay, 1);

        await lists.remove(
          listName: 'fruits',
          entries: const [AppendOnlyListEntry(tag: 't', value: 'banana')],
          relays: [relayUrl],
        );
        await _waitForRelayCount(relay, 2);

        // Local view from the projection (no network).
        final local = await lists.getList(pubkey: pubkey, listName: 'fruits');
        expect(
          local.entries.map((e) => e.value).toSet(),
          equals({'apple', 'cherry'}),
        );

        // What did the relay actually receive?
        final kinds = relay.receivedEvents.map((e) => e['kind']).toList();
        expect(kinds, containsAll([kindAdd, kindRemove]));
      },
    );

    test('a fresh AppendOnlyLists hydrates state from the relay', () async {
      // Seed via instance A.
      await lists.add(
        listName: 'fruits',
        entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
        relays: [relay.url],
      );
      await _waitForRelayCount(relay, 1);

      // Tear down everything except the relay.
      await lists.dispose();
      await outbox.dispose();
      await ndk.destroy();

      // Bring a brand-new stack up, pointing at the same relay.
      final ndk2 = ndkForRelay(relay);
      final outboxDb2 = await newDatabaseFactoryMemory().openDatabase(
        'outbox2.db',
      );
      final projDb2 = await newDatabaseFactoryMemory().openDatabase(
        'projection2.db',
      );
      final outbox2 = OfflineBroadcast.withNdk(ndk2, db: outboxDb2);
      outbox2.start();
      final lists2 = AppendOnlyLists(
        ndk: ndk2,
        outbox: outbox2,
        projection: ProjectionStore(projDb2),
      );

      // Force a refresh so we definitely hit the relay (empty local stores).
      final state = await lists2.getList(
        pubkey: pubkey,
        listName: 'fruits',
        forceRefresh: true,
        relays: [relay.url],
      );

      expect(state.entries.map((e) => e.value).toSet(), equals({'apple'}));

      await lists2.dispose();
      await outbox2.dispose();
      await ndk2.destroy();
    });

    test('watchList emits initial state and updates on local writes', () async {
      final emitted = <Set<String>>[];
      final sub = lists
          .watchList(pubkey: pubkey, listName: 'fruits', relays: [relay.url])
          .listen((s) => emitted.add(s.entries.map((e) => e.value).toSet()));

      // Initial snapshot (empty) should arrive.
      await _waitUntil(() => emitted.isNotEmpty);

      await lists.add(
        listName: 'fruits',
        entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
        relays: [relay.url],
      );

      await _waitUntil(
        () => emitted.any((s) => s.contains('apple')),
        timeout: const Duration(seconds: 3),
      );

      expect(emitted.first, isEmpty);
      expect(emitted.last, equals({'apple'}));

      await sub.cancel();
    });

    test(
      'add auto-resolves NIP-65 write relays when relays: omitted',
      () async {
        // Seed the NDK cache with a NIP-65 list whose only write relay is
        // the mock. With this in place, the resolver should pick that URL
        // without an explicit `relays:` argument.
        await ndk.config.cache.saveUserRelayList(
          UserRelayList(
            pubKey: pubkey,
            relays: {relay.url: ReadWriteMarker.readWrite},
            createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
            refreshedTimestamp: DateTime.now().millisecondsSinceEpoch ~/ 1000,
          ),
        );

        await lists.add(
          listName: 'fruits',
          entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
          // no `relays:`
        );

        await _waitForRelayCount(relay, 1);
        expect(relay.receivedEvents.single['kind'], equals(kindAdd));
      },
    );

    test(
      'add throws when neither relays: nor NIP-65 list is available',
      () async {
        // No NIP-65 cached, no explicit relays passed → should throw.
        expect(
          () => lists.add(
            listName: 'fruits',
            entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
          ),
          throwsStateError,
        );
      },
    );

    test(
      'decryptPending resurfaces private entries once a signer is available',
      () async {
        // 1. Publish an Add with a private entry as the logged signer.
        await lists.add(
          listName: 'fruits',
          entries: const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
            AppendOnlyListEntry(tag: 't', value: 'banana', private: true),
          ],
          relays: [relay.url],
        );
        await _waitForRelayCount(relay, 1);

        // 2. Simulate "signer was never available when this event was
        //    seen" by wiping both the projection AND the persistent
        //    decryption cache for the event in question, then re-folding
        //    without supplying a plaintext map. The private entry stays
        //    opaque and the event id ends up in pending.
        final cached = await ndk.config.cache.loadEvents(
          pubKeys: [pubkey],
          kinds: const [kindAdd, kindRemove],
        );
        await lists.projection.delete(pubkey: pubkey, listName: 'fruits');
        await lists.projection.deleteDecryptedPlaintext(
          cached.map((e) => e.id),
        );
        final reFolded = AppendOnlyListState.fromEvents(
          cached,
          pubkey: pubkey,
          listName: 'fruits',
          // plaintextById omitted on purpose
        );
        await lists.projection.save(reFolded);

        var state = await lists.getList(pubkey: pubkey, listName: 'fruits');
        expect(state.entries.map((e) => e.value).toSet(), equals({'apple'}));
        expect(state.pendingDecryptionEventIds, hasLength(1));

        // 3. Signer is back: decryptPending should fold the private entry
        //    in and clear the pending set.
        state = await lists.decryptPending(pubkey: pubkey, listName: 'fruits');
        expect(
          state.entries.map((e) => e.value).toSet(),
          equals({'apple', 'banana'}),
        );
        expect(state.pendingDecryptionEventIds, isEmpty);
      },
    );

    test(
      'private entries survive a re-fold without the signer (decryption cache hit)',
      () async {
        // Write a private entry while the signer is logged in. _emit
        // populates the persistent decryption cache as a side effect.
        await lists.add(
          listName: 'fruits',
          entries: const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
            AppendOnlyListEntry(tag: 't', value: 'banana', private: true),
          ],
          relays: [relay.url],
        );
        await _waitForRelayCount(relay, 1);

        // Wipe ONLY the projection. The decryption cache and the NDK
        // event cache stay intact - they're the persistence layer the
        // refactor is supposed to lean on.
        await lists.projection.delete(pubkey: pubkey, listName: 'fruits');

        // Log the signer out so the package has no way to call
        // `decryptNip44` during the re-fold. The private entry should
        // still be recovered, purely from the persistent decryption cache.
        ndk.accounts.logout();

        final state = await lists.getList(
          pubkey: pubkey,
          listName: 'fruits',
          forceRefresh: true,
        );
        expect(
          state.entries.map((e) => e.value).toSet(),
          equals({'apple', 'banana'}),
        );
        expect(state.pendingDecryptionEventIds, isEmpty);
      },
    );

    test(
      'consolidate splits fresh Adds and deletions when maxEventBytes is small',
      () async {
        // Reuse the fixture's connected NDK / outbox but swap in a tiny
        // byte-cap AppendOnlyLists pointing at the same projection. With
        // 350 bytes the baseline (~295 for an Add carrying just the d
        // tag) leaves room for only a few short `t` entries per event,
        // so consolidation must split.
        final listsTiny = AppendOnlyLists(
          ndk: ndk,
          outbox: outbox,
          projection: lists.projection,
          maxEventBytes: 350,
        );

        await listsTiny.add(
          listName: 'fruits',
          entries: const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
            AppendOnlyListEntry(tag: 't', value: 'banana'),
            AppendOnlyListEntry(tag: 't', value: 'cherry'),
            AppendOnlyListEntry(tag: 't', value: 'durian'),
            AppendOnlyListEntry(tag: 't', value: 'elderberry'),
            AppendOnlyListEntry(tag: 't', value: 'fig'),
          ],
          relays: [relay.url],
        );
        await _waitForRelayCount(relay, 1);

        await listsTiny.consolidate(listName: 'fruits', relays: [relay.url]);
        // 1 original + ≥2 fresh Adds + ≥1 deletion = ≥4.
        await _waitForRelayCount(relay, 4, timeout: const Duration(seconds: 5));

        final adds = relay.receivedEvents
            .where((e) => e['kind'] == kindAdd)
            .toList();
        final deletions = relay.receivedEvents
            .where((e) => e['kind'] == 5)
            .toList();

        // 1 original Add + at least 2 fresh Adds from the split.
        expect(adds.length, greaterThanOrEqualTo(3));
        // The fresh-Add events (everything except the very first one) must
        // each carry fewer entries than the original.
        final freshAdds = adds.sublist(1);
        for (final fresh in freshAdds) {
          final tCount = (fresh['tags'] as List)
              .cast<List>()
              .where((t) => t[0] == 't')
              .length;
          expect(tCount, lessThan(6));
        }
        // The fresh Adds collectively cover every entry.
        final coveredValues = <String>{};
        for (final fresh in freshAdds) {
          for (final tag in (fresh['tags'] as List).cast<List>()) {
            if (tag[0] == 't') coveredValues.add(tag[1] as String);
          }
        }
        expect(
          coveredValues,
          equals({'apple', 'banana', 'cherry', 'durian', 'elderberry', 'fig'}),
        );
        expect(deletions, isNotEmpty);
      },
    );

    test(
      'a second device honors NIP-09 deletions emitted by consolidation',
      () async {
        // Device A: add two entries, then consolidate. Consolidation emits
        // a fresh Add carrying the same state plus a kind 5 deletion for
        // the two originals.
        await lists.add(
          listName: 'fruits',
          entries: const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
            AppendOnlyListEntry(tag: 't', value: 'banana'),
          ],
          relays: [relay.url],
        );
        await _waitForRelayCount(relay, 1);
        await lists.consolidate(listName: 'fruits', relays: [relay.url]);
        // 1 original + 1 fresh Add + 1 deletion = 3.
        await _waitForRelayCount(relay, 3, timeout: const Duration(seconds: 5));

        final originalAddId = relay.receivedEvents.first['id'] as String;

        // Tear device A down before bringing device B up. Two NDK
        // instances against the same WebSocket relay can race on the
        // subscription stream - keeping them sequential makes the test
        // deterministic.
        await lists.dispose();
        await outbox.dispose();
        await ndk.destroy();

        // Device B: brand-new stack pointing at the same relay.
        final ndk2 = ndkForRelay(relay);
        final outboxDb2 = await newDatabaseFactoryMemory().openDatabase(
          'outbox-b.db',
        );
        final projDb2 = await newDatabaseFactoryMemory().openDatabase(
          'projection-b.db',
        );
        final outbox2 = OfflineBroadcast.withNdk(ndk2, db: outboxDb2);
        outbox2.start();
        final lists2 = AppendOnlyLists(
          ndk: ndk2,
          outbox: outbox2,
          projection: ProjectionStore(projDb2),
        );

        // Sync from the relay (no local cache yet).
        final state = await lists2.getList(
          pubkey: pubkey,
          listName: 'fruits',
          forceRefresh: true,
          relays: [relay.url],
        );

        // Final state is correct in either implementation, but the
        // tombstone path is what we're testing: the original Add's id
        // must be persisted as a tombstone on device B.
        expect(
          state.entries.map((e) => e.value).toSet(),
          equals({'apple', 'banana'}),
        );
        final tombstones = await lists2.projection.loadTombstones(
          pubkey: pubkey,
          listName: 'fruits',
        );
        expect(tombstones, contains(originalAddId));

        await lists2.dispose();
        await outbox2.dispose();
        await ndk2.destroy();

        // Re-assign locals so tearDown's dispose calls are safe.
        ndk = ndk2;
        outbox = outbox2;
        lists = lists2;
      },
    );

    test(
      'write methods throw when neither signer arg nor logged account',
      () async {
        // Tear down the default fixture so we can build a stack with no logged
        // account.
        await lists.dispose();
        await outbox.dispose();
        await ndk.destroy();

        final ndk2 = ndkForRelay(relay);
        final outboxDb2 = await newDatabaseFactoryMemory().openDatabase(
          'outbox-na.db',
        );
        final projDb2 = await newDatabaseFactoryMemory().openDatabase(
          'projection-na.db',
        );
        final outbox2 = OfflineBroadcast.withNdk(ndk2, db: outboxDb2);
        outbox2.start();
        final lists2 = AppendOnlyLists(
          ndk: ndk2,
          outbox: outbox2,
          projection: ProjectionStore(projDb2),
        );

        expect(
          () => lists2.add(
            listName: 'fruits',
            entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
            relays: [relay.url],
          ),
          throwsStateError,
        );

        // Re-assign locals so tearDown's dispose calls are safe.
        ndk = ndk2;
        outbox = outbox2;
        lists = lists2;
      },
    );

    test('consolidate emits a fresh Add + a NIP-09 deletion event', () async {
      await lists.add(
        listName: 'fruits',
        entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
        relays: [relay.url],
      );
      await lists.add(
        listName: 'fruits',
        entries: const [AppendOnlyListEntry(tag: 't', value: 'banana')],
        relays: [relay.url],
      );
      await lists.remove(
        listName: 'fruits',
        entries: const [AppendOnlyListEntry(tag: 't', value: 'apple')],
        relays: [relay.url],
      );
      await _waitForRelayCount(relay, 3);

      await lists.consolidate(listName: 'fruits', relays: [relay.url]);
      // Expect 3 originals + 1 fresh Add + 1 deletion = 5
      await _waitForRelayCount(relay, 5, timeout: const Duration(seconds: 5));

      final deletions = relay.receivedEvents
          .where((e) => e['kind'] == 5)
          .toList();
      expect(deletions, hasLength(1));
      final eTags = (deletions.single['tags'] as List)
          .cast<List>()
          .where((t) => t[0] == 'e')
          .map((t) => t[1] as String)
          .toList();
      // The deletion should reference the 3 superseded events.
      expect(eTags, hasLength(3));
    });
  });
}

/// Polls until [check] returns true or [timeout] elapses.
Future<void> _waitUntil(
  bool Function() check, {
  Duration timeout = const Duration(seconds: 2),
  Duration interval = const Duration(milliseconds: 25),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!check()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Condition not met within $timeout');
    }
    await Future<void>.delayed(interval);
  }
}

Future<void> _waitForRelayCount(
  MockRelay relay,
  int count, {
  Duration timeout = const Duration(seconds: 3),
}) => _waitUntil(() => relay.receivedEvents.length >= count, timeout: timeout);
