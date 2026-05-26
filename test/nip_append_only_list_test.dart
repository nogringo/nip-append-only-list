import 'package:ndk/ndk.dart';
import 'package:nip_append_only_list/nip_append_only_list.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

void main() {
  group('AppendOnlyListEntry', () {
    test('equality ignores `private` flag', () {
      const a = AppendOnlyListEntry(tag: 't', value: 'apple');
      const b = AppendOnlyListEntry(tag: 't', value: 'apple', private: true);
      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
    });

    test('different tag or value ⇒ different entry', () {
      expect(
        const AppendOnlyListEntry(tag: 't', value: 'apple'),
        isNot(equals(const AppendOnlyListEntry(tag: 't', value: 'banana'))),
      );
      expect(
        const AppendOnlyListEntry(tag: 't', value: 'apple'),
        isNot(equals(const AppendOnlyListEntry(tag: 'p', value: 'apple'))),
      );
    });
  });

  group('EntryStat (OR-Set semantics)', () {
    test('Add only ⇒ present', () {
      final s = const EntryStat().applyAdd(10, private: false);
      expect(s.isPresent, isTrue);
    });

    test('Remove strictly later than Add ⇒ absent', () {
      final s = const EntryStat().applyAdd(10, private: false).applyRemove(20);
      expect(s.isPresent, isFalse);
    });

    test('Add strictly later than Remove ⇒ present', () {
      final s = const EntryStat().applyRemove(10).applyAdd(20, private: false);
      expect(s.isPresent, isTrue);
    });

    test(
      'Tie on timestamps ⇒ Add wins (NIP: Remove must be strictly later)',
      () {
        final s = const EntryStat()
            .applyAdd(10, private: false)
            .applyRemove(10);
        expect(s.isPresent, isTrue);
      },
    );

    test('Operations are commutative', () {
      // Add(10), Add(20), Remove(15) → present (highest Add=20 ≥ Remove=15)
      final a = const EntryStat()
          .applyAdd(10, private: false)
          .applyAdd(20, private: false)
          .applyRemove(15);
      final b = const EntryStat()
          .applyRemove(15)
          .applyAdd(20, private: false)
          .applyAdd(10, private: false);
      expect(a.isPresent, isTrue);
      expect(a.isPresent, equals(b.isPresent));
      expect(a.lastAddAt, equals(b.lastAddAt));
      expect(a.lastRemoveAt, equals(b.lastRemoveAt));
    });

    test('Idempotent on repeated Add at same timestamp', () {
      final once = const EntryStat().applyAdd(10, private: false);
      final twice = once.applyAdd(10, private: false);
      expect(once.isPresent, equals(twice.isPresent));
      expect(once.lastAddAt, equals(twice.lastAddAt));
    });
  });

  group('AppendOnlyListState.foldEvents', () {
    AppendOnlyListEvent ev(
      AppendOnlyListOp op,
      int createdAt,
      List<AppendOnlyListEntry> entries, {
      String listName = 'fruits',
      String pubkey = 'deadbeef',
    }) => AppendOnlyListEvent(
      op: op,
      listName: listName,
      pubkey: pubkey,
      createdAt: createdAt,
      eventId: 'id-$createdAt-${op.kind}',
      entries: entries,
      hasEncryptedContent: false,
    );

    test('NIP example: add apple, banana, cherry then remove banana', () {
      final state = AppendOnlyListState.foldEvents(
        [
          ev(AppendOnlyListOp.add, 1715000000, const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
            AppendOnlyListEntry(tag: 't', value: 'banana'),
            AppendOnlyListEntry(tag: 't', value: 'cherry'),
          ]),
          ev(AppendOnlyListOp.remove, 1715100000, const [
            AppendOnlyListEntry(tag: 't', value: 'banana'),
          ]),
        ],
        listName: 'fruits',
        pubkey: 'deadbeef',
      );
      expect(
        state.entries.map((e) => e.value).toSet(),
        equals({'apple', 'cherry'}),
      );
      expect(state.pendingDecryptionEventIds, isEmpty);
    });

    test('Mismatched author or list is filtered out', () {
      final state = AppendOnlyListState.foldEvents(
        [
          ev(AppendOnlyListOp.add, 1, const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
          ]),
          ev(AppendOnlyListOp.add, 2, const [
            AppendOnlyListEntry(tag: 't', value: 'banana'),
          ], pubkey: 'other'),
          ev(AppendOnlyListOp.add, 3, const [
            AppendOnlyListEntry(tag: 't', value: 'cherry'),
          ], listName: 'other'),
        ],
        listName: 'fruits',
        pubkey: 'deadbeef',
      );
      expect(state.entries.map((e) => e.value), equals({'apple'}));
    });

    test('Re-add after remove resurrects the entry', () {
      final state = AppendOnlyListState.foldEvents(
        [
          ev(AppendOnlyListOp.add, 1, const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
          ]),
          ev(AppendOnlyListOp.remove, 2, const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
          ]),
          ev(AppendOnlyListOp.add, 3, const [
            AppendOnlyListEntry(tag: 't', value: 'apple'),
          ]),
        ],
        listName: 'fruits',
        pubkey: 'deadbeef',
      );
      expect(state.entries.map((e) => e.value), equals({'apple'}));
    });
  });

  group('buildAppendOnlyEvent', () {
    test('public-only entries set tags and leave content empty', () async {
      final event = await buildAppendOnlyEvent(
        op: AppendOnlyListOp.add,
        listName: 'fruits',
        entries: const [
          AppendOnlyListEntry(tag: 't', value: 'apple'),
          AppendOnlyListEntry(tag: 't', value: 'banana'),
        ],
        pubkey: 'deadbeef',
        createdAt: 1000,
      );
      expect(event.kind, equals(kindAdd));
      expect(event.content, isEmpty);
      expect(event.getDtag(), equals('fruits'));
      expect(event.tTags, containsAll(['apple', 'banana']));
    });

    test('throws when private entries are requested without a signer', () {
      expect(
        () => buildAppendOnlyEvent(
          op: AppendOnlyListOp.add,
          listName: 'fruits',
          entries: const [
            AppendOnlyListEntry(tag: 't', value: 'apple', private: true),
          ],
          pubkey: 'deadbeef',
        ),
        throwsStateError,
      );
    });
  });

  group('build/parse roundtrip with NIP-44 self-encryption', () {
    test('private entries decrypt back when signer available', () async {
      final factory = const Bip340EventSignerFactory();
      final signer = factory.createWithNewKeyPair();
      final pubkey = signer.getPublicKey();

      final raw = await buildAppendOnlyEvent(
        op: AppendOnlyListOp.add,
        listName: 'fruits',
        entries: const [
          AppendOnlyListEntry(tag: 't', value: 'apple'), // public
          AppendOnlyListEntry(tag: 't', value: 'banana', private: true),
        ],
        pubkey: pubkey,
        signer: signer,
        createdAt: 1000,
      );
      expect(raw.content, isNotEmpty); // encrypted content present

      // `parse` is pure now: the caller resolves the NIP-44 plaintext
      // separately (typically via the persistent decryption cache + signer
      // fallback) and passes it in.
      final plaintext = await signer.decryptNip44(
        ciphertext: raw.content,
        senderPubKey: signer.getPublicKey(),
      );
      final parsedWithPlaintext = AppendOnlyListEvent.parse(
        raw,
        plaintext: plaintext,
      );
      expect(parsedWithPlaintext, isNotNull);
      expect(
        parsedWithPlaintext!.entries.map((e) => e.value).toSet(),
        equals({'apple', 'banana'}),
      );
      expect(
        parsedWithPlaintext.entries
            .firstWhere((e) => e.value == 'banana')
            .private,
        isTrue,
      );

      // Without a plaintext, the public tag is still parsed; private is opaque.
      final parsedWithout = AppendOnlyListEvent.parse(raw);
      expect(parsedWithout!.entries.map((e) => e.value), equals(['apple']));
      expect(parsedWithout.hasEncryptedContent, isTrue);
    });
  });

  group('ProjectionStore', () {
    test(
      'persists entries and survives a re-open of the same database',
      () async {
        final db = await newDatabaseFactoryMemory().openDatabase('proj.db');
        final store = ProjectionStore(db);

        final state = AppendOnlyListState(
          listName: 'fruits',
          pubkey: 'deadbeef',
          stats: {
            const AppendOnlyListEntry(tag: 't', value: 'apple'):
                const EntryStat(lastAddAt: 10, privateOnLastAdd: false),
            const AppendOnlyListEntry(tag: 't', value: 'banana'):
                const EntryStat(lastAddAt: 5, lastRemoveAt: 8),
          },
          pendingDecryptionEventIds: const {'evid-1'},
        );
        await store.save(state);

        // Reload through a fresh store instance (same db handle).
        final fresh = ProjectionStore(db);
        final loaded = await fresh.load(pubkey: 'deadbeef', listName: 'fruits');
        expect(loaded.pendingDecryptionEventIds, equals({'evid-1'}));
        expect(loaded.entries.map((e) => e.value), equals({'apple'}));
        expect(
          loaded
              .stats[const AppendOnlyListEntry(tag: 't', value: 'banana')]
              ?.isPresent,
          isFalse,
        );
      },
    );

    test('update() runs a read-modify-write atomically', () async {
      final db = await newDatabaseFactoryMemory().openDatabase('proj2.db');
      final store = ProjectionStore(db);

      await store.update(
        pubkey: 'deadbeef',
        listName: 'fruits',
        mutator: (current) => AppendOnlyListState(
          listName: current.listName,
          pubkey: current.pubkey,
          stats: {
            const AppendOnlyListEntry(tag: 't', value: 'cherry'):
                const EntryStat(lastAddAt: 42),
          },
          pendingDecryptionEventIds: const {},
        ),
      );

      final out = await store.load(pubkey: 'deadbeef', listName: 'fruits');
      expect(out.entries.single.value, equals('cherry'));
    });
  });

  group('filters', () {
    test('listFilter wires up authors, kinds, #d and since', () {
      final f = listFilter(pubkey: 'deadbeef', listName: 'fruits', since: 1000);
      expect(f.authors, equals(['deadbeef']));
      expect(f.kinds, equals(appendOnlyKinds));
      expect(f.since, equals(1000));
      expect(f.tags?['#d'], equals(['fruits']));
    });
  });
}
