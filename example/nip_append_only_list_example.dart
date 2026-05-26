import 'package:broadcast_queue_shim_for_ndk/broadcast_queue_shim_for_ndk.dart';
import 'package:ndk/ndk.dart';
import 'package:nip_append_only_list/nip_append_only_list.dart';
import 'package:sembast/sembast_memory.dart';

/// Minimal end-to-end wiring of [AppendOnlyLists].
///
/// The example uses in-memory stores so it can run as a standalone Dart
/// program; in a real app, swap to file-backed sembast and ndk's
/// SembastCacheManager.
Future<void> main() async {
  // 1. NDK with an in-memory event cache.
  final ndk = Ndk(
    NdkConfig(eventVerifier: Bip340EventVerifier(), cache: MemCacheManager()),
  );

  // 2. Persistent (here: in-memory) sembast DBs for the outbox and our
  //    cleartext projection. The caller owns both lifecycles.
  final outboxDb = await newDatabaseFactoryMemory().openDatabase('outbox.db');
  final projectionDb = await newDatabaseFactoryMemory().openDatabase(
    'projection.db',
  );

  // 3. Outbox: persists outgoing events, retries until every relay acks.
  final outbox = OfflineBroadcast.withNdk(ndk, db: outboxDb);
  outbox.start();

  // 4. Cleartext projection store.
  final projection = ProjectionStore(projectionDb);

  // 5. The usecase.
  final lists = AppendOnlyLists(
    ndk: ndk,
    outbox: outbox,
    projection: projection,
  );

  // 6. Generate a fresh keypair and log it into NDK. Once logged in,
  //    write methods pick up the signer automatically.
  final signer = const Bip340EventSignerFactory().createWithNewKeyPair();
  ndk.accounts.loginExternalSigner(signer: signer);

  // Add three fruits. `relays` is omitted: NIP-65 write relays for the
  // signer are resolved via NDK. Pass `relays:` explicitly to override.
  await lists.add(
    listName: 'fruits',
    entries: const [
      AppendOnlyListEntry(tag: 't', value: 'apple'),
      AppendOnlyListEntry(tag: 't', value: 'banana'),
      AppendOnlyListEntry(tag: 't', value: 'cherry'),
    ],
  );

  // Remove banana.
  await lists.remove(
    listName: 'fruits',
    entries: const [AppendOnlyListEntry(tag: 't', value: 'banana')],
  );

  // Read back from the local projection (no network needed).
  final state = await lists.getList(
    pubkey: signer.getPublicKey(),
    listName: 'fruits',
  );
  print(state.entries.map((e) => e.value).toList()); // [apple, cherry]

  await lists.dispose();
  await outbox.dispose();
  await ndk.destroy();
}
