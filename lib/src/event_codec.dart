import 'dart:convert';

import 'package:ndk/ndk.dart';

import 'entry.dart';
import 'kinds.dart';

/// A parsed append-only list event, with public and private entries already
/// separated. Private entries are populated only when content decryption
/// succeeded (i.e. a capable [EventSigner] was provided to [parse]).
class AppendOnlyListEvent {
  /// `add` for kind 1990, `remove` for kind 1991.
  final AppendOnlyListOp op;

  /// List name, taken from the `d` tag.
  final String listName;

  /// Author public key (event `pubkey`).
  final String pubkey;

  /// Event `created_at` in seconds since epoch.
  final int createdAt;

  /// Event id (hex). Useful for deletions during consolidation.
  final String eventId;

  /// Effective entries operated on by this event: union of public tags and
  /// (when successfully decrypted) entries from encrypted content.
  final List<AppendOnlyListEntry> entries;

  /// Whether this event carried encrypted content. When `true` and no
  /// entries are flagged `private`, the content has not been decrypted yet.
  final bool hasEncryptedContent;

  const AppendOnlyListEvent({
    required this.op,
    required this.listName,
    required this.pubkey,
    required this.createdAt,
    required this.eventId,
    required this.entries,
    required this.hasEncryptedContent,
  });

  /// Parses a raw nostr event into an [AppendOnlyListEvent].
  ///
  /// Returns `null` if [event] is not a 1990/1991 kind, or has no `d` tag.
  ///
  /// [plaintext] is the NIP-44-decrypted content. Pass it when you have
  /// recovered (or just decrypted) the encrypted content; the function
  /// decodes the private entries from it. If `null` and the event has
  /// encrypted content, private entries stay unresolved and the consumer
  /// can retry later (typically by storing the event id under
  /// `AppendOnlyListState.pendingDecryptionEventIds`).
  ///
  /// This function performs no I/O - decryption is the caller's job, which
  /// keeps `parse` deterministic and testable and lets a higher layer
  /// cache cleartexts for re-fold without a signer.
  static AppendOnlyListEvent? parse(Nip01Event event, {String? plaintext}) {
    final op = AppendOnlyListOp.fromKind(event.kind);
    if (op == null) return null;
    final name = event.getDtag();
    if (name == null) return null;

    final entries = <AppendOnlyListEntry>[];
    for (final tag in event.tags) {
      if (tag.length < 2) continue;
      final tagName = tag[0];
      if (tagName == dTag) continue;
      // Accept any single-letter or short tag as an entry; the spec
      // inherits NIP-51's tag set without enumerating it.
      entries.add(
        AppendOnlyListEntry(tag: tagName, value: tag[1], private: false),
      );
    }

    final hasContent = event.content.isNotEmpty;
    if (hasContent && plaintext != null && plaintext.isNotEmpty) {
      try {
        final decoded = jsonDecode(plaintext);
        if (decoded is List) {
          for (final raw in decoded) {
            if (raw is! List || raw.length < 2) continue;
            entries.add(
              AppendOnlyListEntry(
                tag: raw[0].toString(),
                value: raw[1].toString(),
                private: true,
              ),
            );
          }
        }
      } catch (_) {
        // Malformed plaintext: skip private entries silently. The event id
        // stays in the pending set so a future retry can fix it.
      }
    }

    return AppendOnlyListEvent(
      op: op,
      listName: name,
      pubkey: event.pubKey,
      createdAt: event.createdAt,
      eventId: event.id,
      entries: entries,
      hasEncryptedContent: hasContent,
    );
  }
}

/// Builds (but does not sign or broadcast) a kind 1990 / 1991 event.
///
/// Public entries become event tags; private entries are encrypted as a
/// JSON array of `[tag, value]` tuples into the event content using NIP-44
/// self-encryption. [signer] is required when any entry is private.
Future<Nip01Event> buildAppendOnlyEvent({
  required AppendOnlyListOp op,
  required String listName,
  required List<AppendOnlyListEntry> entries,
  required String pubkey,
  EventSigner? signer,
  int? createdAt,
}) async {
  final publicEntries = entries.where((e) => !e.private).toList();
  final privateEntries = entries.where((e) => e.private).toList();

  if (privateEntries.isNotEmpty && (signer == null || !signer.canSign())) {
    throw StateError(
      'A signer with NIP-44 capability is required to build an event with '
      'private entries.',
    );
  }

  final tags = <List<String>>[
    <String>[dTag, listName],
    for (final e in publicEntries) <String>[e.tag, e.value],
  ];

  var content = '';
  if (privateEntries.isNotEmpty) {
    final plaintext = jsonEncode(
      privateEntries.map((e) => [e.tag, e.value]).toList(),
    );
    final cipher = await signer!.encryptNip44(
      plaintext: plaintext,
      recipientPubKey: signer.getPublicKey(),
    );
    if (cipher == null) {
      throw StateError('NIP-44 encryption returned null.');
    }
    content = cipher;
  }

  return Nip01Event(
    pubKey: pubkey,
    kind: op.kind,
    tags: tags,
    content: content,
    createdAt: createdAt ?? DateTime.now().millisecondsSinceEpoch ~/ 1000,
  );
}
