import 'package:ndk/ndk.dart';

import 'kinds.dart';

/// Returns a [Filter] that fetches an author's append-only list events.
///
/// [since] is an optional `created_at` floor (in seconds since epoch) for
/// incremental sync - pass the previous high-water-mark to receive only
/// events newer than what is already cached.
Filter listFilter({
  required String pubkey,
  required String listName,
  int? since,
}) => Filter(
  authors: [pubkey],
  kinds: appendOnlyKinds,
  tags: {
    '#$dTag': [listName],
  },
  since: since,
);

/// Returns a [Filter] that fetches NIP-09 deletion events from [pubkey]
/// that target append-only kinds. The `#k` tag constraint is what makes
/// this query cheap: relays only return deletions whose `k` tag is
/// `"1990"` or `"1991"`, skipping every unrelated deletion the author
/// has ever made.
///
/// Note: kind 5 events have no `d` tag, so the consumer must intersect
/// the referenced event ids with the events it actually has for the
/// target list (or accept all of them and re-fold from cache).
Filter deletionFilter({required String pubkey, int? since}) => Filter(
  authors: [pubkey],
  kinds: const [5],
  tags: {'#k': appendOnlyKinds.map((k) => '$k').toList()},
  since: since,
);
