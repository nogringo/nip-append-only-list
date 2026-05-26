/// A single entry in an append-only list.
///
/// Entries are identified by the pair `(tag, value)`. The [private] flag
/// records whether the entry was carried in the NIP-44 encrypted content of
/// its most recent Add event; it is metadata, not part of CRDT identity.
class AppendOnlyListEntry {
  /// Tag name (e.g. `t`, `p`, `e`, `a`, `r`, `word`, `relay`, `emoji`).
  final String tag;

  /// Tag value.
  final String value;

  /// Whether the entry was published privately (encrypted content) in its
  /// most recent Add. Not part of equality.
  final bool private;

  const AppendOnlyListEntry({
    required this.tag,
    required this.value,
    this.private = false,
  });

  AppendOnlyListEntry copyWith({String? tag, String? value, bool? private}) =>
      AppendOnlyListEntry(
        tag: tag ?? this.tag,
        value: value ?? this.value,
        private: private ?? this.private,
      );

  @override
  bool operator ==(Object other) =>
      other is AppendOnlyListEntry && other.tag == tag && other.value == value;

  @override
  int get hashCode => Object.hash(tag, value);

  @override
  String toString() =>
      'AppendOnlyListEntry($tag=$value${private ? ', private' : ''})';
}
