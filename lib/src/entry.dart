/// A single entry in an append-only list.
///
/// Entries are identified by the pair `(tag, value)`. The [private] flag
/// records whether the entry was carried in the NIP-44 encrypted content of
/// its most recent Add event, and [extras] holds the tag elements after the
/// value; both are metadata, not part of CRDT identity.
class AppendOnlyListEntry {
  /// Tag name (e.g. `t`, `p`, `e`, `a`, `r`, `word`, `relay`, `emoji`).
  final String tag;

  /// Tag value.
  final String value;

  /// Whether the entry was published privately (encrypted content) in its
  /// most recent Add. Not part of equality.
  final bool private;

  /// Tag elements after [value] (relay hints, petnames, app data), as
  /// published in the most recent Add. Not part of equality.
  final List<String> extras;

  const AppendOnlyListEntry({
    required this.tag,
    required this.value,
    this.private = false,
    this.extras = const [],
  });

  /// The entry stripped of its metadata, as used for state keys.
  AppendOnlyListEntry get identity =>
      AppendOnlyListEntry(tag: tag, value: value);

  /// The entry as a tag tuple: `[tag, value, ...extras]`.
  List<String> toTag() => [tag, value, ...extras];

  AppendOnlyListEntry copyWith({
    String? tag,
    String? value,
    bool? private,
    List<String>? extras,
  }) => AppendOnlyListEntry(
    tag: tag ?? this.tag,
    value: value ?? this.value,
    private: private ?? this.private,
    extras: extras ?? this.extras,
  );

  @override
  bool operator ==(Object other) =>
      other is AppendOnlyListEntry && other.tag == tag && other.value == value;

  @override
  int get hashCode => Object.hash(tag, value);

  @override
  String toString() =>
      'AppendOnlyListEntry($tag=$value${extras.isEmpty ? '' : ' $extras'}'
      '${private ? ', private' : ''})';
}
