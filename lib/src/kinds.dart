/// Event kinds defined by the Append-Only Lists NIP.
///
/// `kind:1990` carries an addition; `kind:1991` carries a removal. Both are
/// regular (non-replaceable) event kinds.
library;

/// Operation carried by an append-only list event.
enum AppendOnlyListOp {
  add(kindAdd),
  remove(kindRemove);

  const AppendOnlyListOp(this.kind);

  final int kind;

  static AppendOnlyListOp? fromKind(int kind) => switch (kind) {
    kindAdd => AppendOnlyListOp.add,
    kindRemove => AppendOnlyListOp.remove,
    _ => null,
  };
}

const int kindAdd = 1990;
const int kindRemove = 1991;

const List<int> appendOnlyKinds = <int>[kindAdd, kindRemove];

/// Tag name identifying the list (`d` tag, inherited from NIP-51).
const String dTag = 'd';
