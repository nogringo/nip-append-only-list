import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:ndk/ndk.dart';

/// Minimal in-process Nostr relay for tests.
///
/// Implements the relay-side subset we need to exercise [AppendOnlyLists]:
///   * `["EVENT", e]` → store, reply with `["OK", id, true, ""]`, fan out
///     to matching open subscriptions.
///   * `["REQ", subId, filter, …]` → reply with stored matches followed by
///     `["EOSE", subId]`, then keep streaming new matching events until
///     `["CLOSE", subId]`.
///
/// Filter support: `kinds`, `authors`, `since`, `until`, and `#<tag>` tag
/// constraints. That's enough for the append-only NIP.
class MockRelay {
  MockRelay({this.port = 0});

  final int port;
  HttpServer? _server;
  final List<Map<String, dynamic>> _stored = [];
  final Map<WebSocket, Map<String, List<Map<String, dynamic>>>> _subscriptions =
      {};

  String get url => 'ws://localhost:${_server!.port}';

  /// Returns a snapshot of every event the relay has accepted so far.
  List<Map<String, dynamic>> get receivedEvents =>
      List<Map<String, dynamic>>.unmodifiable(_stored);

  Future<void> start() async {
    _server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
    _server!.transform(WebSocketTransformer()).listen(_handleSocket);
  }

  Future<void> stop() async {
    for (final socket in _subscriptions.keys.toList()) {
      await socket.close();
    }
    _subscriptions.clear();
    await _server?.close(force: true);
    _server = null;
  }

  void _handleSocket(WebSocket socket) {
    _subscriptions[socket] = {};
    socket.listen(
      (data) => _handleMessage(socket, data),
      onDone: () => _subscriptions.remove(socket),
      onError: (_) => _subscriptions.remove(socket),
    );
  }

  void _handleMessage(WebSocket socket, dynamic data) {
    final raw = jsonDecode(data as String);
    if (raw is! List || raw.isEmpty) return;
    switch (raw[0]) {
      case 'EVENT':
        if (raw.length < 2 || raw[1] is! Map) return;
        final event = Map<String, dynamic>.from(raw[1] as Map);
        _stored.add(event);
        socket.add(jsonEncode(['OK', event['id'], true, '']));
        // Fan out to live subscribers.
        for (final entry in _subscriptions.entries) {
          for (final sub in entry.value.entries) {
            if (sub.value.any((f) => _matches(event, f))) {
              entry.key.add(jsonEncode(['EVENT', sub.key, event]));
            }
          }
        }
        break;
      case 'REQ':
        if (raw.length < 3) return;
        final subs = _subscriptions[socket];
        if (subs == null) return;
        final subId = raw[1] as String;
        final filters = raw.sublist(2).cast<Map<String, dynamic>>();
        subs[subId] = filters;
        for (final event in _stored) {
          if (filters.any((f) => _matches(event, f))) {
            socket.add(jsonEncode(['EVENT', subId, event]));
          }
        }
        socket.add(jsonEncode(['EOSE', subId]));
        break;
      case 'CLOSE':
        if (raw.length < 2) return;
        _subscriptions[socket]?.remove(raw[1]);
        break;
    }
  }

  bool _matches(Map<String, dynamic> event, Map<String, dynamic> filter) {
    final kinds = (filter['kinds'] as List?)?.cast<int>();
    if (kinds != null && !kinds.contains(event['kind'])) return false;

    final authors = (filter['authors'] as List?)?.cast<String>();
    if (authors != null && !authors.contains(event['pubkey'])) return false;

    final since = filter['since'];
    if (since is int && (event['created_at'] as int) < since) return false;
    final until = filter['until'];
    if (until is int && (event['created_at'] as int) > until) return false;

    final ids = (filter['ids'] as List?)?.cast<String>();
    if (ids != null && !ids.contains(event['id'])) return false;

    // Tag filters: keys of the form "#<single-letter>" → list of allowed values.
    for (final entry in filter.entries) {
      if (!entry.key.startsWith('#')) continue;
      final tagName = entry.key.substring(1);
      final allowed = (entry.value as List).cast<String>();
      final eventTags = (event['tags'] as List).cast<List>();
      final hasMatch = eventTags.any(
        (t) => t.length >= 2 && t[0] == tagName && allowed.contains(t[1]),
      );
      if (!hasMatch) return false;
    }
    return true;
  }
}

/// Builds an [Ndk] wired to a single [MockRelay] with an in-memory cache.
Ndk ndkForRelay(MockRelay relay) {
  return Ndk(
    NdkConfig(
      eventVerifier: Bip340EventVerifier(),
      cache: MemCacheManager(),
      bootstrapRelays: [relay.url],
    ),
  );
}
