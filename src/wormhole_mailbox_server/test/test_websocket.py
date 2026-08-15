import json
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet.address import IPv4Address
from ..server_websocket import WebSocketServerFactory
from ..connections import ConnectionTable
from ..server import make_server
from ..database import create_channel_db
from autobahn.twisted.testing import create_pumper, create_memory_agent, MemoryReactorClock
from autobahn.twisted.websocket import WebSocketClientProtocol


class FakeServer:
    """
    Fake enough of the internal 'Server' object to appease the
    WebSocket server.

    """
    def __init__(self):
        self._connection_table = ConnectionTable(None)

    def get_welcome(self):
        return {
            "motd": "fake message of the day"
        }

    def get_log_requests(self):
        return False

    def get_address_id(self, peer_type, peer_host):
        return None

    def connection_established(self, address_id, now):
        c = self._connection_table.established(address_id, now)
        return c


class WebSocket(unittest.TestCase):
    """
    Details of the server WebSocket protocol
    """

    def setUp(self):
        self.pumper = create_pumper()
        self.reactor = MemoryReactorClock()
        return self.pumper.start()

    def tearDown(self):
        return self.pumper.stop()

    def create_server_protocol(self):
        """
        Used by the Agent to create the in-memory transport server-side
        WebSocket protocol (we actually create the 'real' protocol
        objects since that is what we're testing here.)
        """
        factory = WebSocketServerFactory(
            "ws://localhost:4000/v1",
            FakeServer(),
        )
        addr = IPv4Address("TCP", "localhost", 4000)
        return factory.buildProtocol(addr)

    @inlineCallbacks
    def test_server_version_string(self):
        """
        Our server version string from Autobahn should make sense
        """
        server_header = None
        agent = create_memory_agent(
            self.reactor,
            self.pumper,
            self.create_server_protocol
        )

        class FakeClient(WebSocketClientProtocol):
            def onConnect(self, cr):
                nonlocal server_header
                server_header = cr.headers.get("server")

        proto = yield agent.open("ws://localhost:4000/v1", dict(), FakeClient)
        proto.sendClose()
        yield proto.is_closed

        assert "Magic Wormhole" in server_header, "Incorrect Server: header sent"

    @inlineCallbacks
    def test_reflected_address(self):
        """
        The Welcome message should include our address information
        """
        welcome = None
        agent = create_memory_agent(
            self.reactor,
            self.pumper,
            self.create_server_protocol
        )

        class FakeClient(WebSocketClientProtocol):
            def onMessage(self, payload, isBinary):
                js = json.loads(payload)
                nonlocal welcome
                if welcome is None:
                    welcome = js.get("welcome", None)
                return super().onMessage(payload, isBinary)

        proto = yield agent.open("ws://localhost:4000/v1", dict(), FakeClient)
        proto.sendClose()
        yield proto.is_closed

        assert welcome is not None, "Failed to receive Welcome message"
        ya = welcome.get("your-address", None)
        assert ya, "Expected 'your-address' in Welcome message"
        assert ya["port"] == 31337
        assert "ipv4" in ya or "ipv6" in ya, "Expected either IPv4 or IPv6 address"

    @inlineCallbacks
    def test_reflected_caddy(self):
        """
        The Welcome message should include our address information
        when sent via x-real-ip headers
        """
        welcome = None

        headers = {
            "x-real-ip": "127.1.2.3",
            "x-real-port": "54321",
        }

        agent = create_memory_agent(
            self.reactor,
            self.pumper,
            self.create_server_protocol,
        )

        class FakeClient(WebSocketClientProtocol):
            def onMessage(self, payload, isBinary):
                js = json.loads(payload)
                nonlocal welcome
                if welcome is None:
                    welcome = js.get("welcome", None)
                return super().onMessage(payload, isBinary)

        proto = yield agent.open("ws://localhost:4000/v1", {"headers": headers}, FakeClient)
        proto.sendClose()
        yield proto.is_closed

        assert welcome is not None, "Failed to receive Welcome message"
        self.assertEqual(
            welcome["your-address"],
            {
                "ipv4": "127.1.2.3",
                "port": 54321,
            }
        )

    @inlineCallbacks
    def test_reflected_caddyv6(self):
        """
        The Welcome message should include our address information
        when sent via x-real-ip headers (IPv6 version)
        """
        welcome = None

        headers = {
            "x-real-ip": "::1",
            "x-real-port": "54321",
        }

        agent = create_memory_agent(
            self.reactor,
            self.pumper,
            self.create_server_protocol,
        )

        class FakeClient(WebSocketClientProtocol):
            def onMessage(self, payload, isBinary):
                js = json.loads(payload)
                nonlocal welcome
                if welcome is None:
                    welcome = js.get("welcome", None)
                return super().onMessage(payload, isBinary)

        proto = yield agent.open("ws://localhost:4000/v1", {"headers": headers}, FakeClient)
        proto.sendClose()
        yield proto.is_closed

        assert welcome is not None, "Failed to receive Welcome message"
        self.assertEqual(
            welcome["your-address"],
            {
                "ipv6": "::1",
                "port": 54321,
            }
        )


class FakeClient(WebSocketClientProtocol):
    """
    A client that collects messages and allows tests to interact
    as they see fit.
    """
    # FIXME: half of this is just "Next" from fowl.util .. can we
    # export that utility somehow?

    def __init__(self, *args, **kw):
        self._messages = []
        self._awaiters = []
        super().__init__(*args, **kw)

    def next_message(self):
        if not self._messages:
            d = Deferred()
            self._awaiters.append(d)
        else:
            result = self._messages.pop(0)
            d = Deferred()
            if 'error' in result:
                d.errback(RuntimeError(result['error']))
            else:
                d.callback(result)
        return d

    @inlineCallbacks
    def wait_for(self, message_type, cleanup_on_error=True):
        """
        Wait for a specific 'type' key in a message, ignorning all others.
        If a type=error message arrives, this will errback
        """
        while True:
            try:
                msg = yield self.next_message()
            except Exception:
                if cleanup_on_error:
                    self.sendClose()
                    yield self.is_closed
                raise
            if msg['type'] == message_type:
                return msg
        # cannot reach

    def onMessage(self, payload, isBinary):
        print("<   ", payload)
        msg = json.loads(payload)
        if self._awaiters:
            notify = self._awaiters
            self._awaiters = []
            for d in notify:
                if 'error' in msg:
                    d.errback(RuntimeError(msg['error']))
                else:
                    d.callback(msg)
        else:
            self._messages.append(msg)
        return super().onMessage(payload, isBinary)


class NameplateCrowded(unittest.TestCase):
    """
    We don't always want additional messages to keep a nameplate alive.

    For example, repeated CLAIMs on an already-crowded nameplate
    should not continue to retain it forever.

    This suite uses a real server object.
    """

    def setUp(self):
        self.pumper = create_pumper()
        self.reactor = MemoryReactorClock()
        self.db = create_channel_db(":memory:")
        self.server = make_server(self.db)
        return self.pumper.start()

    def tearDown(self):
        return self.pumper.stop()

    def create_server_protocol(self):
        """
        Used by the Agent to create the in-memory transport server-side
        WebSocket protocol (we actually create the 'real' protocol
        objects since that is what we're testing here).
        """
        factory = WebSocketServerFactory(
            "ws://localhost:4000/v1",
            self.server,
        )
        factory.reactor = self.reactor
        addr = IPv4Address("TCP", "localhost", 4000)
        return factory.buildProtocol(addr)

    @inlineCallbacks
    def create_proto(self, agent, side, client, nameplate=None, cleanup_on_error=True):
        proto = yield agent.open("ws://localhost:4000/v1", {}, lambda: client)

        orig_send = proto.sendMessage

        def logSendMessage(payload):
            print("   >", payload)
            return orig_send(payload)
        proto.sendMessage = logSendMessage

        proto.sendMessage(
            json.dumps({
                "type": "bind",
                "appid": "test",
                "side": side,
            }).encode("utf8")
        )
        if not nameplate:
            proto.sendMessage(
                json.dumps({
                    "type": "allocate",
                    "appid": "test",
                    "side": side,
                }).encode("utf8")
            )

            msg = yield proto.wait_for("allocated", cleanup_on_error)
            nameplate = msg["nameplate"]

        proto.sendMessage(
            json.dumps({
                "type": "claim",
                "appid": "test",
                "side": side,
                "nameplate": nameplate,
            }).encode("utf8")
        )

        yield proto.wait_for("claimed", cleanup_on_error)
        return proto, nameplate


    @inlineCallbacks
    def test_crowded(self):
        """
        A nameplate with 3 sides is CROWDED
        """

        agent = create_memory_agent(
            self.reactor,
            self.pumper,
            self.create_server_protocol,
        )

        proto0, nameplate = yield self.create_proto(agent, "one", FakeClient())
        proto1, _ = yield self.create_proto(agent, "two", FakeClient(), nameplate)

        # we have two sides now, give some time gab (more obvious debugging)
        self.reactor.advance(123)

        with self.assertRaises(RuntimeError):
            proto2, _ = yield self.create_proto(agent, "three", FakeClient(), nameplate)

        app = self.server.get_app("test")
        before = app.get_nameplate_ids()
        app.prune(self.reactor.seconds(), self.reactor.seconds() - 42)
        after = app.get_nameplate_ids()

        assert len(before) == 1, "should be one active nameplate before prune()"
        assert after == set(), "prune() should remove the CROWDED nameplate"

        for p in (proto0, proto1):
            p.sendClose()
            yield p.is_closed
