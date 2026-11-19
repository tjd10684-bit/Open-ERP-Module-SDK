from __future__ import annotations

from datetime import datetime, timezone
import json
import queue
import threading

import pytest

from open_erp_module import Equipment, Module, ModuleError, Reading, Theme, serve


class Gauge(Module):
    module_id = "sample-gauge"
    name = "Sample Gauge"
    version = "1.2.0"

    def __init__(self, reading=None):
        self.contexts = []
        self.cancelled = []
        self.shutdowns = 0
        self.reading = reading or (lambda request: Reading(2.5, request.units, "sample:1"))

    def initialize(self, context):
        self.contexts.append(context)
        self.report_status("ready", "Sample ready")

    def linked_assets(self):
        return ["A1", "A2", "A1"]

    def capture(self, request):
        return self.reading(request)

    def cancel(self, capture_id):
        self.cancelled.append(capture_id)

    def shutdown(self):
        self.shutdowns += 1


class FakeHost:
    """Stands in for Open ERP: it is the module's stdin and stdout."""

    def __init__(self):
        self.inbox: queue.Queue = queue.Queue()
        self.outbox: queue.Queue = queue.Queue()
        self.notifications: list[dict] = []
        self.next_id = 1

    def readline(self, _limit=-1):
        return self.inbox.get()

    def write(self, data):
        self.outbox.put(json.loads(data))

    def flush(self):
        pass

    def send(self, method, params=None, *, notify=False):
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        if not notify:
            message["id"] = self.next_id
            self.next_id += 1
        self.inbox.put(json.dumps(message).encode() + b"\n")
        return message.get("id")

    def answer(self, request_id):
        while True:
            message = self.outbox.get(timeout=5)
            if "method" in message:
                self.notifications.append(message)
            elif message.get("id") == request_id:
                return message

    def request(self, method, params=None):
        return self.answer(self.send(method, params))


def converse(module, script):
    """Serve ``module`` on this (the main) thread while ``script(host)`` plays Open ERP on another."""
    host, outcome = FakeHost(), {}

    def drive():
        try:
            outcome["value"] = script(host)
        except BaseException as error:  # noqa: BLE001 - re-raised on the test thread.
            outcome["error"] = error
        finally:
            host.inbox.put(b"")

    thread = threading.Thread(target=drive, daemon=True)
    thread.start()
    assert serve(module, host, host) == 0
    thread.join(5)
    if "error" in outcome:
        raise outcome["error"]
    return host, outcome.get("value")


def hello(tmp_path):
    return {
        "protocol_version": 1, "host": {"name": "Open ERP", "version": "0.2.10"},
        "module_id": "sample-gauge", "data_dir": str(tmp_path),
    }


def capture(asset_id, capture_id="c1", **changes):
    return {
        "capture_id": capture_id, "asset_id": asset_id, "units": "mm", "input_name": "measurement",
        "requested_at": datetime.now(timezone.utc).isoformat(), "timeout_seconds": 5, **changes,
    }


def test_handshake_linked_assets_and_capture_answer_with_the_protocol_fields(tmp_path):
    module = Gauge()
    before = datetime.now(timezone.utc)
    host, (started, linked, reading, stopped) = converse(module, lambda host: (
        host.request("initialize", hello(tmp_path)),
        host.request("linked_assets"),
        host.request("capture", capture("A1")),
        host.request("shutdown"),
    ))

    assert started["result"] == {
        "protocol_version": 1, "module": {"id": "sample-gauge", "name": "Sample Gauge", "version": "1.2.0"},
        "acquisition_mode": "latest", "capabilities": {"setup": False},
    }
    context = module.contexts[0]
    assert (context.data_dir, context.host_name, context.host_version) == (tmp_path, "Open ERP", "0.2.10")
    assert host.notifications == [
        {"jsonrpc": "2.0", "method": "status", "params": {"state": "ready", "message": "Sample ready"}}
    ]
    assert linked["result"] == {"asset_ids": ["A1", "A2"]}
    result = reading["result"]
    received = datetime.fromisoformat(result.pop("received_at"))
    assert result == {
        "capture_id": "c1", "asset_id": "A1", "value": 2.5, "units": "mm", "source_id": "sample:1",
        "acquisition_mode": "latest", "sampled_at": None,
    }
    assert before <= received <= datetime.now(timezone.utc)
    assert stopped["result"] == {}
    assert module.shutdowns == 1


def test_requests_are_refused_before_initialize_and_after_shutdown(tmp_path):
    module = Gauge()
    _host, (early, _started, _stopped, late, again) = converse(module, lambda host: (
        host.request("capture", capture("A1")),
        host.request("initialize", hello(tmp_path)),
        host.request("shutdown"),
        host.request("linked_assets"),
        host.request("shutdown"),
    ))

    assert early["error"]["code"] == -32002
    assert late["error"] == {"code": 1000, "message": "The module is shutting down."}
    assert again["result"] == {}
    assert module.shutdowns == 1


def test_module_errors_and_bad_readings_are_answered_with_errors(tmp_path):
    def reading(request):
        if request.asset_id == "NOT-LINKED":
            raise ModuleError("This asset is not linked.", 1001)
        if request.asset_id == "NAN":
            return Reading(float("nan"), "mm", "sample:1")
        return Reading(1.0, "cm", "sample:1")

    def script(host):
        host.request("initialize", hello(tmp_path))
        answers = [
            host.request("capture", capture("NOT-LINKED")),
            host.request("capture", capture("NAN")),
            host.request("capture", capture("CM")),
            host.request("capture", capture("A1", input_name="")),
            host.request("calibrate"),
        ]
        host.inbox.put(b"{not json\n")
        answers.append(host.answer(None))
        answers.append(host.request("linked_assets"))
        return answers

    _host, answers = converse(Gauge(reading), script)

    *errors, linked = answers
    assert [answer["error"]["code"] for answer in errors] == [1001, -32603, -32603, -32602, -32601, -32700]
    assert errors[0]["error"]["message"] == "This asset is not linked."
    assert "finite number" in errors[1]["error"]["message"]
    assert "mm, in" in errors[2]["error"]["message"]
    assert linked["result"] == {"asset_ids": ["A1", "A2"]}


def test_cancel_sets_the_request_event_and_fails_the_capture(tmp_path):
    def reading(request):
        assert request.cancelled.wait(5)
        return Reading(1.0, "mm", "sample:1")

    module = Gauge(reading)

    def script(host):
        host.request("initialize", hello(tmp_path))
        capture_id = host.send("capture", capture("A1", capture_id="slow"))
        host.send("cancel", {"capture_id": "slow"}, notify=True)
        return host.answer(capture_id)

    _host, answer = converse(module, script)

    assert answer["error"] == {"code": 1000, "message": "The capture was cancelled."}
    assert module.cancelled == ["slow"]


def test_open_setup_runs_on_the_thread_that_called_serve(tmp_path):
    class SetupGauge(Gauge):
        def open_setup(self, equipment):
            self.setup = (threading.current_thread() is threading.main_thread(), equipment)

    module = SetupGauge()
    _host, (started, setup) = converse(module, lambda host: (
        host.request("initialize", hello(tmp_path)),
        host.request("open_setup", {"equipment": [
            {"asset_id": "A1", "tag": "G-1", "name": "Caliper", "category": "Calipers"},
        ]}),
    ))

    assert started["result"]["capabilities"] == {"setup": True}
    assert setup["result"] == {}
    assert module.setup == (True, [Equipment("A1", "G-1", "Caliper", "Calipers")])


def setup_with(tmp_path, *themes):
    """The ``host_theme`` the module saw in one ``open_setup`` per entry of ``themes`` (``...`` sends none)."""
    class SetupGauge(Gauge):
        def __init__(self):
            super().__init__()
            self.seen = []

        def open_setup(self, equipment):
            self.seen.append(self.host_theme)

    module = SetupGauge()

    def play(host):
        host.request("initialize", hello(tmp_path))
        for theme in themes:
            params = {"equipment": []} if theme is ... else {"equipment": [], "theme": theme}
            assert host.request("open_setup", params)["result"] == {}

    converse(module, play)
    return module.seen


def test_open_setup_gives_the_module_the_hosts_theme(tmp_path):
    sent = {"version": 1, "mode": "dark", "colors": {"background": "#202124", "text": "#E6E6E6"}}

    assert setup_with(tmp_path, sent) == [Theme("dark", {"background": "#202124", "text": "#e6e6e6"})]


def test_a_host_that_sends_no_theme_leaves_it_unset_and_each_setup_replaces_the_last(tmp_path):
    sent = {"version": 1, "mode": "light", "colors": {"accent": "#245b75"}}

    assert setup_with(tmp_path, ...) == [None]
    assert setup_with(tmp_path, sent, ...) == [Theme("light", {"accent": "#245b75"}), None]


@pytest.mark.parametrize("theme", [
    "dark", ["dark"], {}, {"mode": "dark"}, {"version": 2, "mode": "dark"},
    {"version": 1, "mode": "sepia"}, {"version": True, "mode": "dark"},
])
def test_a_theme_the_library_cannot_use_is_dropped_and_setup_still_runs(tmp_path, theme):
    assert setup_with(tmp_path, theme) == [None]


def test_only_known_roles_with_hex_colours_are_kept(tmp_path):
    colors = {
        "background": "#202124", "text": "red", "accent": "#12345", "ready": "#12345g", "error": 7,
        "neutral": "#aabbcc", "gradient": "#000000", "field": "#2b2d31ff",
    }

    [theme] = setup_with(tmp_path, {"version": 1, "mode": "dark", "colors": colors})

    assert theme == Theme("dark", {"background": "#202124", "neutral": "#aabbcc"})


def test_a_theme_with_no_usable_colours_still_tells_the_module_the_mode(tmp_path):
    assert setup_with(tmp_path, {"version": 1, "mode": "light", "colors": "none"}) == [Theme("light", {})]
