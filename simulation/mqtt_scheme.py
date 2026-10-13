import asyncio
import threading
import time

import paho.mqtt.client as mqtt

from simulation import config as C


class MqttSender:
    """Publisher satu node, terhubung lewat port injector."""

    def __init__(self, node_id, port, tag):
        self.node_id, self.port = node_id, port
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=f"pub-{tag}-{node_id}"[:60],
                                  protocol=mqtt.MQTTv311)
        self._lock = threading.Lock()
        self._pending, self._early = {}, {}
        self._loop = None

    async def start(self):
        self._loop = asyncio.get_running_loop()
        connected = self._loop.create_future()

        def on_connect(client, userdata, flags, reason_code, properties=None):
            self._loop.call_soon_threadsafe(
                lambda: connected.done() or connected.set_result(True))

        self.client.on_connect = on_connect
        self.client.on_publish = self._on_publish
        self.client.max_inflight_messages_set(1000)
        self.client.max_queued_messages_set(0)
        self.client.connect_async(C.HOST, self.port, keepalive=60)
        self.client.loop_start()
        await asyncio.wait_for(connected, 30)

    def _on_publish(self, client, userdata, mid, *args):
        t = time.perf_counter()
        with self._lock:
            fut = self._pending.pop(mid, None)
            if fut is None:
                self._early[mid] = t
                return
        self._loop.call_soon_threadsafe(self._resolve, fut, t)

    @staticmethod
    def _resolve(fut, t):
        if not fut.done():
            fut.set_result(t)

    async def send(self, msg, qos):
        rec = {"protocol": f"mqtt_qos{qos}", "t_send": time.perf_counter(),
               "t_ack": None, "acked": False}
        fut = self._loop.create_future()
        info = self.client.publish(f"{C.MQTT_TOPIC}/{self.node_id}",
                                   msg["payload"], qos=qos)
        if qos == 0 or info.rc != mqtt.MQTT_ERR_SUCCESS:
            return rec
        with self._lock:
            t_early = self._early.pop(info.mid, None)
            if t_early is None:
                self._pending[info.mid] = fut
        if t_early is not None:
            rec.update(t_ack=t_early, acked=True)
            return rec
        try:
            rec["t_ack"] = await asyncio.wait_for(fut, C.MQTT_ACK_WAIT)
            rec["acked"] = True
        except asyncio.TimeoutError:
            with self._lock:
                self._pending.pop(info.mid, None)
        return rec

    async def stop(self):
        try:
            self.client.disconnect()
            self.client.loop_stop()
        except Exception:
            pass