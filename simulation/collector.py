import asyncio
import json
import os
import time

import aiocoap
import aiocoap.resource as resource
import paho.mqtt.client as mqtt

from simulation import config as C


class Collector:
    """Mencatat waktu terima pertama tiap msg_id (duplikat diabaikan)."""

    def __init__(self):
        self.recv = {}
        self.client = None

    def on_payload(self, raw, via):
        t = time.perf_counter()
        try:
            mid = json.loads(raw)["m"]
        except Exception:
            return
        self.recv.setdefault(mid, (t, via))

    async def start_mqtt(self):
        loop = asyncio.get_running_loop()
        ready = loop.create_future()
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                        client_id=f"collector-{os.getpid()}", protocol=mqtt.MQTTv311)

        def on_connect(cl, ud, flags, rc, props=None):
            cl.subscribe(f"{C.MQTT_TOPIC}/#", qos=2)

        def on_subscribe(cl, ud, mid, *args):
            loop.call_soon_threadsafe(lambda: ready.done() or ready.set_result(True))

        c.on_connect, c.on_subscribe = on_connect, on_subscribe
        c.on_message = lambda cl, ud, m: self.on_payload(m.payload, "mqtt")
        c.connect_async(C.HOST, C.BROKER_PORT, keepalive=60)   # langsung ke broker
        c.loop_start()
        self.client = c
        await asyncio.wait_for(ready, 30)

    def stop(self):
        if self.client:
            self.client.disconnect()
            self.client.loop_stop()


class Pm25Resource(resource.Resource):
    def __init__(self, collector):
        super().__init__()
        self.collector = collector

    async def render_post(self, request):
        self.collector.on_payload(request.payload, "coap")
        return aiocoap.Message(code=aiocoap.CHANGED)


async def start_coap_server(collector, port):
    root = resource.Site()
    root.add_resource(["pm25"], Pm25Resource(collector))
    return await aiocoap.Context.create_server_context(root, bind=(C.HOST, port))