import asyncio
from collections import deque

from fuzzy_engine.decision_engine import compute_reliability, map_to_protocol
from simulation import config as C


class AdaptiveController:
    """Satu instance per node. Estimasi jaringan dari ACK, diperbarui tiap
    WINDOW pesan; PM2.5 24 jam dievaluasi pada tiap pesan."""

    def __init__(self, mqtt_sender, coap_sender):
        self.mqtt, self.coap = mqtt_sender, coap_sender
        self.window = deque(maxlen=C.WINDOW)     # (rtt_ms|None, gagal, n_arah)
        self.n_sent = 0
        self.rtt_ms = self.loss_pct = None
        self.watchers = []

    def _refresh(self):
        obs = list(self.window)
        if not obs:
            return
        f = sum(1 for _, bad, _ in obs if bad) / len(obs)
        mean_dir = sum(n for _, _, n in obs) / len(obs)
        # gagal-percobaan-pertama per pertukaran -> loss per arah (Tabel 3.3)
        self.loss_pct = 100.0 * (1.0 - (1.0 - f) ** (1.0 / mean_dir))
        rtts = [r for r, bad, _ in obs if not bad and r is not None]
        self.rtt_ms = sum(rtts) / len(rtts) if rtts else None

    def decide(self, pm25_24h):
        if self.loss_pct is None:
            score = C.DEFAULT_SCORE                       # periode pertama
        else:
            rtt = self.rtt_ms if self.rtt_ms is not None else C.RTT_MS["high"][1]
            score = compute_reliability(rtt, self.loss_pct, pm25_24h)
        return score, map_to_protocol(score)

    async def _watch(self, inner, proto, qos):
        n_dir = 4 if (proto == "mqtt" and qos == 2) else 2
        done, _ = await asyncio.wait({inner}, timeout=C.FIRST_TRY_S[proto])
        if not done:
            self.window.append((None, True, n_dir))       # belum di-ACK = gagal
            return
        if inner.cancelled() or inner.exception() is not None:
            return
        rec = inner.result()
        if rec["acked"]:
            rtt = (rec["t_ack"] - rec["t_send"]) * 1000.0 / (n_dir / 2)
            self.window.append((rtt, False, n_dir))
        else:
            self.window.append((None, True, n_dir))

    async def send(self, msg):
        if self.n_sent and self.n_sent % C.WINDOW == 0:
            self._refresh()
        self.n_sent += 1
        score, dec = self.decide(msg["pm25_24h"])
        if dec["protocol"] == "MQTT":
            proto, qos = "mqtt", dec["qos"]
            inner = asyncio.ensure_future(self.mqtt.send(msg, qos))
        else:
            proto, qos = "coap", 0
            inner = asyncio.ensure_future(self.coap.send(msg, dec["mode"]))
        self.watchers.append(asyncio.create_task(self._watch(inner, proto, qos)))
        rec = await inner
        rec["score"] = round(score, 1)
        return rec