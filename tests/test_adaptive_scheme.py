import asyncio
import time
import unittest

from simulation import config as C
from simulation.adaptive_scheme import AdaptiveController


class _MqttSender:
    async def send(self, msg, qos):
        sent = time.perf_counter()
        return {
            "protocol": f"mqtt_qos{qos}",
            "t_send": sent,
            "t_ack": sent + 0.01,
            "acked": True,
        }


class _CoapSender:
    async def send(self, msg, mode):
        sent = time.perf_counter()
        return {
            "protocol": f"coap_{mode.lower()}",
            "t_send": sent,
            "t_ack": sent + 0.01,
            "acked": True,
        }


class AdaptiveControllerRefreshTests(unittest.TestCase):
    def setUp(self):
        self.controller = AdaptiveController(_MqttSender(), _CoapSender())

    def test_empty_window_keeps_estimates_unset(self):
        self.controller._refresh()

        self.assertIsNone(self.controller.rtt_ms)
        self.assertIsNone(self.controller.loss_pct)

    def test_successful_samples_update_mean_rtt(self):
        self.controller.window.extend([
            (20.0, False, 2),
            (40.0, False, 2),
            (None, False, 2),
        ])

        self.controller._refresh()

        self.assertEqual(self.controller.rtt_ms, 30.0)
        self.assertEqual(self.controller.loss_pct, 0.0)

    def test_failure_only_window_has_no_rtt_sample(self):
        self.controller.window.extend([
            (None, True, 2),
            (None, True, 4),
        ])

        self.controller._refresh()

        self.assertIsNone(self.controller.rtt_ms)
        self.assertEqual(self.controller.loss_pct, 100.0)

    def test_repeated_refresh_replaces_estimates(self):
        self.controller.window.append((20.0, False, 2))
        self.controller._refresh()
        self.assertEqual(self.controller.rtt_ms, 20.0)

        self.controller.window.clear()
        self.controller.window.append((None, True, 2))
        self.controller._refresh()

        self.assertIsNone(self.controller.rtt_ms)
        self.assertEqual(self.controller.loss_pct, 100.0)

    def test_send_crosses_first_and_second_window(self):
        async def send_messages():
            controller = AdaptiveController(_MqttSender(), _CoapSender())
            for _ in range(2 * C.WINDOW + 1):
                await controller.send({"pm25_24h": 25.0, "payload": b"test"})
                await asyncio.sleep(0)
            await asyncio.gather(*controller.watchers)
            return controller

        controller = asyncio.run(send_messages())

        self.assertEqual(controller.n_sent, 2 * C.WINDOW + 1)
        self.assertAlmostEqual(controller.rtt_ms, 10.0, places=6)


if __name__ == "__main__":
    unittest.main()
