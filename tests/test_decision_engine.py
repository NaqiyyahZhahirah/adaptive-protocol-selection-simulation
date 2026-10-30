import unittest

from fuzzy_engine.decision_engine import map_to_protocol


class MapToProtocolTests(unittest.TestCase):
    def test_scores_below_35_select_coap_con(self):
        for score in (0, 20, 34.99):
            with self.subTest(score=score):
                self.assertEqual(
                    map_to_protocol(score),
                    {"protocol": "CoAP", "mode": "CON"},
                )

    def test_score_35_and_above_select_mqtt_qos1(self):
        for score in (35, 50, 65, 100):
            with self.subTest(score=score):
                self.assertEqual(
                    map_to_protocol(score),
                    {"protocol": "MQTT", "qos": 1},
                )


if __name__ == "__main__":
    unittest.main()
