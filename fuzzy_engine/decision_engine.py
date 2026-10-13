import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl

RTT_MAX = 358.8             # P99 CAIDA
LOSS_MAX = 30.0             # batas atas Tabel 3.3
V_MAKS = 62.71291666666673  # P99 rata-rata 24 jam data uji

rtt = ctrl.Antecedent(np.linspace(0, RTT_MAX, 3589), "rtt_ms")
packet_loss = ctrl.Antecedent(np.linspace(0, LOSS_MAX, 301), "packet_loss_pct")
pm25 = ctrl.Antecedent(np.linspace(0, V_MAKS, 1001), "pm25_24h")
reliability = ctrl.Consequent(np.arange(0, 101, 1), "reliability_need")

# Tabel 3.4
rtt["low"] = fuzz.trimf(rtt.universe, [0, 0, 129.3])
rtt["medium"] = fuzz.trimf(rtt.universe, [58.4, 129.3, 218.6])
rtt["high"] = fuzz.trimf(rtt.universe, [129.3, RTT_MAX, RTT_MAX])

packet_loss["low"] = fuzz.trimf(packet_loss.universe, [0, 0, 10])
packet_loss["medium"] = fuzz.trimf(packet_loss.universe, [5, 12.5, 20])
packet_loss["high"] = fuzz.trimf(packet_loss.universe, [15, 30, 30])

pm25["normal"] = fuzz.trimf(pm25.universe, [0, 0, 30])
pm25["warning"] = fuzz.trimf(pm25.universe, [15, 32.5, 50])
pm25["critical"] = fuzz.trimf(pm25.universe, [35, V_MAKS, V_MAKS])

reliability["low"] = fuzz.trimf(reliability.universe, [0, 0, 50])
reliability["medium"] = fuzz.trimf(reliability.universe, [25, 50, 75])
reliability["high"] = fuzz.trimf(reliability.universe, [50, 100, 100])

# Tabel 3.5: baris = packet loss (low, medium, high), kolom = delay (low, medium, high)
LV = ["low", "medium", "high"]
RULE_TABLE = {
    "normal":   [["low", "low", "medium"],
                 ["low", "medium", "medium"],
                 ["medium", "medium", "high"]],
    "warning":  [["low", "medium", "medium"],
                 ["medium", "medium", "high"],
                 ["medium", "high", "high"]],
    "critical": [["medium", "high", "high"],
                 ["high", "high", "high"],
                 ["high", "high", "high"]],
}

rules = [
    ctrl.Rule(pm25[p] & packet_loss[LV[i]] & rtt[LV[j]], reliability[RULE_TABLE[p][i][j]])
    for p in RULE_TABLE for i in range(3) for j in range(3)
]
assert len(rules) == 27

decision_ctrl = ctrl.ControlSystem(rules)


def compute_reliability(rtt_ms, packet_loss_pct, pm25_24h):
    sim = ctrl.ControlSystemSimulation(decision_ctrl)
    sim.input["rtt_ms"] = float(np.clip(rtt_ms, 0, RTT_MAX))
    sim.input["packet_loss_pct"] = float(np.clip(packet_loss_pct, 0, LOSS_MAX))
    sim.input["pm25_24h"] = float(np.clip(pm25_24h, 0, V_MAKS))
    sim.compute()
    return sim.output["reliability_need"]


def map_to_protocol(score):
    """Tabel 3.6"""
    if score >= 65:
        return {"protocol": "MQTT", "qos": 2}
    if score >= 35:
        return {"protocol": "MQTT", "qos": 1}
    return {"protocol": "CoAP", "mode": "CON"}

_sim = ctrl.ControlSystemSimulation(decision_ctrl)

def compute_reliability(rtt_ms, packet_loss_pct, pm25_24h):
    _sim.input["rtt_ms"] = float(np.clip(rtt_ms, 0, RTT_MAX))
    _sim.input["packet_loss_pct"] = float(np.clip(packet_loss_pct, 0, LOSS_MAX))
    _sim.input["pm25_24h"] = float(np.clip(pm25_24h, 0, V_MAKS))
    _sim.compute()
    return _sim.output["reliability_need"]