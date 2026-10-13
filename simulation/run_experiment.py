import argparse
import asyncio
import csv
import math
import os
import socket
import subprocess
import sys
import time

from dotenv import load_dotenv

from simulation import config as C
from simulation.adaptive_scheme import AdaptiveController
from simulation.coap_scheme import CoapSender
from simulation.collector import Collector, start_coap_server
from simulation.mqtt_scheme import MqttSender
from simulation.network_injector import Condition, Injector, InjectorServer, make_schedule
from simulation.node_simulator import build_messages, slug

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CHAR_SCENARIOS = ["mqtt_qos0", "mqtt_qos1", "mqtt_qos2", "coap_non", "coap_con"]
COMP_SCENARIOS = ["mqtt_qos1", "coap_con", "adaptive"]
STATIC = [Condition(d, l).name for d in C.LEVELS for l in C.LEVELS]


class ConditionDriver:
    """Kondisi tetap (statis) atau berganti tiap 60 pesan global (dinamis)."""

    def __init__(self, injectors, cond_name, seed, n_msgs):
        self.inj, self.k, self.block, self.cur = injectors, 0, -1, None
        self.dynamic = cond_name == "dynamic"
        if self.dynamic:
            self.schedule = make_schedule(seed, math.ceil(n_msgs / C.DYNAMIC_BLOCK))
        else:
            d, l = cond_name.split("_")
            self.fixed = Condition(d[1:], l[1:])

    def tick(self):
        blk = self.k // C.DYNAMIC_BLOCK if self.dynamic else 0
        if blk != self.block:
            self.cur = self.schedule[blk] if self.dynamic else self.fixed
            for i in self.inj:
                i.set_condition(self.cur)
            self.block = blk
        self.k += 1
        return self.cur.name


def make_sender(scenario, i, mq, co, ctl):
    if scenario.startswith("mqtt_qos"):
        qos = int(scenario[-1])
        return lambda m: mq[i].send(m, qos)
    if scenario == "coap_con":
        return lambda m: co[i].send(m, "CON")
    if scenario == "coap_non":
        return lambda m: co[i].send(m, "NON")
    return ctl[i].send


async def run_one(scenario, cond_name, seed, messages, run_id):
    nodes = C.NODES
    n_msgs = sum(len(messages[n]) for n in nodes)
    injectors = [Injector(slug(n), seed) for n in nodes]
    servers = [InjectorServer(inj, C.TCP_PORT_BASE + i, C.UDP_PORT_BASE + i)
               for i, inj in enumerate(injectors)]
    collector, coap_srv, mq, co, ctl = Collector(), None, [], [], []
    try:
        for s in servers:
            await s.start()
        coap_srv = await start_coap_server(collector, C.COAP_SERVER_PORT)
        await collector.start_mqtt()
        need_mqtt = scenario.startswith("mqtt") or scenario == "adaptive"
        need_coap = scenario.startswith("coap") or scenario == "adaptive"
        for i, n in enumerate(nodes):
            if need_mqtt:
                m = MqttSender(slug(n), C.TCP_PORT_BASE + i, run_id[-24:])
                await m.start()
                mq.append(m)
            if need_coap:
                c = CoapSender(C.UDP_PORT_BASE + i)
                await c.start()
                co.append(c)
        if scenario == "adaptive":
            ctl = [AdaptiveController(mq[i], co[i]) for i in range(len(nodes))]

        driver = ConditionDriver(injectors, cond_name, seed, n_msgs)
        senders = [make_sender(scenario, i, mq, co, ctl) for i in range(len(nodes))]
        for inj in injectors:
            inj.reset_counters()                     # abaikan byte setup koneksi

        results, sent_at, cond_of, tasks = {}, {}, {}, []

        async def send_one(i, m, cname):
            sent_at[m["msg_id"]], cond_of[m["msg_id"]] = time.perf_counter(), cname
            results[m["msg_id"]] = await senders[i](m)

        async def node_loop(i):
            for m in messages[nodes[i]]:
                cname = driver.tick()
                cond_of[m["msg_id"]] = cname
                tasks.append(asyncio.create_task(send_one(i, m, cname)))
                await asyncio.sleep(C.SEND_INTERVAL)

        t0 = time.perf_counter()
        await asyncio.gather(*(node_loop(i) for i in range(len(nodes))))
        _, pending = await asyncio.wait(tasks, timeout=C.DRAIN_TIMEOUT)
        for p in pending:
            p.cancel()
        await asyncio.sleep(0.5)
        duration = time.perf_counter() - t0

        rows, n_recv, payload_total = [], 0, 0
        for n in nodes:
            for m in messages[n]:
                mid, rec = m["msg_id"], results.get(m["msg_id"], {})
                got, ts = collector.recv.get(mid), sent_at.get(mid)
                ok = got is not None and ts is not None
                n_recv += ok
                payload_total += len(m["payload"])
                rows.append({
                    "run_id": run_id, "scenario": scenario, "condition": cond_of.get(mid, ""),
                    "seed": seed, "node": n, "msg_id": mid, "seq": m["seq"],
                    "category": m["category"], "pm25_hourly": m["pm25_hourly"],
                    "pm25_24h": round(m["pm25_24h"], 2),
                    "protocol": rec.get("protocol", "unknown"), "score": rec.get("score", ""),
                    "acked": int(rec.get("acked", False)), "success": int(ok),
                    "t_send": round(ts - t0, 4) if ts else "",
                    "latency_ms": round((got[0] - ts) * 1000, 2) if ok else "",
                    "payload_bytes": len(m["payload"])})
        wire_tcp = sum(i.bytes["tcp"] for i in injectors)
        wire_udp = sum(i.bytes["udp"] for i in injectors)
        summary = {"run_id": run_id, "scenario": scenario, "condition": cond_name,
                   "seed": seed, "n_sent": len(rows), "n_recv": n_recv,
                   "payload_bytes_total": payload_total, "wire_tcp": wire_tcp,
                   "wire_udp": wire_udp, "wire_total": wire_tcp + wire_udp,
                   "duration_s": round(duration, 1)}
        return rows, summary
    finally:
        for s in mq:
            await s.stop()
        for s in co:
            await s.stop()
        collector.stop()
        if coap_srv:
            await coap_srv.shutdown()
        for s in servers:
            await s.stop()


def append_csv(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if new:
            w.writeheader()
        w.writerows(rows)


def done_runs(path):
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {r["run_id"] for r in csv.DictReader(f)}


def build_plan(mode, seeds):
    if mode == "smoke":
        return [("mqtt_qos1", "dlow_llow", 1), ("coap_con", "dlow_llow", 1),
                ("adaptive", "dlow_llow", 1)]
    scenarios = CHAR_SCENARIOS if mode == "char" else COMP_SCENARIOS
    conds = STATIC if mode == "char" else STATIC + ["dynamic"]
    return [(sc, cd, sd) for sd in seeds for cd in conds for sc in scenarios]


async def run_batch(mode, seeds):
    messages = build_messages(limit=12 if mode == "smoke" else None)
    out = C.RESULTS_DIR / {"smoke": "smoke", "char": "characterization",
                           "comp": "comparative"}[mode]
    finished = done_runs(out / "runs.csv")
    plan = build_plan(mode, seeds)
    for k, (sc, cd, sd) in enumerate(plan, 1):
        run_id = f"{mode}-{sc}-{cd}-s{sd}"
        if run_id in finished:
            continue
        t = time.perf_counter()
        rows, summ = await run_one(sc, cd, sd, messages, run_id)
        append_csv(out / "messages.csv", rows)
        append_csv(out / "runs.csv", [summ])
        print(f"[{k}/{len(plan)}] {run_id}: PDR={100*summ['n_recv']/summ['n_sent']:.1f}% "
              f"wire={summ['wire_total']}B ({time.perf_counter()-t:.0f}s)", flush=True)
        await asyncio.sleep(0.5)


def broker_alive():
    try:
        socket.create_connection((C.HOST, C.BROKER_PORT), timeout=2).close()
        return True
    except OSError:
        return False


def main():
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["smoke", "char", "comp"])
    ap.add_argument("--seeds", type=int, nargs="*", default=C.SEEDS)
    args = ap.parse_args()
    broker = None
    if not broker_alive() and os.getenv("MOSQUITTO_PATH"):
        broker = subprocess.Popen([os.getenv("MOSQUITTO_PATH"), "-c", str(C.BROKER_CONF)])
        time.sleep(1.5)
    if not broker_alive():
        sys.exit(f"Broker tidak aktif di port {C.BROKER_PORT}. Jalankan Mosquitto dulu.")
    try:
        asyncio.run(run_batch(args.mode, args.seeds))
    finally:
        if broker:
            broker.terminate()


if __name__ == "__main__":
    main()