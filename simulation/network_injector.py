import asyncio
import random
from dataclasses import dataclass

from simulation import config as C


@dataclass(frozen=True)
class Condition:
    delay: str
    loss: str

    @property
    def name(self):
        return f"d{self.delay}_l{self.loss}"


def make_schedule(seed, n_blocks):
    r = random.Random(f"sched-{seed}")
    return [Condition(r.choice(C.LEVELS), r.choice(C.LEVELS)) for _ in range(n_blocks)]


class Injector:
    """Satu instance per node. Loss p berlaku PER ARAH."""

    def __init__(self, node_id, seed):
        self.rng = random.Random(f"{seed}-{node_id}")
        self.bytes = {"tcp": 0, "udp": 0}
        self.cond, self.p_loss = None, 0.0
        self.set_condition(Condition("low", "low"))

    def set_condition(self, cond):
        self.cond = cond
        lo, hi = C.LOSS_PCT[cond.loss]
        self.p_loss = self.rng.uniform(lo, hi) / 100.0

    def one_way_delay(self):
        lo, hi = C.RTT_MS[self.cond.delay]
        return self.rng.uniform(lo, hi) / 2000.0     # ms -> detik, RTT/2

    def is_lost(self):
        return self.rng.random() < self.p_loss

    def count(self, proto, n):
        self.bytes[proto] += n

    def reset_counters(self):
        self.bytes = {"tcp": 0, "udp": 0}


# ---------------- UDP (CoAP): datagram benar-benar dibuang ----------------
class UdpProxy(asyncio.DatagramProtocol):
    def __init__(self, inj, upstream):
        self.inj, self.upstream = inj, upstream
        self.transport = None
        self.backs = {}
        self.lock = asyncio.Lock()
        self.closed = False
        self.tasks = set()

    def spawn(self, coro):
        t = asyncio.ensure_future(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    def connection_made(self, transport):
        self.transport = transport

    def error_received(self, exc):
        pass

    def datagram_received(self, data, addr):                 # client -> server
        if self.closed:
            return
        self.inj.count("udp", len(data) + C.IP_UDP_HDR)
        self.spawn(self._forward(data, addr))

    async def _forward(self, data, addr):
        if self.inj.is_lost():
            return
        await asyncio.sleep(self.inj.one_way_delay())
        if self.closed:
            return
        async with self.lock:
            up = self.backs.get(addr)
            if up is None:
                loop = asyncio.get_running_loop()
                up, _ = await loop.create_datagram_endpoint(
                    lambda: _UdpBack(self, addr), remote_addr=self.upstream)
                self.backs[addr] = up
        if not self.closed:
            up.sendto(data)

    def close(self):
        self.closed = True
        for t in list(self.tasks):
            t.cancel()
        for t in self.backs.values():
            t.close()
        if self.transport:
            self.transport.close()


class _UdpBack(asyncio.DatagramProtocol):                    # server -> client
    def __init__(self, front, addr):
        self.front, self.addr = front, addr

    def error_received(self, exc):
        pass

    def datagram_received(self, data, _):
        if self.front.closed:
            return
        self.front.inj.count("udp", len(data) + C.IP_UDP_HDR)
        self.front.spawn(self._send(data))

    async def _send(self, data):
        if self.front.inj.is_lost():
            return
        await asyncio.sleep(self.front.inj.one_way_delay())
        if not self.front.closed:
            self.front.transport.sendto(data, self.addr)


# ---------------- TCP (MQTT): loss = penalti retransmisi, urutan tetap -----
async def _tcp_pipe(reader, writer, inj):
    loop = asyncio.get_running_loop()
    q = asyncio.Queue()

    async def sender():
        try:
            while (item := await q.get()) is not None:
                release, data = item
                await asyncio.sleep(max(0.0, release - loop.time()))
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    task, last = asyncio.create_task(sender()), 0.0
    try:
        while data := await reader.read(65536):
            d = inj.one_way_delay()
            extra, tries = 0.0, 0
            while tries < C.TCP_MAX_TRIES and inj.is_lost():
                extra += C.TCP_RTO_MIN * (2 ** tries)
                tries += 1
            inj.count("tcp", (len(data) + C.IP_TCP_HDR) * (1 + tries))
            last = max(last, loop.time() + d + extra)        # jaga urutan
            await q.put((last, data))
    except (ConnectionError, OSError):
        pass
    finally:
        await q.put(None)
        await task


async def _tcp_handle(cr, cw, inj, upstream):
    try:
        ur, uw = await asyncio.open_connection(*upstream)
    except OSError:
        cw.close()
        return
    await asyncio.gather(_tcp_pipe(cr, uw, inj), _tcp_pipe(ur, cw, inj),
                         return_exceptions=True)


class InjectorServer:
    def __init__(self, inj, tcp_port, udp_port):
        self.inj, self.tcp_port, self.udp_port = inj, tcp_port, udp_port

    async def start(self):
        up_tcp = (C.HOST, C.BROKER_PORT)
        self.tcp = await asyncio.start_server(
            lambda r, w: _tcp_handle(r, w, self.inj, up_tcp), C.HOST, self.tcp_port)
        loop = asyncio.get_running_loop()
        _, self.udp = await loop.create_datagram_endpoint(
            lambda: UdpProxy(self.inj, (C.HOST, C.COAP_SERVER_PORT)),
            local_addr=(C.HOST, self.udp_port))

    async def stop(self):
        self.tcp.close()
        self.udp.close()
        try:
            await asyncio.wait_for(self.tcp.wait_closed(), 2)
        except Exception:
            pass