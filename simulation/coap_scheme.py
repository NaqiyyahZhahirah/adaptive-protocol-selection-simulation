import asyncio
import time

import aiocoap

from simulation import config as C


class CoapSender:
    """CON: pool context (satu pertukaran aktif per context, sesuai NSTART=1
    aiocoap). NON: satu context sendiri karena tidak diantre."""

    def __init__(self, port, max_ctx=32):
        self.port, self.max_ctx = port, max_ctx
        self.free, self.all, self.waiters = [], [], []
        self.non_ctx = None

    async def start(self):
        self.non_ctx = await aiocoap.Context.create_client_context()

    async def _acquire(self):
        if self.free:
            return self.free.pop()
        if len(self.all) < self.max_ctx:
            ctx = await aiocoap.Context.create_client_context()
            self.all.append(ctx)
            return ctx
        fut = asyncio.get_running_loop().create_future()
        self.waiters.append(fut)
        return await fut

    def _release(self, ctx):
        while self.waiters:
            fut = self.waiters.pop(0)
            if not fut.done():
                fut.set_result(ctx)
                return
        self.free.append(ctx)

    async def send(self, msg, mode):                         # "CON" / "NON"
        rec = {"protocol": f"coap_{mode.lower()}", "t_send": time.perf_counter(),
               "t_ack": None, "acked": False}
        req = aiocoap.Message(code=aiocoap.POST,
                              mtype=aiocoap.CON if mode == "CON" else aiocoap.NON,
                              uri=f"coap://{C.HOST}:{self.port}/pm25",
                              payload=msg["payload"])
        ctx = self.non_ctx if mode == "NON" else await self._acquire()
        try:
            await asyncio.wait_for(ctx.request(req).response, C.COAP_WAIT[mode])
            rec.update(t_ack=time.perf_counter(), acked=True)
        except Exception:
            pass
        finally:
            if mode == "CON":
                self._release(ctx)
        return rec

    async def stop(self):
        for c in self.all + [self.non_ctx]:
            try:
                await c.shutdown()
            except Exception:
                pass