
import { DurableObject } from "cloudflare:workers";

export class DhanLive extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
    this.upstream = null;
    this.activeKey = null;
  }

  async fetch(request) {
    const url = new URL(request.url);

    const token = this.env.DHAN_ACCESS_TOKEN;
    const clientId = this.env.DHAN_CLIENT_ID;

    if (!token || !clientId) {
      return Response.json({
        ok: false,
        status: "missing_credentials"
      }, { status: 500 });
    }

    const securityId = url.searchParams.get("securityId");
    const segment = url.searchParams.get("segment") || "NSE_FNO";

    if (!securityId || !/^\d+$/.test(securityId)) {
      return Response.json({
        ok: false,
        status: "security_id_required",
        message: "Send /live?segment=NSE_FNO&securityId=YOUR_SECURITY_ID"
      }, { status: 400 });
    }

    if (!["NSE_FNO", "BSE_FNO"].includes(segment)) {
      return Response.json({
        ok: false,
        status: "invalid_segment"
      }, { status: 400 });
    }

    const key = `${segment}:${securityId}`;

    if (
      this.upstream &&
      this.upstream.readyState === WebSocket.OPEN &&
      this.activeKey === key
    ) {
      const saved = await this.ctx.storage.get("feedState");
      return Response.json({
        ok: true,
        status: "connected",
        marketFeed: "connected",
        lastTick: saved?.lastTick || null
      });
    }

    try {
      if (this.upstream) {
        try {
          this.upstream.close();
        } catch {}
      }

      const feedUrl = new URL("wss://api-feed.dhan.co");
      feedUrl.searchParams.set("version", "2");
      feedUrl.searchParams.set("token", token);
      feedUrl.searchParams.set("clientId", clientId);
      feedUrl.searchParams.set("authType", "2");

      const response = await fetch(feedUrl.toString(), {
        headers: {
          Upgrade: "websocket"
        }
      });

      const ws = response.webSocket;

      if (!ws) {
        return Response.json({
          ok: false,
          status: "websocket_connection_failed",
          httpStatus: response.status
        }, { status: 502 });
      }

      ws.accept();
      ws.binaryType = "arraybuffer";

      this.upstream = ws;
      this.activeKey = key;

      ws.addEventListener("message", async (event) => {
        try {
          if (!(event.data instanceof ArrayBuffer)) return;

          const data = new DataView(event.data);

          if (data.byteLength < 17) return;

          const responseCode = data.getUint8(0);
          const receivedSecurityId = data.getInt32(4, true);

          if (responseCode === 2) {
            const ltp = data.getFloat32(8, true);
            const tradeTime = data.getInt32(12, true);

            if (!Number.isFinite(ltp) || ltp <= 0) return;

            const tick = {
              securityId: String(receivedSecurityId),
              segment,
              ltp,
              tradeTime,
              receivedAt: new Date().toISOString()
            };

            await this.ctx.storage.put("feedState", {
              status: "connected",
              lastTick: tick
            });
          }

          if (responseCode === 50) {
            await this.ctx.storage.put("feedState", {
              status: "disconnected",
              lastTick: null
            });
          }
        } catch (error) {
          console.log("Dhan packet processing error");
        }
      });

      ws.addEventListener("close", async () => {
        await this.ctx.storage.put("feedState", {
          status: "disconnected",
          lastTick: null
        });
        this.upstream = null;
        this.activeKey = null;
      });

      ws.addEventListener("error", async () => {
        await this.ctx.storage.put("feedState", {
          status: "error",
          lastTick: null
        });
      });

      ws.send(JSON.stringify({
        RequestCode: 15,
        InstrumentCount: 1,
        InstrumentList: [
          {
            ExchangeSegment: segment,
            SecurityId: securityId
          }
        ]
      }));

      await this.ctx.storage.put("feedState", {
        status: "connected_waiting_for_tick",
        lastTick: null
      });

      return Response.json({
        ok: true,
        status: "connected_waiting_for_tick",
        marketFeed: "connected",
        securityId,
        segment,
        message: "Waiting for first live market tick"
      });

    } catch (error) {
      return Response.json({
        ok: false,
        status: "connection_error",
        message: "Dhan feed connection could not be established"
      }, { status: 502 });
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({
        ok: true,
        service: "Dhan Backend"
      });
    }

    if (url.pathname === "/live") {
      const id = env.DHAN_LIVE.idFromName("main");
      const stub = env.DHAN_LIVE.get(id);
      return stub.fetch(request);
    }

    return new Response("Dhan Backend Ready");
  }
};
