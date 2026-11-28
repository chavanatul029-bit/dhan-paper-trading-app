import { DurableObject } from "cloudflare:workers";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "https://chavanatul029-bit.github.io",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400"
};

function corsResponse(response) {
  const headers = new Headers(response.headers);
  for (const [key, value] of Object.entries(CORS_HEADERS)) headers.set(key, value);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}

// Dhan TOTP secret is Base32 (the key shown when TOTP was configured).
function base32ToBytes(input) {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  const clean = String(input || "").toUpperCase().replace(/\s|=/g, "");
  if (!clean || /[^A-Z2-7]/.test(clean)) throw new Error("invalid_totp_secret");

  let bits = "";
  for (const char of clean) {
    const value = alphabet.indexOf(char);
    if (value < 0) throw new Error("invalid_totp_secret");
    bits += value.toString(2).padStart(5, "0");
  }

  const bytes = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) {
    bytes.push(parseInt(bits.slice(i, i + 8), 2));
  }
  return new Uint8Array(bytes);
}

async function generateTotp(secret) {
  const keyBytes = base32ToBytes(secret);
  const counter = BigInt(Math.floor(Date.now() / 1000 / 30));
  const message = new Uint8Array(8);
  let value = counter;
  for (let i = 7; i >= 0; i--) {
    message[i] = Number(value & 255n);
    value >>= 8n;
  }

  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-1" },
    false,
    ["sign"]
  );
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", key, message));
  const offset = digest[digest.length - 1] & 15;
  const binary =
    ((digest[offset] & 127) << 24) |
    ((digest[offset + 1] & 255) << 16) |
    ((digest[offset + 2] & 255) << 8) |
    (digest[offset + 3] & 255);

  return String(binary % 1000000).padStart(6, "0");
}

export class DhanLive extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
    this.upstream = null;
    this.activeKey = null;
  }

  async getAccessToken() {
    const saved = await this.ctx.storage.get("dhanAccessToken");
    if (saved?.token && saved?.reuseUntil > Date.now() + 60000) {
      return saved.token;
    }

    const clientId = this.env.DHAN_CLIENT_ID;
    const pin = this.env.DHAN_PIN;
    const totpSecret = this.env.DHAN_TOTP_SECRET;

    if (!clientId || !/^\d{6}$/.test(String(pin || "")) || !totpSecret) {
      throw new Error("missing_or_invalid_totp_credentials");
    }

    const totp = await generateTotp(totpSecret);
    const authUrl = new URL("https://auth.dhan.co/app/generateAccessToken");
    authUrl.searchParams.set("dhanClientId", clientId);
    authUrl.searchParams.set("pin", pin);
    authUrl.searchParams.set("totp", totp);

    const response = await fetch(authUrl.toString(), { method: "POST" });
    const result = await response.json().catch(() => ({}));

    if (!response.ok || !result.accessToken) {
      console.log("Dhan token generation failed", response.status, result?.errorMessage || result?.message || "No access token returned");
      throw new Error("dhan_token_generation_failed");
    }

    // Reuse for at most 23 hours, leaving a safety margin before Dhan's 24-hour expiry.
    await this.ctx.storage.put("dhanAccessToken", {
      token: result.accessToken,
      reuseUntil: Date.now() + 23 * 60 * 60 * 1000
    });

    return result.accessToken;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const securityId = url.searchParams.get("securityId");
    const segment = url.searchParams.get("segment") || "NSE_FNO";

    if (!securityId || !/^\d+$/.test(securityId)) {
      return Response.json({ ok: false, status: "security_id_required" }, { status: 400 });
    }
    if (!["NSE_FNO", "BSE_FNO"].includes(segment)) {
      return Response.json({ ok: false, status: "invalid_segment" }, { status: 400 });
    }

    const key = `${segment}:${securityId}`;

    if (this.upstream && this.upstream.readyState === WebSocket.OPEN && this.activeKey === key) {
      const saved = await this.ctx.storage.get("feedState");
      return Response.json({
        ok: true,
        status: saved?.status || "connected",
        marketFeed: "connected",
        lastTick: saved?.lastTick || null
      });
    }

    let stage = "token_generation";

    try {
      const token = await this.getAccessToken();
      const clientId = this.env.DHAN_CLIENT_ID;

      if (this.upstream) {
        try { this.upstream.close(); } catch {}
      }

      stage = "websocket_handshake";
      const feedUrl = new URL("https://api-feed.dhan.co");
      feedUrl.searchParams.set("version", "2");
      feedUrl.searchParams.set("token", token);
      feedUrl.searchParams.set("clientId", clientId);
      feedUrl.searchParams.set("authType", "2");

      const response = await fetch(feedUrl.toString(), {
        headers: { Upgrade: "websocket" }
      });
      const ws = response.webSocket;

      if (!ws) {
        return Response.json({
          ok: false,
          status: "websocket_connection_failed",
          stage,
          httpStatus: response.status,
          statusText: response.statusText
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
          if (data.byteLength < 16) return;

          const responseCode = data.getUint8(0);
          const receivedSecurityId = data.getInt32(4, true);

          if (responseCode === 2) {
            const ltp = data.getFloat32(8, true);
            const tradeTime = data.getInt32(12, true);
            if (!Number.isFinite(ltp) || ltp <= 0) return;

            await this.ctx.storage.put("feedState", {
              status: "tick_received",
              lastTick: {
                securityId: String(receivedSecurityId),
                segment,
                ltp,
                tradeTime,
                receivedAt: new Date().toISOString()
              }
            });
          }

          if (responseCode === 50) {
            await this.ctx.storage.put("feedState", { status: "disconnected", lastTick: null });
          }
        } catch {
          console.log("Dhan packet processing error");
        }
      });

      ws.addEventListener("close", async () => {
        await this.ctx.storage.put("feedState", { status: "disconnected", lastTick: null });
        this.upstream = null;
        this.activeKey = null;
      });

      ws.addEventListener("error", async () => {
        await this.ctx.storage.put("feedState", { status: "error", lastTick: null });
      });

      stage = "subscribe_instrument";
      ws.send(JSON.stringify({
        RequestCode: 15,
        InstrumentCount: 1,
        InstrumentList: [{ ExchangeSegment: segment, SecurityId: securityId }]
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
        segment
      });
    } catch (error) {
      console.log("Dhan live connection failed at", stage, error?.message || "unknown_error");
      return Response.json({
        ok: false,
        status: stage === "token_generation" ? "token_generation_failed" : "connection_error",
        stage,
        errorName: error?.name || "UnknownError"
      }, { status: 502 });
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (url.pathname === "/health") {
      return corsResponse(Response.json({ ok: true, service: "Dhan Backend" }));
    }

    if (url.pathname === "/live") {
      const id = env.DHAN_LIVE.idFromName("main");
      const stub = env.DHAN_LIVE.get(id);
      return corsResponse(await stub.fetch(request));
    }

    return corsResponse(new Response("Dhan Backend Ready"));
  }
};
