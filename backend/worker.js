import { DurableObject } from "cloudflare:workers";

export class DhanLive extends DurableObject {
  async fetch(request) {
    return Response.json({
      ok: true,
      service: "Dhan Live Backend",
      status: "ready",
      marketFeed: "not_connected"
    });
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
