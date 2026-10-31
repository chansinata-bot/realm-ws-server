export { GameRoom } from './room.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      // Single shared room for the whole game world.
      const id = env.ROOM.idFromName('main');
      const stub = env.ROOM.get(id);
      return stub.fetch(request);
    }
    return new Response('realm-ws-server is running. Connect via wss://<this-domain>/ws', { status: 200 });
  }
};
