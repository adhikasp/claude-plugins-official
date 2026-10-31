// PoC: single Telegram poller that fans messages out to one Claude session per route.
// Route key: dm:<user_id> | <chat_id>:<topic id> (General / non-forum = 0).
// Clients connect over a unix socket (newline-delimited JSON) and may only send
// to the chat+topic of their own route.
import { unlinkSync } from 'node:fs'

export type Route = string
export type RouterOpts = {
  token: string
  apiRoot?: string
  sock: string
  allowFrom: string[]           // user ids allowed in DMs and groups
  groups: string[]              // allowed group chat ids
  spawn: (route: Route) => void // start a session for a route with no client yet
  pollTimeout?: number
}

export function routeOf(msg: any): { route: Route; chat: string; thread?: number } {
  const chat = String(msg.chat.id)
  if (msg.chat.type === 'private') return { route: `dm:${msg.from.id}`, chat }
  const thread = msg.is_topic_message ? msg.message_thread_id : undefined
  return { route: `${chat}:${thread ?? 0}`, chat, thread }
}

export async function startRouter(o: RouterOpts) {
  const api = (o.apiRoot ?? 'https://api.telegram.org') + `/bot${o.token}`
  const call = async (method: string, params: any) => {
    const r = await fetch(`${api}/${method}`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(params),
    })
    const j: any = await r.json()
    if (!j.ok) throw new Error(`${method}: ${j.description ?? r.status}`)
    return j.result
  }
  const clients = new Map<Route, any>()       // route -> socket
  const queues = new Map<Route, any[]>()      // buffered inbound while no client
  const scope = new Map<Route, { chat: string; thread?: number }>()
  const log: string[] = []
  const send = (s: any, obj: any) => s.write(JSON.stringify(obj) + '\n')

  try { unlinkSync(o.sock) } catch {}
  const server = Bun.listen<{ route?: Route; buf: string }>({
    unix: o.sock,
    socket: {
      open(s) { s.data = { buf: '' } },
      data(s, d) {
        s.data.buf += d.toString()
        let i
        while ((i = s.data.buf.indexOf('\n')) >= 0) {
          const line = s.data.buf.slice(0, i); s.data.buf = s.data.buf.slice(i + 1)
          void onFrame(s, JSON.parse(line))
        }
      },
      close(s) { if (s.data.route && clients.get(s.data.route) === s) clients.delete(s.data.route) },
    },
  })

  async function onFrame(s: any, f: any) {
    if (f.type === 'hello') {
      s.data.route = f.route; clients.set(f.route, s)
      for (const m of queues.get(f.route) ?? []) send(s, m)
      queues.delete(f.route)
      return
    }
    if (f.type === 'call') {
      try {
        const sc = scope.get(s.data.route!)
        const p = f.params ?? {}
        if (!sc || String(p.chat_id) !== sc.chat || (sc.thread ?? 0) !== Number(p.message_thread_id ?? 0))
          throw new Error(`route ${s.data.route} may not send to chat ${p.chat_id} topic ${p.message_thread_id ?? 0}`)
        send(s, { type: 'result', id: f.id, ok: true, result: await call(f.method, p) })
      } catch (e: any) { send(s, { type: 'result', id: f.id, ok: false, error: String(e.message ?? e) }) }
    }
  }

  const allowedUser = (id: any) => o.allowFrom.includes(String(id))
  function deliver(msg: any) {
    const { route, chat, thread } = routeOf(msg)
    if (!allowedUser(msg.from?.id)) return log.push(`drop ${route} (user)`)
    if (msg.chat.type !== 'private' && !o.groups.includes(chat)) return log.push(`drop ${route} (group)`)
    scope.set(route, { chat, thread })
    const frame = { type: 'inbound', route, content: msg.text ?? '', meta: {
      chat_id: chat, message_id: String(msg.message_id), ...(thread != null ? { thread_id: String(thread) } : {}),
      user: msg.from.username ?? String(msg.from.id), user_id: String(msg.from.id),
    } }
    const c = clients.get(route)
    if (c) return send(c, frame)
    const q = queues.get(route) ?? []; q.push(frame); queues.set(route, q)
    if (q.length === 1) { log.push(`spawn ${route}`); o.spawn(route) }
  }

  let offset = 0, running = true
  const loop = (async () => {
    while (running) {
      try {
        const ups = await call('getUpdates', { offset, timeout: o.pollTimeout ?? 1, allowed_updates: ['message'] })
        for (const u of ups) { offset = u.update_id + 1; if (u.message) deliver(u.message) }
      } catch (e: any) { log.push(`poll error ${e.message}`); await Bun.sleep(200) }
    }
  })()
  return { log, clients, queues, stop: async () => { running = false; await loop; server.stop(true); try { unlinkSync(o.sock) } catch {} } }
}
