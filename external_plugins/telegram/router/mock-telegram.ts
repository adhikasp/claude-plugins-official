// Minimal fake Bot API: getUpdates (long poll, counts concurrent pollers) + sendMessage.
export function startMock(port: number, token: string) {
  const updates: any[] = []; let nextId = 100; const sent: any[] = []; let concurrent = 0, maxConcurrent = 0
  const server = Bun.serve({ port, async fetch(req) {
    const url = new URL(req.url); const method = url.pathname.split('/').pop()
    if (!url.pathname.startsWith(`/bot${token}/`)) return Response.json({ ok: false, description: 'unauthorized' }, { status: 401 })
    const body: any = await req.json().catch(() => ({}))
    if (method === 'getUpdates') {
      concurrent++; maxConcurrent = Math.max(maxConcurrent, concurrent)
      try {
        const deadline = Date.now() + (body.timeout ?? 0) * 1000
        while (true) {
          const out = updates.filter(u => u.update_id >= (body.offset ?? 0))
          if (out.length || Date.now() >= deadline) return Response.json({ ok: true, result: out })
          await Bun.sleep(20)
        }
      } finally { concurrent-- }
    }
    if (method === 'sendMessage') { sent.push(body); return Response.json({ ok: true, result: { message_id: sent.length, ...body } }) }
    return Response.json({ ok: false, description: `unknown ${method}` })
  } })
  return {
    sent, stats: () => ({ maxConcurrent }), stop: () => server.stop(true),
    push: (message: any) => updates.push({ update_id: nextId++, message }),
  }
}
