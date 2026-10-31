// Minimal fake Bot API for tests: long-polling getUpdates (counts concurrent pollers),
// getMe, and a generic recorder for every other method.
export function startMock(port: number, token: string) {
  const updates: any[] = []; let nextId = 100; const calls: { method: string; body: any }[] = []
  let concurrent = 0, maxConcurrent = 0; let msgId = 5000
  const server = Bun.serve({ port, async fetch(req) {
    const url = new URL(req.url); const method = url.pathname.split('/').pop()!
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
    calls.push({ method, body })
    if (method === 'getMe') return Response.json({ ok: true, result: { id: 42, is_bot: true, first_name: 'Test', username: 'semar_test_bot', can_join_groups: true, can_read_all_group_messages: true, supports_inline_queries: false } })
    if (method === 'sendMessage') return Response.json({ ok: true, result: { message_id: ++msgId, date: 0, chat: { id: body.chat_id, type: 'private' }, text: body.text } })
    return Response.json({ ok: true, result: true })
  } })
  return {
    calls, stats: () => ({ maxConcurrent }), stop: () => server.stop(true),
    sent: () => calls.filter(c => c.method === 'sendMessage').map(c => c.body),
    pushUpdate: (u: any) => updates.push({ update_id: nextId++, ...u }),
    push: (message: any) => updates.push({ update_id: nextId++, message }),
  }
}
