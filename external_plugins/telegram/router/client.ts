// PoC session-side client (what the plugin's "client mode" would do): bind to one route.
export async function connect(sock: string, route: string, onInbound: (f: any) => void) {
  let nextId = 1; const pending = new Map<number, (r: any) => void>(); let buf = ''
  const s = await Bun.connect<undefined>({ unix: sock, socket: {
    data(_s, d) {
      buf += d.toString(); let i
      while ((i = buf.indexOf('\n')) >= 0) {
        const f = JSON.parse(buf.slice(0, i)); buf = buf.slice(i + 1)
        if (f.type === 'inbound') onInbound(f); else if (f.type === 'result') pending.get(f.id)?.(f)
      }
    },
  } })
  s.write(JSON.stringify({ type: 'hello', route }) + '\n')
  return {
    call: (method: string, params: any) => new Promise<any>(res => {
      const id = nextId++; pending.set(id, res)
      s.write(JSON.stringify({ type: 'call', id, method, params }) + '\n')
    }),
    close: () => s.end(),
  }
}
