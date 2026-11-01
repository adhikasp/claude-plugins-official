// Router restart resilience: a session attached through tg-bridge must survive a router restart
// (bridge reconnects, replays the MCP handshake) and keep receiving inbound messages and serving tools.
// Run: BRIDGE=bridge/target/release/tg-bridge bun router/restart.test.ts
import { startMock } from './mock-telegram'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { strict as assert } from 'node:assert'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const TOKEN = 'T', PORT = 18997, SERVER = resolve(import.meta.dir, '..', 'server.ts')
const BRIDGE = process.env.BRIDGE!; assert.ok(BRIDGE, 'set BRIDGE')
const dir = mkdtempSync(join(tmpdir(), 'tg-rst-')); const STATE = join(dir, 's'); Bun.spawnSync(['mkdir', '-p', STATE])
writeFileSync(join(STATE, 'access.json'), JSON.stringify({ dmPolicy: 'allowlist', allowFrom: ['1'], groups: {} }))
const env = { ...Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^TELEGRAM_/.test(k))),
  TELEGRAM_BOT_TOKEN: TOKEN, TELEGRAM_API_ROOT: `http://localhost:${PORT}`, TELEGRAM_STATE_DIR: STATE,
  TELEGRAM_ACCESS_MODE: 'static', TELEGRAM_ROUTER_SOCK: join(STATE, 'router.sock') } as Record<string, string>
const mock = startMock(PORT, TOKEN)
const startRouter = () => Bun.spawn(['bun', SERVER], { env: { ...env, TELEGRAM_ROLE: 'router', TELEGRAM_SPAWN_CMD: 'true' }, stdout: 'ignore', stderr: 'ignore' })
let router = startRouter()
const inbox: any[] = []
const client = new Client({ name: 't', version: '0' }, { capabilities: {} })
client.fallbackNotificationHandler = async (n: any) => { if (n.method === 'notifications/claude/channel') inbox.push(n.params) }
await client.connect(new StdioClientTransport({ command: BRIDGE, args: [], env: { ...env, TELEGRAM_ROUTE: 'dm:1', TELEGRAM_ROUTE_ADMIN: '1' } }))
const until = async (f: () => boolean, ms = 10000) => { const t = Date.now(); while (!f()) { if (Date.now() - t > ms) throw new Error('timeout ' + f); await Bun.sleep(50) } }
const dm = (text: string) => ({ message_id: Math.floor(Math.random() * 1e6), date: 1, from: { id: 1, is_bot: false, first_name: 'A' }, chat: { id: 1, type: 'private', first_name: 'A' }, text })
await Bun.sleep(800)
mock.push(dm('one')); await until(() => inbox.length === 1)
console.log('ok 1 - inbound before restart')
router.kill(); await router.exited; await Bun.sleep(500)
router = startRouter()
await Bun.sleep(3000) // bun boot + bridge reconnect
mock.push(dm('two')); await until(() => inbox.some(m => m.content === 'two'))
console.log('ok 2 - inbound after router restart (same MCP session)')
const r: any = await client.callTool({ name: 'reply', arguments: { chat_id: '1', text: 'hi' } })
assert.ok(!r.isError); console.log('ok 3 - tool call after restart')
await client.close(); router.kill(); mock.stop(); console.log('restart checks passed'); process.exit(0)
