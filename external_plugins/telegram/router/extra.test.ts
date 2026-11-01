// Differential checks for paths the main e2e doesn't cover. Runs against whichever router is selected
// (ROUTER_BIN=<tg-router> or default server.ts) with BRIDGE=<tg-bridge> or in-process bun clients.
import { startMock } from './mock-telegram'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { strict as assert } from 'node:assert'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const TOKEN = 'T2', PORT = 18996, SERVER = resolve(import.meta.dir, '..', 'server.ts')
const dir = mkdtempSync(join(tmpdir(), 'tg-x-')); const STATE = join(dir, 's'); Bun.spawnSync(['mkdir', '-p', STATE])
writeFileSync(join(STATE, 'access.json'), JSON.stringify({ dmPolicy: 'allowlist', allowFrom: ['1'], groups: {}, chunkMode: 'newline' }))
writeFileSync(join(STATE, '.env'), 'SECRET=1\n')
const env = { ...Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^TELEGRAM_/.test(k))),
  TELEGRAM_BOT_TOKEN: TOKEN, TELEGRAM_API_ROOT: `http://localhost:${PORT}`, TELEGRAM_STATE_DIR: STATE,
  TELEGRAM_ACCESS_MODE: 'static', TELEGRAM_ROUTER_SOCK: join(STATE, 'router.sock') } as Record<string, string>
const mock = startMock(PORT, TOKEN)
const router = Bun.spawn(process.env.ROUTER_BIN ? [process.env.ROUTER_BIN] : ['bun', SERVER],
  { env: { ...env, TELEGRAM_ROLE: 'router', TELEGRAM_SPAWN_CMD: 'true' }, stdout: 'ignore', stderr: 'inherit' })
const inbox: any[] = []
const client = new Client({ name: 't', version: '0' }, { capabilities: {} })
client.fallbackNotificationHandler = async (n: any) => { if (n.method === 'notifications/claude/channel') inbox.push(n.params) }
await client.connect(new StdioClientTransport({ command: process.env.BRIDGE ?? 'bun', args: process.env.BRIDGE ? [] : [SERVER],
  env: { ...env, TELEGRAM_ROLE: 'client', TELEGRAM_ROUTE: 'dm:1', TELEGRAM_ROUTE_ADMIN: '1' } }))
const until = async (f: () => boolean, ms = 8000) => { const t = Date.now(); while (!f()) { if (Date.now() - t > ms) throw new Error('timeout ' + f); await Bun.sleep(50) } }
const user = { id: 1, is_bot: false, first_name: 'A', username: 'adhika' }
let mid = 100
const dm = (text: string, extra: any = {}) => ({ message_id: ++mid, date: 1700000000, from: user, chat: { id: 1, type: 'private', first_name: 'A' }, text, ...extra })
const call = (name: string, args: any) => client.callTool({ name, arguments: args }) as Promise<any>
const last = (m: string) => mock.calls.filter(c => c.method === m).at(-1)?.body
await Bun.sleep(800)
let n = 0; const ok = (s: string) => console.log(`ok ${++n} - ${s}`)

// 1. inbound: 👀 ack, ts format, reply context
mock.push(dm('hello **there**', { reply_to_message: { message_id: 7, date: 1, chat: { id: 1, type: 'private' }, from: { id: 99, is_bot: true, username: 'semar_test_bot', first_name: 'B' }, text: '  quoted   text ' } }))
await until(() => inbox.length === 1)
const m0 = inbox[0].meta
assert.equal(m0.ts, '2023-11-14T22:13:20.000Z'); assert.equal(m0.reply_to_text, 'quoted text'); assert.equal(m0.reply_to_user, 'semar_test_bot'); assert.equal(m0.user, 'adhika')
assert.equal(inbox[0].content, 'hello **there**'); ok('inbound meta (ts iso, reply context)')
assert.ok(mock.calls.some(c => c.method === 'setMessageReaction' && c.body.reaction?.[0]?.emoji === '👀')); ok('👀 ack reaction set')
assert.ok(mock.calls.some(c => c.method === 'sendChatAction')); ok('typing action sent')

// 2. turn_done clears the ack (empty reaction list)
mock.calls.length = 0
const s = await Bun.connect({ unix: join(STATE, 'router.sock'), socket: { data() {}, open(sock) { sock.write(JSON.stringify({ type: 'turn_done', route: 'dm:1' }) + '\n') } } })
await until(() => mock.calls.some(c => c.method === 'setMessageReaction' && c.body.reaction?.length === 0)); ok('turn_done clears 👀')

// 3. agent-reacted message is dropped from ack tracking
mock.push(dm('second')); await until(() => inbox.length === 2)
await call('react', { chat_id: '1', message_id: String(inbox[1].meta.message_id), emoji: '👍' })
mock.calls.length = 0
const s2 = await Bun.connect({ unix: join(STATE, 'router.sock'), socket: { data() {}, open(sock) { sock.write(JSON.stringify({ type: 'turn_done', route: 'dm:1' }) + '\n') } } })
await Bun.sleep(500); assert.ok(!mock.calls.some(c => c.method === 'setMessageReaction' && c.body.reaction?.length === 0)); ok('react drops message from ack tracking')

// 4. markdown conversion + chunking
mock.calls.length = 0
let r = await call('reply', { chat_id: '1', text: '**bold** a.b - [l](https://x.y) `c.d`' })
assert.ok(!r.isError); const b = last('sendMessage'); assert.equal(b.text, '*bold* a\\.b \\- [l](https://x.y) `c.d`'); assert.equal(b.parse_mode, 'MarkdownV2'); ok('markdown → MarkdownV2')
mock.calls.length = 0
r = await call('reply', { chat_id: '1', text: 'plain *text*', format: 'plain' }); assert.equal(last('sendMessage').text, 'plain *text*'); assert.equal(last('sendMessage').parse_mode, undefined); ok('plain format passes through')
mock.calls.length = 0
r = await call('reply', { chat_id: '1', text: ('para one.\n\n' + 'x'.repeat(3000) + '\n\n' + 'y'.repeat(3000)) })
assert.equal(mock.calls.filter(c => c.method === 'sendMessage').length >= 2, true); assert.match(r.content[0].text, /parts/); ok('long reply chunked')

// 5. edit_message
mock.calls.length = 0
r = await call('edit_message', { chat_id: '1', message_id: '5001', text: 'done.' })
assert.equal(last('editMessageText').text, 'done\\.'); assert.match(r.content[0].text, /^edited/); ok('edit_message converts and edits')

// 6. files: photo vs document; refuses channel state
const png = join(dir, 'a.png'); const txt = join(dir, 'b.txt'); writeFileSync(png, 'x'); writeFileSync(txt, 'y')
mock.calls.length = 0
r = await call('reply', { chat_id: '1', text: 'files', files: [png, txt] }); assert.ok(!r.isError, JSON.stringify(r))
assert.deepEqual(mock.calls.map(c => c.method).filter(m => m.startsWith('send') && m !== 'sendChatAction'), ['sendMessage', 'sendPhoto', 'sendDocument']); ok('photo/document routing')
r = await call('reply', { chat_id: '1', text: 'x', files: [join(STATE, '.env')] }); assert.ok(r.isError); assert.match(r.content[0].text, /refusing to send channel state/); ok('channel state not sendable')
r = await call('reply', { chat_id: '555', text: 'x' }); assert.ok(r.isError); assert.match(r.content[0].text, /not allowlisted/); ok('non-allowlisted chat refused')

// 7. /model keyboard in DM
mock.calls.length = 0
mock.push(dm('/model', { entities: [{ type: 'bot_command', offset: 0, length: 6 }] }))
await until(() => !!last('sendMessage')); const kb = last('sendMessage').reply_markup.inline_keyboard
assert.equal(kb.length, 2); assert.equal(kb[0].length, 5); assert.equal(kb[0][0].callback_data, 'mdl:model:opus'); assert.equal(kb[1][4].callback_data, 'mdl:effort:max')
assert.equal(inbox.length, 2); ok('/model keyboard, not forwarded as chat')

// 8. strangers and unknown commands
mock.push({ ...dm('hi'), from: { id: 99, is_bot: false, first_name: 'X' }, chat: { id: 99, type: 'private' } }); await Bun.sleep(300); assert.equal(inbox.length, 2); ok('stranger dropped')
mock.push(dm('/start', { entities: [{ type: 'bot_command', offset: 0, length: 6 }] })); await until(() => inbox.length === 3); assert.equal(inbox[2].content, '/start'); ok('other commands forwarded as chat')

// 9. attachments meta
mock.push(dm('', { text: undefined, document: { file_id: 'F1', file_unique_id: 'U1', file_name: 'a<b>.pdf', file_size: 12, mime_type: 'application/pdf' } }))
await until(() => inbox.length === 4); const am = inbox[3].meta
assert.equal(am.attachment_kind, 'document'); assert.equal(am.attachment_file_id, 'F1'); assert.equal(am.attachment_name, 'a_b_.pdf'); assert.equal(am.attachment_size, '12'); assert.equal(inbox[3].content, '(document: a_b_.pdf)'); ok('document attachment meta sanitized')
console.log(`extra checks passed (${n})`); await client.close(); router.kill(); mock.stop(); process.exit(0)
