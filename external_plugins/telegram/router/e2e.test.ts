// End-to-end test of the per-route architecture against a fake Bot API:
// real router process + real client MCP servers (stdio) driven by the MCP SDK.
// Run: bun router/e2e.test.ts   (from external_plugins/telegram)
import { startMock } from './mock-telegram'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { strict as assert } from 'node:assert'
import { mkdtempSync, writeFileSync, readFileSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const TOKEN = 'TESTTOKEN', PORT = 18998
const G = -1001, DM = 1
const SERVER = resolve(import.meta.dir, '..', 'server.ts')
const dir = mkdtempSync(join(tmpdir(), 'tg-e2e-'))
const STATE = join(dir, 'state'); Bun.spawnSync(['mkdir', '-p', STATE])
writeFileSync(join(STATE, 'access.json'), JSON.stringify({
  dmPolicy: 'allowlist', allowFrom: ['1'], groups: { [String(G)]: { requireMention: false, allowFrom: ['1'] } },
}))
const spawnLog = join(dir, 'spawn.log')
// don't inherit another session's role/route (a developer shell may itself be a Telegram client)
const cleanEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^TELEGRAM_/.test(k)))
const baseEnv = {
  ...cleanEnv, TELEGRAM_BOT_TOKEN: TOKEN, TELEGRAM_API_ROOT: `http://localhost:${PORT}`, TELEGRAM_STATE_DIR: STATE,
  TELEGRAM_ACCESS_MODE: 'static', TELEGRAM_ROUTER_SOCK: join(STATE, 'router.sock'),
} as Record<string, string>

const mock = startMock(PORT, TOKEN)
const router = Bun.spawn(['bun', SERVER], {
  env: { ...baseEnv, TELEGRAM_ROLE: 'router', TELEGRAM_SPAWN_CMD: `echo "$TG_ROUTE|$TG_TITLE" >> ${spawnLog}` },
  stdout: 'ignore', stderr: 'inherit',
})

type Sess = { client: Client; inbox: any[]; perms: any[] }
async function startClient(route: string, admin = false): Promise<Sess> {
  const client = new Client({ name: 'test', version: '0' }, { capabilities: {} })
  const s: Sess = { client, inbox: [], perms: [] }
  client.fallbackNotificationHandler = async (n: any) => {
    if (n.method === 'notifications/claude/channel') s.inbox.push(n.params)
    if (n.method === 'notifications/claude/channel/permission') s.perms.push(n.params)
  }
  await client.connect(new StdioClientTransport({
    // BRIDGE=/path/to/tg-bridge runs every client through the Rust relay instead of an in-process bun client
    command: process.env.BRIDGE ?? 'bun', args: process.env.BRIDGE ? [] : [SERVER],
    env: { ...baseEnv, TELEGRAM_ROLE: 'client', TELEGRAM_ROUTE: route, ...(admin ? { TELEGRAM_ROUTE_ADMIN: '1' } : {}) },
  }))
  return s
}
const until = async (f: () => boolean, ms = 8000) => { const t = Date.now(); while (!f()) { if (Date.now() - t > ms) throw new Error('timeout: ' + f.toString()); await Bun.sleep(50) } }
const spawned = () => existsSync(spawnLog) ? readFileSync(spawnLog, 'utf8').trim().split('\n').filter(Boolean) : []
const reply = (s: Sess, args: any) => s.client.callTool({ name: 'reply', arguments: args }) as Promise<any>

let mid = 0
const user = { id: 1, username: 'adhika', is_bot: false, first_name: 'A' }, stranger = { id: 99, username: 'x', is_bot: false, first_name: 'X' }
const grpChat = { id: G, type: 'supergroup', title: 'Keluarga AI', is_forum: true }
const topicMsg = (from: any, text: string, thread: number, title?: string) => ({
  message_id: ++mid, date: 1, from, chat: grpChat, text, is_topic_message: true, message_thread_id: thread,
  reply_to_message: title ? { message_id: thread, date: 1, chat: grpChat, forum_topic_created: { name: title, icon_color: 0 } } : undefined,
})
const generalMsg = (from: any, text: string) => ({ message_id: ++mid, date: 1, from, chat: grpChat, text })
const dmMsg = (from: any, text: string) => ({ message_id: ++mid, date: 1, from, chat: { id: from.id, type: 'private', first_name: 'A' }, text })

let n = 0; const ok = (m: string) => console.log(`ok ${++n} - ${m}`)
try {
  await until(() => existsSync(join(STATE, 'router.sock')))

  // 1. topic 40 message: session is spawned once, with the topic title, before any client exists
  mock.push(topicMsg(user, 'halo topic 40', 40, 'Tiket Jakarta'))
  mock.push(topicMsg(user, 'dua', 40, 'Tiket Jakarta'))
  mock.push(topicMsg(stranger, 'spam', 40, 'Tiket Jakarta'))
  await until(() => spawned().length >= 1)
  await Bun.sleep(500)
  assert.deepEqual(spawned(), [`${G}:40|Tiket Jakarta`]); ok('one spawn for topic 40 with its title; stranger ignored')
  const s40 = await startClient(`${G}:40`)
  await until(() => s40.inbox.length >= 2)
  assert.deepEqual(s40.inbox.map(p => p.content), ['halo topic 40', 'dua']); ok('queued messages delivered in order once the client connects')
  const meta = s40.inbox[0].meta
  assert.equal(meta.thread_id, '40'); assert.equal(meta.topic_title, 'Tiket Jakarta'); assert.equal(meta.chat_title, 'Keluarga AI'); assert.equal(meta.chat_id, String(G))
  ok('meta carries thread_id, topic_title and chat_title')

  // 2. replies go into the topic automatically; other topics/chats are refused
  mock.calls.length = 0
  let r = await reply(s40, { chat_id: String(G), text: 'jawab' })
  assert.ok(!r.isError); assert.equal(mock.sent().at(-1).message_thread_id, 40); ok('reply without thread lands in own topic')
  r = await reply(s40, { chat_id: String(G), text: 'x', message_thread_id: '44' })
  assert.ok(r.isError); ok('cannot post into another topic')
  r = await reply(s40, { chat_id: '1', text: 'x' })
  assert.ok(r.isError); ok('cannot post into a DM')

  // 3. title learned from a service message, General has no thread, DM route is separate
  mock.pushUpdate({ message: { message_id: 44, date: 1, from: user, chat: grpChat, is_topic_message: true, message_thread_id: 44, forum_topic_created: { name: 'Plugin dev', icon_color: 0 } } })
  await Bun.sleep(300)
  mock.push(topicMsg(user, 'in 44', 44)); mock.push(generalMsg(user, 'general')); mock.push(dmMsg(user, 'dm hi'))
  await until(() => spawned().length >= 4)
  assert.ok(spawned().includes(`${G}:44|Plugin dev`)); ok('topic title learned from forum_topic_created')
  assert.ok(spawned().some(l => l.startsWith(`${G}:0|`))); assert.ok(spawned().some(l => l.startsWith('dm:1|'))); ok('General and DM are their own routes')
  assert.equal(spawned().length, 4); ok('no extra spawns')
  const s40b = s40.inbox.length; await Bun.sleep(300); assert.equal(s40.inbox.length, s40b); ok('topic 40 session saw nothing from other routes')

  // 4. admin DM session may post into a specific topic; non-admin DM may not leave its DM
  const sdm = await startClient('dm:1', true)
  await until(() => sdm.inbox.length >= 1); assert.equal(sdm.inbox[0].content, 'dm hi')
  r = await reply(sdm, { chat_id: String(G), message_thread_id: '44', text: 'from admin' })
  assert.ok(!r.isError); assert.equal(mock.sent().at(-1).message_thread_id, 44); ok('admin DM session can post into a chosen topic')
  const sdm2 = await startClient('dm:1', false)
  r = await reply(sdm2, { chat_id: String(G), text: 'x' }); assert.ok(r.isError); ok('non-admin DM session is confined to its DM')
  await sdm2.client.close()

  // 5. permission relay: prompt goes to the DM, button press reaches only the owning session
  mock.calls.length = 0
  await s40.client.notification({ method: 'notifications/claude/channel/permission_request', params: { request_id: 'abcde', tool_name: 'Bash', description: 'ls', input_preview: '{}' } })
  await until(() => mock.sent().some(m => String(m.chat_id) === '1' && m.reply_markup))
  mock.pushUpdate({ callback_query: { id: 'cb1', from: user, chat_instance: 'x', data: 'perm:allow:abcde', message: { message_id: 7, date: 1, chat: { id: 1, type: 'private' }, text: 'perm' } } })
  await until(() => s40.perms.length >= 1)
  assert.deepEqual(s40.perms[0], { request_id: 'abcde', behavior: 'allow' }); assert.equal(sdm.perms.length, 0); ok('permission button routed to the session that asked')
  mock.push(dmMsg(user, 'yes fghij'))
  await Bun.sleep(400); assert.equal(sdm.inbox.length, 1); ok('permission-looking text is intercepted, not delivered as chat')

  // 5b. DM topics (threaded mode): own route per topic, replies carry the thread, main DM stays separate
  const dmTopic = (text: string, thread: number, title?: string) => ({
    message_id: ++mid, date: 1, from: user, chat: { id: 1, type: 'private', first_name: 'A' }, text, is_topic_message: true, message_thread_id: thread,
    reply_to_message: title ? { message_id: thread, date: 1, chat: { id: 1, type: 'private', first_name: 'A' }, forum_topic_created: { name: title, icon_color: 0 } } : undefined,
  })
  mock.push(dmTopic('in dm topic', 7, 'Belanja'))
  await until(() => spawned().some(l => l.startsWith('dm:1:7|')))
  assert.ok(spawned().includes('dm:1:7|Belanja')); ok('DM topic gets its own route and title')
  const sdt = await startClient('dm:1:7', true)
  await until(() => sdt.inbox.length >= 1)
  assert.equal(sdt.inbox[0].meta.thread_id, '7'); assert.equal(sdt.inbox[0].meta.topic_title, 'Belanja'); ok('DM topic meta carries thread_id and topic_title')
  r = await reply(sdt, { chat_id: '1', text: 'jawab dm topic' })
  assert.ok(!r.isError); assert.equal(mock.sent().at(-1).message_thread_id, 7); ok('DM topic reply lands in the same thread')
  assert.equal(sdm.inbox.length, 1); ok('main DM session did not receive the DM topic message')
  await sdt.client.close()

  // 6. housekeeping
  const routes = JSON.parse(readFileSync(join(STATE, 'routes.json'), 'utf8'))
  assert.ok(routes[`${G}:40`] && routes['dm:1'] && routes[`${G}:40`].title === 'Tiket Jakarta'); ok('routes.json registry written')
  assert.equal(mock.stats().maxConcurrent, 1); ok('single getUpdates poller')

  console.log(`\n${n} checks passed`)
  await s40.client.close(); await sdm.client.close()
} finally {
  router.kill(); mock.stop()
}
process.exit(0)
