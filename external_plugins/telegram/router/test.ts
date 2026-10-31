import { startMock } from './mock-telegram'
import { startRouter } from './router'
import { connect } from './client'
import { strict as assert } from 'node:assert'

const TOKEN = 'TEST', PORT = 18999, SOCK = '/tmp/tg-router-poc.sock'
const mock = startMock(PORT, TOKEN)
const G = -1001
const adhika = { id: 1, username: 'adhika' }, stranger = { id: 99, username: 'x' }
let mid = 0
const dm = (from: any, text: string) => ({ message_id: ++mid, from, chat: { id: from.id, type: 'private' }, text })
const grp = (from: any, text: string, thread?: number) => ({
  message_id: ++mid, from, chat: { id: G, type: 'supergroup' }, text,
  ...(thread ? { is_topic_message: true, message_thread_id: thread } : {}),
})

const spawned: string[] = []
const inbox: Record<string, any[]> = {}
const clients: Record<string, Awaited<ReturnType<typeof connect>>> = {}
const router = await startRouter({
  token: TOKEN, apiRoot: `http://localhost:${PORT}`, sock: SOCK, allowFrom: ['1'], groups: [String(G)],
  // stub "spawn a Claude session": connect a client shortly after, like a booting session would
  spawn: route => { spawned.push(route); setTimeout(async () => {
    inbox[route] = []; clients[route] = await connect(SOCK, route, f => inbox[route]!.push(f))
  }, 100) },
})
const settle = (ms = 500) => Bun.sleep(ms)

mock.push(dm(adhika, 'hi dm')); mock.push(grp(adhika, 'general msg'))
mock.push(grp(adhika, 'in topic 40', 40)); mock.push(grp(adhika, 'in topic 44', 44))
mock.push(grp(adhika, 'second in 40', 40)); mock.push(dm(stranger, 'spam')); mock.push(grp(stranger, 'spam grp', 40))
await settle(1200)

let n = 0; const ok = (name: string) => console.log(`ok ${++n} - ${name}`)
assert.deepEqual([...spawned].sort(), [`${G}:0`, `${G}:40`, `${G}:44`, 'dm:1'].sort()); ok('one session spawned per route, none for dropped senders')
assert.equal(spawned.filter(r => r === `${G}:40`).length, 1); ok('route spawned once despite 2 queued messages')
assert.deepEqual(inbox['dm:1']!.map(f => f.content), ['hi dm']); ok('DM isolated')
assert.deepEqual(inbox[`${G}:0`]!.map(f => f.content), ['general msg']); ok('General topic isolated')
assert.deepEqual(inbox[`${G}:40`]!.map(f => f.content), ['in topic 40', 'second in 40']); ok('topic 40 got only its messages, in order')
assert.deepEqual(inbox[`${G}:44`]!.map(f => f.content), ['in topic 44']); ok('topic 44 isolated')
assert.equal(inbox[`${G}:40`]![0].meta.thread_id, '40'); ok('thread_id in meta')
assert.ok(router.log.some(l => l.startsWith('drop'))); ok('non-allowlisted senders dropped')
assert.equal(mock.stats().maxConcurrent, 1); ok('only one getUpdates in flight (single poller)')

const r1 = await clients[`${G}:40`]!.call('sendMessage', { chat_id: String(G), message_thread_id: 40, text: 'reply 40' })
assert.ok(r1.ok); assert.equal(mock.sent.at(-1).message_thread_id, 40); ok('reply into own topic carries message_thread_id')
const r2 = await clients[`${G}:40`]!.call('sendMessage', { chat_id: String(G), message_thread_id: 44, text: 'sneaky' })
assert.equal(r2.ok, false); ok('session 40 cannot post into topic 44')
const r3 = await clients['dm:1']!.call('sendMessage', { chat_id: String(G), text: 'sneaky2' })
assert.equal(r3.ok, false); ok('DM session cannot post into the group')
const r4 = await clients[`${G}:0`]!.call('sendMessage', { chat_id: String(G), text: 'general reply' })
assert.ok(r4.ok); assert.equal(mock.sent.at(-1).message_thread_id, undefined); ok('General reply has no thread id')

// no redelivery after offset ack; later message goes straight to the live client
const before = inbox[`${G}:44`]!.length
mock.push(grp(adhika, 'later in 44', 44)); await settle(600)
assert.equal(inbox[`${G}:44`]!.length, before + 1); assert.equal(spawned.length, 4); ok('no duplicates, live client reused, no respawn')

console.log(`\n${n} checks passed`)
Object.values(clients).forEach(c => c.close()); await router.stop(); mock.stop()
