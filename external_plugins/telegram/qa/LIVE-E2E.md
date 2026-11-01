# Live e2e via @semar_qa_bot

The router (`telegram-router`, one poller for `@semar_my_ai_bot`) can be driven end to end by a
second bot, `@semar_qa_bot`. Nothing runs behind the QA bot: the agent running the
test is the only user, so there is no reply-loop risk. Send with `qa/qa-send`; the reply arrives as a
normal `<channel>` event in the session for that route (or read it from the topic).

## Preconditions (all were needed)
1. Both bots have **Bot-to-Bot Communication Mode** on in @BotFather.
2. `<QA_BOT_ID>` is in `access.json` `groups.<chat>.allowFrom` (and `allowFrom` for DM tests).
3. **Restart the router after any access.json edit**: it runs `TELEGRAM_ACCESS_MODE=static`
   (snapshot at boot): `systemctl --user restart telegram-router`.
4. QA bot is a member of the group. Plain @mentions in forum topics are delivered (verified).

## Scenarios (adhoc; the agent judges pass/fail from what it receives)
| # | Send (as QA bot) | Expect |
|---|---|---|
| 1 | `qa-send --topic 125 "@semar_my_ai_bot reply with PONG-1"` | session for route `:125` gets a `<channel user="semar_qa_bot">` event and can reply |
| 2 | `qa-send --topic 125 --reply-to <bot msg id> "..."` | reply-to delivery works without a mention |
| 3 | `qa-send --topic <new/idle topic>` | router spawns a session for that route (`tg-routes` shows connected) |
| 4 | `qa-send --topic 125 "no mention at all"` | group has `requireMention:false` so still delivered; flip to a mention-only group  to see it dropped |
| 5 | send as a sender NOT in allowFrom (remove the id, restart router) | dropped silently, no event |
| 6 | DM the main bot as the QA bot | n/a; not possible: bots cannot DM each other as a user would, DMs need a human sender |
| 7 | long text (>4096), markdown/emoji, `\`, `_`, `*` | arrives intact; check the bot's reply formatting |
| 8 | the main bot's `reply` with `files` and `edit_message`: ask the session to send a file, read it via the QA bot's `getUpdates` or `copyMessage` | attachment visible |

To observe what the main bot posted, poll the QA bot: `curl .../bot$T/getUpdates` shows only
messages the QA bot can see (privacy mode on: mentions/replies to it). Make the session address
`@semar_qa_bot` in its reply to see it there.

Honest limits: bots cannot DM each other as a user would (scenario 6), and getUpdates for the QA bot
works only while no one else polls that token.
