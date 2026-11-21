import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { SqliteQueue } from '../lib/queue.js'

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
function tmpDb() {
  const dir = mkdtempSync(join(tmpdir(), 'sqliteq-'))
  return { path: join(dir, 'q.db'), cleanup: () => { try { rmSync(dir, { recursive: true, force: true }) } catch { /* ignore */ } } }
}

test('開起來就是 WAL（不是 DELETE journal）', () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  const raw = new DatabaseSync(path)
  assert.equal(String(raw.prepare('PRAGMA journal_mode').get().journal_mode).toLowerCase(), 'wal')
  raw.close(); q.close(); cleanup()
})

test('enqueue → claim → ack', () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  const { id, deduped } = q.enqueue({ hello: 'world' })
  assert.equal(deduped, false)
  const job = q.claim()
  assert.equal(job.id, id)
  assert.deepEqual(job.payload, { hello: 'world' })
  assert.equal(job.state, 'inflight')
  assert.equal(job.attempts, 1)
  assert.equal(q.ack(id), true)
  assert.deepEqual(q.stats(), { ready: 0, inflight: 0, done: 1, dead: 0 })
  assert.equal(q.claim(), null, 'ack 過的不該再被認領')
  q.close(); cleanup()
})

test('認領後同一筆不會被第二次認領（可見性超時未到）', () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  q.enqueue('a')
  const j1 = q.claim({ visibilityMs: 5000 })
  assert.ok(j1)
  assert.equal(q.claim({ visibilityMs: 5000 }), null, '租約內不該再發一次')
  q.close(); cleanup()
})

test('可見性超時後會重新投遞（至少一次）', async () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  q.enqueue('slow')
  const j1 = q.claim({ visibilityMs: 60 })
  assert.equal(j1.attempts, 1)
  await sleep(140)
  const j2 = q.claim({ visibilityMs: 60 })
  assert.ok(j2, '超時後要能再拿到')
  assert.equal(j2.id, j1.id)
  assert.equal(j2.attempts, 2, 'attempts 要累加')
  q.close(); cleanup()
})

test('fail → 指數退避後重排；達上限進死信', async () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path, { maxAttempts: 2, backoffBaseMs: 30, backoffMaxMs: 60 })
  const { id } = q.enqueue('boom', { maxAttempts: 2 })

  const j1 = q.claim()
  const f1 = q.fail(id, new Error('第一次失敗'))
  assert.equal(f1.requeued, true)
  assert.ok(f1.retryInMs >= 30)
  assert.equal(q.stats().ready, 1)

  await sleep(60)
  const j2 = q.claim()
  assert.ok(j2, '退避後要能再拿到')
  const f2 = q.fail(j2.id, new Error('第二次失敗'))
  assert.equal(f2.dead, true)
  assert.equal(q.stats().dead, 1)
  assert.equal(q.claim(), null, '死信不該再被認領')

  const raw = new DatabaseSync(path)
  const row = raw.prepare('SELECT state,last_error FROM jobs WHERE id=?').get(id)
  assert.equal(row.state, 'dead')
  assert.match(String(row.last_error), /第二次失敗/)
  raw.close(); q.close(); cleanup()
})

test('冪等鍵：重複入列回同一筆；完成後可以再用同一個鍵', () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  const a = q.enqueue('x', { dedupeKey: 'k1' })
  const b = q.enqueue('x', { dedupeKey: 'k1' })
  assert.equal(a.deduped, false)
  assert.equal(b.deduped, true)
  assert.equal(b.id, a.id, '同一鍵要回同一筆')
  assert.equal(q.stats().ready, 1, '不該變成兩筆')

  const job = q.claim()
  q.ack(job.id)
  const c = q.enqueue('x', { dedupeKey: 'k1' })
  assert.equal(c.deduped, false, '結束後同鍵可以再入列')
  assert.notEqual(c.id, a.id)
  q.close(); cleanup()
})

test('兩個連線同時搶：不會重複認領', () => {
  const { path, cleanup } = tmpDb()
  const q1 = new SqliteQueue(path)
  const q2 = new SqliteQueue(path, { busyTimeoutMs: 5000 })
  for (let i = 0; i < 20; i++) q1.enqueue({ i })

  const got = []
  for (let i = 0; i < 20; i++) {
    const a = q1.claim({ visibilityMs: 60_000, workerId: 'w1' })
    const b = q2.claim({ visibilityMs: 60_000, workerId: 'w2' })
    if (a) got.push(a.id)
    if (b) got.push(b.id)
  }
  assert.equal(new Set(got).size, got.length, '同一個 id 不能被兩個 worker 拿到')
  assert.equal(got.length, 20, '20 筆都要被拿走')
  q1.close(); q2.close(); cleanup()
})

test('listen：處理完自動 ack，丟錯自動 fail，閒置可停', async () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path, { pollMs: 20 })
  q.enqueue({ n: 1 })
  q.enqueue({ n: 2 })
  q.enqueue({ n: 3 }, { maxAttempts: 1 })

  const done = []
  await q.listen(async (job) => {
    if (job.payload.n === 3) throw new Error('故意失敗')
    done.push(job.payload.n)
  }, { maxIdleMs: 120, visibilityMs: 5000 })

  assert.deepEqual(done.sort(), [1, 2])
  const s = q.stats()
  assert.equal(s.done, 2)
  assert.equal(s.dead, 1, 'maxAttempts=1 的一次失敗就進死信')
  q.close(); cleanup()
})

test('prune 只清已結束的舊紀錄', async () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  const a = q.enqueue('done-me')
  q.claim(); q.ack(a.id)
  q.enqueue('still-ready')
  assert.equal(q.prune({ olderThanMs: -1 }), 1, '立即過期時只清掉那一筆 done')
  assert.equal(q.stats().ready, 1, 'ready 的不能被 prune 清掉')
  q.close(); cleanup()
})

test('線上備份：備份檔可獨立開啟且筆數一致', async () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  for (let i = 0; i < 7; i++) q.enqueue({ i })
  const dest = path.replace(/q\.db$/, 'backup.db')
  await q.backupTo(dest)

  const b = new DatabaseSync(dest)
  const n = b.prepare('SELECT count(*) n FROM jobs').get().n
  assert.equal(n, 7)
  b.close()
  q.close(); cleanup()
})

test('完整性檢查回 ok', () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  q.enqueue('x')
  assert.equal(q.integrityCheck(), 'ok')
  assert.equal(q.integrityCheck({ quick: true }), 'ok')
  q.close(); cleanup()
})

test('checkpoint 回報 busy/log/checkpointed', () => {
  const { path, cleanup } = tmpDb()
  const q = new SqliteQueue(path)
  q.enqueue('x')
  const r = q.checkpoint()
  for (const k of ['busy', 'log', 'checkpointed']) assert.ok(k in r, '要有 ' + k)
  q.close(); cleanup()
})

test('跨進程鎖：持鎖時第二條連線拿不到，放開後可以', () => {
  const { path, cleanup } = tmpDb()
  const q1 = new SqliteQueue(path)
  const q2 = new SqliteQueue(path, { busyTimeoutMs: 30 })

  const unlock = q1.lock()
  assert.throws(() => q2.lock({ busyTimeoutMs: 30 }), '第二條連線應該被擋住')
  unlock()
  unlock() // 重複釋放不可以炸

  const unlock2 = q2.lock({ busyTimeoutMs: 200 })
  unlock2()
  q1.close(); q2.close(); cleanup()
})

