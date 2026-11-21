// 量自己的數字，不要引別人的宣稱。
// 跑法：node bench/bench.mjs [N]
import { mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { SqliteQueue } from '../lib/queue.js'

const N = Number(process.argv[2] || 5000)
const dir = mkdtempSync(join(tmpdir(), 'sqliteq-bench-'))
const path = join(dir, 'q.db')
const q = new SqliteQueue(path)

const now = () => Number(process.hrtime.bigint()) / 1e6
const rate = (n, ms) => `${n} 次 / ${ms.toFixed(0)}ms = ${(n / (ms / 1000)).toFixed(0)} 次/秒`

let t = now()
for (let i = 0; i < N; i++) q.enqueue({ i, body: 'x'.repeat(200) })
const tEnq = now() - t

t = now()
let claimed = 0
for (let i = 0; i < N; i++) {
  const j = q.claim({ visibilityMs: 60_000 })
  if (!j) break
  q.ack(j.id)
  claimed++
}
const tCons = now() - t

// 單筆交易（不 ack）看起來更接近佇列的「取用」成本
t = now()
for (let i = 0; i < Math.min(N, 2000); i++) q.enqueue({ i })
const tEnq2 = now() - t

const size = statSync(path).size
const walSize = (() => { try { return statSync(path + '-wal').size } catch { return 0 } })()

console.log('N =', N)
console.log('enqueue (200B payload) :', rate(N, tEnq), `  ${(tEnq / N).toFixed(3)} ms/筆`)
console.log('claim+ack              :', rate(claimed, tCons), `  ${(tCons / claimed).toFixed(3)} ms/筆`)
console.log('enqueue (小 payload)    :', rate(Math.min(N, 2000), tEnq2))
console.log('db size                :', (size / 1024).toFixed(0) + ' KB', ' wal:', (walSize / 1024).toFixed(0) + ' KB')
console.log('stats                  :', JSON.stringify(q.stats()))

q.close()
rmSync(dir, { recursive: true, force: true })
