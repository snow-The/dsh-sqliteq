/**
 * sqliteq — 零依賴的 SQLite WAL 任務佇列（Node 內建 `node:sqlite`，不需要 better-sqlite3）。
 *
 * 為什麼要自己寫
 * -------------
 * TS/Node 生態的佇列（BullMQ / WakaQ / pg-boss / Graphile / RRQ）預設的部署單位是
 * 「一台有 Redis 或 PostgreSQL 的商業伺服器」。我們是**單機、單進程、多插件共用同一個
 * 檔案系統**；在這種規模下多開一個 Redis 只為了「等一件事發生」，是拿一個服務換一個 `UPDATE`。
 *
 * 語意照抄誰（不是憑感覺設計的）
 * ---------------------------
 * - **goqite**（Go, ~542★）：用純 SQL 事務做 SQS 風格的 **visibility timeout** ——
 *   認領就是把 `visible_at` 推到未來，超時自動重新可見。它自己宣稱本地 SQLite 約
 *   18,500 次/秒（送+收+刪）；**那個數字是它的 repo 講的，我們沒有複測**。
 * - **Liteq**（Python）：至少一次投遞 + 可見性超時 + 自動死信，是功能對照清單；
 *   強制 WAL、用 `BEGIN IMMEDIATE` 拿寫鎖。
 * - **Honker**（Rust, ~3k★）：把 `.db-wal` 檔本身當信號源（每 1ms stat）而不是輪詢資料表。
 *   我們用它當**喚醒**的靈感，但只用 `fs.watch` + 一個短的保底輪詢——
 *   `fs.watch` 跨平台一致性不如 stat 輪詢，所以這裡不把它當唯一依賴。
 *
 * 刻意不做的事
 * -----------
 * - 不做 worker 之間的心跳/租約仲裁（那是 Honker → LeaseLedger 那一級的問題）。
 *   我們的崩潰模式是「整台機器重開」，不是「某個 worker 卡死但要保住它的鎖」。
 * - 不做跨機器。要跨機器就用 Go 那支（`dshbus`）或再加一層。
 * - 不自動重試無限次：`max_attempts` 到了就進死信，讓它**看得見**。
 */
import { DatabaseSync, backup as sqliteBackup } from 'node:sqlite'
import { watch } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { dirname } from 'node:path'
import { mkdirSync } from 'node:fs'

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id           TEXT PRIMARY KEY,
  queue        TEXT NOT NULL DEFAULT 'default',
  payload      TEXT NOT NULL,
  dedupe_key   TEXT,
  state        TEXT NOT NULL,             -- ready | inflight | done | dead
  attempts     INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  visible_at   INTEGER NOT NULL,          -- epoch ms；<= now 才可被認領
  claimed_by   TEXT,
  claimed_at   INTEGER,
  created_at   INTEGER NOT NULL,
  finished_at  INTEGER,
  last_error   TEXT
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(queue, state, visible_at, created_at);
-- 冪等鍵只在「還沒結束」的時候唯一：完成之後同一個鍵可以再用。
CREATE UNIQUE INDEX IF NOT EXISTS jobs_dedupe
  ON jobs(dedupe_key) WHERE dedupe_key IS NOT NULL AND state IN ('ready','inflight');
`

const DEFAULTS = {
  queue: 'default',
  maxAttempts: 5,
  visibilityMs: 30_000,
  backoffBaseMs: 250,
  backoffMaxMs: 60_000,
  pollMs: 250,
  busyTimeoutMs: 5_000,
}

export class SqliteQueue {
  #db
  #closed = false
  #watchers = []
  #wake = null // 目前正在等信號的那個 resolve

  constructor(path, opts = {}) {
    this.path = path
    this.opts = { ...DEFAULTS, ...opts }
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true })
    this.#db = new DatabaseSync(path)
    // WAL：讀寫不互相阻塞，這是整個設計的前提。
    this.#db.exec('PRAGMA journal_mode = WAL')
    // NORMAL：WAL 下崩潰只可能丟最後幾個交易，換到的是大幅減少的 fsync。
    // 我們的用途是「工作項」不是「帳務」，這個取捨是刻意的。
    this.#db.exec('PRAGMA synchronous = NORMAL')
    this.#db.exec(`PRAGMA busy_timeout = ${Number(this.opts.busyTimeoutMs)}`)
    this.#db.exec('PRAGMA foreign_keys = ON')
    this.#db.exec(SCHEMA)
    if (path !== ':memory:') this.#armWatchers()
  }

  /** 本機寫入的信號源。watcher 只是加速，保底仍有 pollMs 輪詢。 */
  #armWatchers() {
    const files = [this.path, `${this.path}-wal`]
    for (const f of files) {
      try {
        const w = watch(f, { persistent: false }, () => this.#ring())
        w.on('error', () => { /* 檔案還沒建立/被輪替：忽略，保底輪詢會接手 */ })
        this.#watchers.push(w)
      } catch { /* 這個檔案來看不了就算了 */ }
    }
  }

  #ring() {
    const r = this.#wake
    this.#wake = null
    if (r) r()
  }

  #tx(fn) {
    this.#db.exec('BEGIN IMMEDIATE')
    try {
      const out = fn()
      this.#db.exec('COMMIT')
      return out
    } catch (e) {
      try { this.#db.exec('ROLLBACK') } catch { /* 已經結束了 */ }
      throw e
    }
  }

  /**
   * 放一筆工作。
   * @returns {{id: string, deduped: boolean}}
   */
  enqueue(payload, { queue, delayMs = 0, dedupeKey = null, maxAttempts } = {}) {
    const now = Date.now()
    const q = queue || this.opts.queue
    const body = typeof payload === 'string' ? payload : JSON.stringify(payload)
    const ma = Number.isFinite(maxAttempts) ? maxAttempts : this.opts.maxAttempts
    const id = randomUUID()
    try {
      this.#db.prepare(
        `INSERT INTO jobs(id,queue,payload,dedupe_key,state,attempts,max_attempts,visible_at,created_at)
         VALUES(?,?,?,?,'ready',0,?,?,?)`,
      ).run(id, q, body, dedupeKey, ma, now + Math.max(0, delayMs), now)
      this.#ring()
      return { id, deduped: false }
    } catch (e) {
      // 唯一索引撞到 = 同一個冪等鍵已經在排隊/在處理中。這是「已經有人做了」，不是錯誤。
      if (String(e.message || '').includes('UNIQUE')) {
        const row = this.#db.prepare(
          `SELECT id FROM jobs WHERE dedupe_key=? AND state IN ('ready','inflight')`,
        ).get(dedupeKey)
        if (row) return { id: row.id, deduped: true }
      }
      throw e
    }
  }

  /**
   * 認領一筆。認領＝把 visible_at 推到未來（visibility timeout）。
   * 包含 state='inflight' 但租約已過期的，所以「至少一次」成立。
   */
  claim({ queue, visibilityMs, workerId } = {}) {
    const now = Date.now()
    const q = queue || this.opts.queue
    const vis = Number.isFinite(visibilityMs) ? visibilityMs : this.opts.visibilityMs
    const wid = workerId || `${process.pid}`
    return this.#tx(() => {
      const row = this.#db.prepare(
        `UPDATE jobs
            SET state='inflight', claimed_by=?, claimed_at=?, attempts=attempts+1, visible_at=?
          WHERE id = (
            SELECT id FROM jobs
             WHERE queue=? AND state IN ('ready','inflight') AND visible_at<=? AND attempts < max_attempts
             ORDER BY visible_at, created_at LIMIT 1
          )
        RETURNING *`,
      ).get(wid, now, now + vis, q, now)
      if (!row) return null
      return { ...row, attempt: row.attempts, payload: safeParse(row.payload) }
    })
  }

  ack(id) {
    const now = Date.now()
    const r = this.#db.prepare(
      `UPDATE jobs SET state='done', finished_at=?, claimed_by=NULL WHERE id=? AND state='inflight'`,
    ).run(now, id)
    this.#ring()
    return r.changes > 0
  }

  /**
   * 失敗。未達 max_attempts → 回到 ready（指數退避）；達到 → dead（死信）。
   * 死信不刪，讓它看得見；清理走 prune()。
   */
  fail(id, error) {
    const now = Date.now()
    const row = this.#db.prepare('SELECT attempts, max_attempts FROM jobs WHERE id=?').get(id)
    if (!row) return { requeued: false, dead: false }
    const msg = String(error && error.message ? error.message : error || 'unknown').slice(0, 2000)
    if (row.attempts >= row.max_attempts) {
      this.#db.prepare(`UPDATE jobs SET state='dead', finished_at=?, last_error=?, claimed_by=NULL WHERE id=?`)
        .run(now, msg, id)
      this.#ring()
      return { requeued: false, dead: true }
    }
    const delay = Math.min(this.opts.backoffMaxMs, this.opts.backoffBaseMs * 2 ** Math.max(0, row.attempts - 1))
    this.#db.prepare(
      `UPDATE jobs SET state='ready', visible_at=?, last_error=?, claimed_by=NULL, claimed_at=NULL WHERE id=?`,
    ).run(now + delay, msg, id)
    this.#ring()
    return { requeued: true, dead: false, retryInMs: delay }
  }

  stats({ queue } = {}) {
    const rows = queue
      ? this.#db.prepare('SELECT state, count(*) n FROM jobs WHERE queue=? GROUP BY state').all(queue)
      : this.#db.prepare('SELECT state, count(*) n FROM jobs GROUP BY state').all()
    const out = { ready: 0, inflight: 0, done: 0, dead: 0 }
    for (const r of rows) out[r.state] = r.n
    return out
  }

  /** 清掉已結束的紀錄。預設只清超過 1 天的。 */
  prune({ olderThanMs = 86_400_000 } = {}) {
    const cutoff = Date.now() - olderThanMs
    const r = this.#db.prepare(`DELETE FROM jobs WHERE state IN ('done','dead') AND finished_at < ?`).run(cutoff)
    return r.changes
  }

  /** 等一個信號（本機寫入 / 檔案變動），或最多等 ms。 */
  wait(ms = this.opts.pollMs) {
    return new Promise((resolve) => {
      let done = false
      const fin = () => { if (!done) { done = true; clearTimeout(t); this.#wake = null; resolve() } }
      const t = setTimeout(fin, ms)
      if (t.unref) t.unref()
      this.#wake = fin
    })
  }

  /**
   * 跑起來：認領 → handler → 自動 ack（丟錯則 fail）。
   *
   * 刻意**不**提供「手動 ack」：一個工作如果有兩條結束路徑（我 ack 了沒？），
   * 就會出現「看起來完成、其實沒 ack」的狀態——那正是我們在別處踩過的假回報。
   */
  async listen(handler, { queue, visibilityMs, workerId, signal, maxIdleMs } = {}) {
    const q = queue || this.opts.queue
    let idleSince = Date.now()
    while (!this.#closed) {
      if (signal && signal.aborted) break
      const job = this.claim({ queue: q, visibilityMs, workerId })
      if (!job) {
        if (maxIdleMs && Date.now() - idleSince > maxIdleMs) break
        await this.wait(this.opts.pollMs)
        continue
      }
      idleSince = Date.now()
      try {
        await handler(job, this)
        this.ack(job.id)
      } catch (e) {
        this.fail(job.id, e)
      }
    }
  }

  // ── 加強 SQLite 的四件事（全部用 node:sqlite 內建，不需要叢集）────────────
  //
  // 這四支是照著 rqlite / dqlite / Hiqlite 讀完之後的結論：那三個專案解決的是
  // **多機一致性**，我們不需要；但「單機也要能備份、能驗、能節流、能互斥」是同一份清單裡
  // 真正可轉移的部分，而且全部不需要新增任何依賴。

  /**
   * 線上備份到另一個檔案（SQLite online backup API，不需要停寫）。
   * 用途：備份前不必停掉消費者。
   */
  async backupTo(dest) {
    await sqliteBackup(this.#db, dest)
    return dest
  }

  /** 完整性檢查。正常回 'ok'；有問題回描述字串（不會丟錯）。 */
  integrityCheck({ quick = false } = {}) {
    const row = this.#db.prepare(quick ? 'PRAGMA quick_check' : 'PRAGMA integrity_check').get()
    return row ? String(Object.values(row)[0]) : 'unknown'
  }

  /** 把 WAL 併回主檔並截斷。搬移資料庫、或 WAL 長太大時跑一次。 */
  checkpoint() {
    return this.#db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get()
  }

  /**
   * 跨進程互斥（Hiqlite 的 `dlock` 概念，單機版）。
   *
   * 原理：`BEGIN IMMEDIATE` 會立刻拿寫鎖，第二條連線會被 `busy_timeout` 擋住。
   * 回傳一個 `unlock()`；**務必用 try/finally**。
   *
   * ⚠️ 持鎖期間**不要**呼叫 enqueue/claim/ack/fail——那些方法自己會開交易，
   *    嵌套交易會直接報錯。這支是給「外部臨界區」用的，不是給佇列內部用的。
   */
  lock({ busyTimeoutMs = 5000 } = {}) {
    this.#db.exec(`PRAGMA busy_timeout = ${Number(busyTimeoutMs)}`)
    this.#db.exec('BEGIN IMMEDIATE')
    let released = false
    return () => {
      if (released) return
      released = true
      this.#db.exec('COMMIT')
    }
  }

  close() {
    this.#closed = true
    for (const w of this.#watchers) { try { w.close() } catch { /* ignore */ } }
    this.#watchers = []
    this.#ring()
    try { this.#db.close() } catch { /* ignore */ }
  }
}

function safeParse(s) {
  try { return JSON.parse(s) } catch { return s }
}

export function openQueue(path, opts) { return new SqliteQueue(path, opts) }
