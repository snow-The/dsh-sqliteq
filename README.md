# dsh-sqliteq

零依賴的 **SQLite WAL 任務佇列**，給 Node / TypeScript 用。
用 Node 內建的 `node:sqlite`，**不需要 better-sqlite3、不需要 Redis、不需要 PostgreSQL。**

```js
import { SqliteQueue } from '@snow-the/dsh-sqliteq'

const q = new SqliteQueue('C:/Users/snow/.dsh/queue.db')
q.enqueue({ kind: 'reindex', target: 'alexlib' })

await q.listen(async (job) => {
  console.log('doing', job.payload, 'attempt', job.attempt)
  // 正常結束 → 自動 ack；丟錯 → 自動 fail（退避重排，超過次數進死信）
}, { visibilityMs: 30_000 })
```

## 為什麼要自己寫

TS/Node 生態的佇列（BullMQ / WakaQ / pg-boss / Graphile Worker / RRQ）預設的部署單位是
**一台有 Redis 或 PostgreSQL 的商業伺服器**。我們是單機、單進程、多插件共用同一個檔案系統——
為了「等一件事發生」再開一個 Redis，是拿一個服務換一個 `UPDATE`。

## 量到的數字（2026-11-07，本機，`node bench/bench.mjs 5000`）

| 操作 | 吞吐 | 單筆 |
|---|---|---|
| enqueue（200 B payload） | **19,982 次/秒** | 0.050 ms |
| claim + ack | **11,438 次/秒** | 0.087 ms |

參考：goqite（Go）宣稱本地 SQLite 約 18,500 次/秒（送+收+刪）——**那是它 repo 的數字，不是我們複測的**；
上面兩個才是我們自己量的。

## 語意

- **可見性超時（visibility timeout）**：認領＝把 `visible_at` 推到未來；超時自動重新可見。
- **至少一次投遞**：因為超時會重投，處理器必須自備冪等（或用 `dedupeKey`）。
- **退避重試**：`fail()` 走指數退避（預設 250 ms 起、上限 60 s）。
- **死信**：attempts 達 `maxAttempts` 後進 `dead`，**不刪**，讓它看得見；清理走 `prune()`。
- **冪等鍵**：同一個 key 在「未結束」期間只會有一筆；結束後可重複使用。

## API

| 方法 | 說明 |
|---|---|
| `new SqliteQueue(path, opts)` | `path` 可以是 `:memory:` |
| `enqueue(payload, { queue, delayMs, dedupeKey, maxAttempts })` | 回 `{ id, deduped }` |
| `claim({ queue, visibilityMs, workerId })` | 回 job 或 `null` |
| `ack(id)` / `fail(id, error)` | 只對 `inflight` 有效 |
| `stats({ queue })` | `{ ready, inflight, done, dead }` |
| `prune({ olderThanMs })` | 清已結束的舊紀錄 |
| `listen(handler, { queue, visibilityMs, maxIdleMs, signal })` | 自動 ack/fail |

## 設計上的取捨（寫給以後想改的人）

1. **不提供手動 ack**。一個工作如果有兩條結束路徑（我 ack 了沒？），就會出現
   「看起來完成、其實沒 ack」的狀態——那正是我們在別處踩過的假回報。`listen()` 只有一條：
   handler 正常返回＝ack，丟錯＝fail。
2. **`synchronous = NORMAL`**。WAL 下崩潰只可能丟最後幾個交易，換到大幅減少的 fsync。
   我們存的是「工作項」不是「帳務」，這個取捨是刻意的。
3. **喚醒用 `fs.watch` ＋ 保底輪詢**。Honker（Rust）把 `.db-wal` 當信號源、每 1 ms stat；
   我們用它當靈感，但不把 `fs.watch` 當唯一依賴——它跨平台一致性不如 stat 輪詢。
   所以最短延遲是 `pollMs`（預設 250 ms），檔案變動可提早喚醒。
4. **不做跨機器、不做 worker 租約仲裁**。崩潰模式是「整台機器重開」，不是「某個 worker 卡死
   但要保住它的鎖」。真的要跨機器就用 Go 那支（`dshbus`，服務 7331）。

## 測試

```bash
node --test test/*.test.mjs   # 10 個測試
```

涵蓋：WAL 真的開啟、可見性超時重投、兩條連線不重複認領、退避→死信、冪等鍵、延遲入列、
`listen` 自動 ack/fail、prune 只清已結束的。
