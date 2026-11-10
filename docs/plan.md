# remote-task-runner 実装計画

`docs/spec.md`（仕様版 1.2）の実装を、PR 単位に分けた計画である。仕様の受入条件 A01〜A49 は、この表のいずれかの行が担当する。

## 前提

- 全 PR を `main` 宛てにし、単体で CI が通る形で都度マージする。依存列の「#N」は「#N が main にマージされた後、main から起点を切る」。
- 共通の検証コマンド（RTR = remote-task-runner）: `cargo fmt --all --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`、GitHub Actions の緑。純粋モジュールは local-ci と同じく行カバレッジ 100% のゲートと cargo-schemata の変異テストに登録する。
- 略称: RTR = remote-task-runner、LC = local-ci、TTC = task-traffic-controller。

## 分割表

| # | repo | タスク | スコープ | 完了条件 | 非スコープ | 依存 |
|---|---|---|---|---|---|---|
| S0 | RTR | 仕様 v1.2 | `docs/spec.md`、`docs/plan.md`（この表と A 対応表） | 仕様 v1.2 を反映（workspace は fast-worktree、基準版を local-ci 0.9.0 / task-traffic-controller 0.5.0 へ、Linux と一時 worker、`.worktreeinclude`、A45〜A48）。公開禁止語の grep が 0 件 | コード | なし |
| S1 | RTR | crate の骨組みと CI | `Cargo.toml`（local-ci 0.9・task-traffic-controller 0.5 を crates.io から）、§18 の module を空で全部作る、clap で §4 の全サブコマンド（未実装は `NOT_IMPLEMENTED` の構造化 error）、`--json` の外枠（schema_version・error code）、`.github/workflows/ci.yml`（fmt・clippy・test を macOS と Linux）、coverage gate と cargo-schemata の枠 | 共通の検証コマンドが緑。`remote-task-runner <各サブコマンド> --json` が schema_version 付きの error を返すテスト | 各機能の中身 | なし |
| X1 | LC・TTC | core の不足調査（読むだけ・完了） | §16 の core API・§17 の公開 API と local-ci 0.9.0 / task-traffic-controller 0.5.0 の差分。結果は仕様 §16・§17 の「0.5.0 / 0.9.0 での状態」列に反映済み | 各 API が有る／名前違いで有る／無いと、必要な変更の一覧。T1・L1a・L1b・L2・L3 のスコープがこれで確定した | 実装 | なし |
| S2 | RTR | 契約（ID・状態・wire） | §10 の ID 型、状態機械と terminal outcome・観測状態・receipt、error code、§11 の全操作の request/response DTO（schema_version・digest） | 純粋モジュールとしてカバレッジ 100%・変異テスト。状態遷移表のテスト | 通信・保存の実装 | S1 |
| S3 | RTR | 設定 | §5 のユーザー設定・プロジェクト設定、優先順位、未知キーの error、`.local-ci.toml` の fallback と矛盾の出所表示、`[toolchain]`・`[source] secret_files`・`[[outputs]]` の型 | 設定例 2 つが通る、未知キー・矛盾のテスト | 設定の利用側 | S1 |
| S4 | RTR | storage | SQLite（WAL）、migration と互換表、親 DB・worker DB の schema、条件付き terminal 確定、event journal、retire tombstone の永続保持 | 未知 version を書き換えないテスト、terminal が 1 回だけ確定する競合テスト | 配置・通信 | S2 |
| S5 | RTR | protocol 転送層 | 長さ prefix JSON frame、`hello` の version・feature 照合、control を log が塞がない多重化、chunk 転送の offset・digest 再開、`worker --stdio` の Unix socket bridge、SSH 起動（argv をシェル文字列にしない） | 切断・再開・非互換 version のテスト | 各操作の業務処理 | S2 |
| S6 | RTR | source capture と object store | `local_ci::capture_source` を使った worktree / commit / directory、private object store、§6 の SourceManifest、SOURCE_BUSY、未 push objects の bundle、submodule・LFS、展開時の path 安全性 | A11〜A14 相当のテスト（一時 Git repo の自作 fixture） | worker での展開 | S2（X1 で capture に不足があれば L 行も） |
| S7 | RTR | worker の workspace | repo ごとの canonical clone、attempt workspace を fast-worktree で作る・消す（プロジェクトの `.fast-worktree.toml` と `.worktreeinclude` をそのまま使う）、directory snapshot、`exec --in-place`（親 local worker 限定） | macOS と Linux で add / remove が通るテスト、元 worktree を書き換えないテスト（A48） | setup template（S17） | S6 |
| S8 | RTR | secret_files | §22.3 の SecretBundle（cluster 鍵の HMAC）、親での 0600 保持と削除、転送操作、0600 配置、terminal 時と reconcile 時の消去、ログの mask | A43・A47 のテスト | toolchain | S5・S6 |
| S9a | RTR | supervisor（寿命・lease・停止） | attempt ごとの別プロセス、task-traffic-controller の managed `register` / `try_admit` / `run_command`、launch intent、process identity（boot ID・開始時刻）、suspend を含む単調時計の lease、renew の冪等性、cancel の TERM→KILL と停止確認、3 種の timeout、実行中の sleep 抑止 | A20・A21・A22・A25・A26（時計）のテスト。OS 依存部分（単調時計・boot ID・process identity・sleep 抑止）の macOS 版と Linux 版（A45） | log spool（S9b） | S4 |
| S9b | RTR | log spool と結果記録 | stdout / stderr の disk spool、stream ID・offset・sequence、非 UTF-8、上限 1 GiB と `log_truncated`、結果と artifact の attempt 別保存 | A29 のテスト | 回収（S14） | S4 |
| S10 | RTR | worker agent | §11 の worker 側操作すべて、`attempt_id + payload_hash` の冪等受理、retire tombstone、再起動時の supervisor adopt、boot ID 変化で interrupted、`ack_collected` までの GC 保護 | A18・A19・A23・A24（擬似）のテスト | forecast（S11）・導入（S22） | S4・S5・S9a・S9b |
| S11 | RTR | worker forecast | task-traffic-controller の forecast に受理済み未登録の overlay を重ねる、coverage（attempt・plan_digest・expansion_seq・node）、読み取り専用、TTL と epoch 失効、measured / fallback / unknown | A06〜A10 のテスト | 配置の判断（S13） | S10・X1 |
| S12 | RTR | coordinator の中核 | durable queue（run・homework・attempt）、prefetch / max_active を守る dispatch、未 ACK 需要を同じ transaction に記録、retire してから再配置、unknown の保持、epoch と再起動時の reconcile・adopt、cancel の伝達 | A19・A21・A22・A23 のテスト（親側） | 予測による選択（S13）・回収（S14） | S4・S5・S10 |
| S13 | RTR | scheduler | 1 actor で配置を直列化、転送＋setup＋実行＋回収の予測完了時間で比較、同点規則、途中参加 worker の取り込み、配置理由の表示 | A01〜A04 のテスト（1 台上の複数 worker） | CI 固有の処理 | S11・S12 |
| S14 | RTR | 結果の回収 | collecting 状態、artifact の digest 検証と atomic 公開、`ack_collected`、回収だけの再開、完全性検査と `no_work`、保持 7 日の GC | A30・A31 のテスト | GitHub status（S19） | S9b・S12 |
| S15 | RTR | 利用者コマンド | `init`・`exec`・`submit`・`ls`・`show`・`logs --follow`・`wait`・`cancel`（Ctrl-C）・`retry`・`outputs`・`estimate`、JSON と exit code | 各コマンドの統合テスト、`exec` の exit code 透過 | `ci`（S16）・`workers`（S21〜S23） | S12・S14 |
| S16 | RTR | CI 統合 | `ci` / `ci plan`、FrozenPlan を 1 回だけ作る、Job DAG の配置と needs output、dynamic matrix の展開 event、host をまたぐ中間 artifact の receipt（workflow 単位）、matrix の global permit・max-parallel・fail-fast・join・決定的な集約、分割できない理由の表示 | A10・A27・A28・A38 のテスト | test sharding（S18） | S13・S14・L1a・L1b |
| S17 | RTR | setup と template | SetupRecipe / PreparedTemplate receipt、worker 内 singleflight の builder（builder_id・consumer 参照・独自 lease）、immutable template と書き込み分離の複製、`--no-cache`、`[prepare]`。fast-worktree の `[[clone]]` との境界は仕様 v1.2 に従う | A15〜A17 のテスト | toolchain の導入（S20） | S7・S9a・S10 |
| L1a | LC | step 実行の差し込み口 | `exec::Opts` に `CommandExecutor` 相当の trait（`StepRequest` を受けて `StepExit` を返す）を足す。executor が無いときは従来どおりバイナリを子プロセスで起動する。pipe fd（token の受け渡し）と process group の扱いを trait の契約に明記する。local-ci は task-traffic-controller の crate に依存しない | local-ci の CI（coverage・変異）緑、executor 未設定の挙動がバイト単位で不変（A36）、`cargo publish` まで | L1b・L2・L3 | なし |
| L1b | LC | task demand 列挙と wire DTO | `plan_task_demand`（task-traffic-controller の型に依存しない DTO。条件式・動的 matrix は `conditional`）、`SetupRecipe`・`PreparedReceipt`・`TaskResult`・`TaskInputs`・`ExecutionTask`・`JobGraph` の schema_version と digest、wire から host path を除く（`StepResult.log`・`RunResult.run_dir`）、`RunResult` の `Deserialize` | local-ci の CI 緑、既存 CLI の JSON 出力は不変（A36）、`cargo publish` まで | test sharding（L2・L3） | L1a・L2 |
| L2 | LC | test sharding（土台＋cargo test・Vitest・Jest） | `partition_tests`、adapter の discover / partition / invoke / collect / merge、履歴による長い順の割り当てと決定的な fallback、全件 1 回の集合検証 | A39・A40 のテスト（同梱 example） | Playwright・Cypress・coverage merge | X1 |
| L3 | LC | test sharding（Playwright・Cypress・coverage merge・分割禁止条件） | 2 adapter、runner 対応の coverage merge、DB / port / container を隔離できない E2E の分割禁止 | A41 のテスト、`cargo publish` まで | RTR 側の配置 | L2 |
| T1 | TTC | 分散用 API の不足分 | `exec::Opts` にenv map（`env_clear` 後に設定）とstdio（stdout・stderrの追記先log path、stdinはnull）を足し、`managed::run_command` まで通す。`None` 既定で既存CLIの挙動を変えず、履歴keyにenvを混ぜない。任意: `legacy_clock`、inspect の history quality | task-traffic-controller の CI 緑、env 隔離・log 追記・lease と marker が残るテスト、既存 CLI の挙動不変、`cargo publish` まで | RTR 側 | なし |
| S18 | RTR | sharding の配置 | ShardGroup の配置と集約、fail-fast、失敗 Shard の retry、通過済み結果の保持 | A39〜A41 の統合テスト | adapter 本体 | S16・L3 |
| S19 | RTR | GitHub status | 親だけが投稿、公開 commit と検査入力の一致検証、dirty / synthetic は success にしない、計算結果と投稿失敗を分けて報告 | A32 のテスト | local-ci 単体の status の変更（必要なら L 行を追加） | S16 |
| S20 | RTR | toolchain | §22.1 の版指定の読み取り、baseline、match 規則、adapter（nodenv・rustup・mise・fvm・corepack）の detect / resolve / install / activate、setup と同じ経路の導入、`blocked: environment_mismatch` | A33・A42 のテスト | diff / sync（S21） | S10・S17 |
| S21 | RTR | `workers diff` / `sync` | §22.2 の差分項目と 5 分類、sync は自動導入できる差だけ、`doctor` へ含める | A44 のテスト | 導入・常駐（S22） | S20・S22 |
| S22 | RTR | 導入と常駐（macOS） | `workers add --install`（SSH bootstrap、版別ディレクトリ）、LaunchAgent / LaunchDaemon（`UserName`）、start / stop / drain / resume / `stop --cancel-running` | 1 台上での install → start → stop の統合テスト | upgrade・doctor（S23）・Mac 以外（S24） | S10 |
| S23 | RTR | upgrade・rollback・doctor | drain → 配置 → 互換検査 → 切替 → health → resume、失敗時の rollback、doctor（task-traffic-controller runtime の互換・policy digest・state path・Docker の resource domain・service 環境での profile 検査） | A05・A34・A36 のテスト | Mac 以外 | S22 |
| S24 | RTR | Linux の常駐と Colab の一時 worker | Linux の systemd user service（install / start / stop / upgrade）、Colab の一時 worker（`worker --ephemeral` をノートブックから起動、利用者が用意したトンネル経由で親が SSH、起動ごとに新しい identity、`workers forget` で消滅を宣言したときだけ interrupted に確定） | Linux（CI の ubuntu）での install → start → stop の統合テスト、ephemeral の identity と forget のテスト | macOS の常駐（S22）、トンネルの提供 | S22・S23 |
| S25 | RTR | 1 台上の試験ハーネスと障害注入 | 複数 worker（別 state dir）を 1 台で立てる、kill 点・ACK 消失・時計のずれの注入口、A 対応表のうち実機不要のものを自動試験にまとめる | 実機不要の A がすべて自動で緑 | 実機の試験（S26） | S15・S16・S18・S23 |
| S26 | RTR | 実機での受入と初回リリース | 手元の Mac ＋ 2 台目の Mac（＋ S24 の Linux・一時 worker）で A01・A02・A24・A26・A34・A35・A37・A45・A46、結果の報告、0.1.0 の `cargo publish` とタグ・Release | §19 の全 A が緑、§20 の完了報告 | なし（最終行） | 全行、実機（2 台目の Mac）の用意 |

並列に走るもの: 最初は S0・S1・X1 の 3 本（S0 は回答待ち）。S1 が入ると S2・S3、S2 が入ると S4・S5・S6 が並ぶ。

## A01〜A49 の対応

| A | 担当行 | 実機要 |
|---|---|---|
| A01〜A04 | S13（A01 は S26 でも） | A01・A02 |
| A05 | S9a・S23 | |
| A06〜A10 | S11（A10 は S16 も） | |
| A11〜A14 | S6・S7（A13 は S5 も） | |
| A15〜A17 | S17 | |
| A18〜A19 | S10・S12 | |
| A20〜A23 | S9a・S10・S12 | |
| A24 | S10（擬似）・S26 | ○ |
| A25 | S9a | |
| A26 | S9a・S26 | ○ |
| A27・A28・A38 | S16 | |
| A29 | S9b・S5 | |
| A30・A31 | S14 | |
| A32 | S19 | |
| A33 | S20・S8 | |
| A34・A35 | S22・S23・S26 | ○ |
| A36 | L1a・L1b・L2・L3・T1・S23 | |
| A37 | S26 | ○ |
| A39〜A41 | L2・L3・S18 | |
| A42 | S20 | |
| A43 | S8 | |
| A44 | S21 | |
| A45 | S9a・S10（OS 依存部分）・S24 | ○（S26 の Linux） |
| A46 | S24・S26 | ○ |
| A47 | S8 | |
| A48 | S7 | |
| A49 | S13（配置）・S27（設定） | |

## 確定事項（仕様 v1.2 に反映済み）

- Linux（x86_64・arm64）を正式対応する（systemd user service）。Google Colab は常駐なしの一時 worker とする。OS 依存部分（suspend を含む単調時計、boot ID、process identity、sleep 抑止）は platform module に macOS と Linux の実装を置き、S9a・S10 から両方で動かす。
- `.worktreeinclude` に一致する ignored ファイルは、自動で secret_files と同じ経路で送る。`secret_files` には追加分だけを書く。
- 実機（2 台目の Mac）は S26 までに用意する。それまでは 1 台上の複数 worker と障害注入で進める。
