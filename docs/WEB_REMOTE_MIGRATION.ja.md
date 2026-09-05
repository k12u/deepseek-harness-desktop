# Web / Remote 運用移行メモ（Harness 0.1.2 対応）

> このメモは、server + iOS クライアント運用を DeepSeek Harness 0.1.2 系に
> 移行するために必要な変更点をまとめたもの。手順の詳細は
> [STANDALONE_WEB_REMOTE.md](STANDALONE_WEB_REMOTE.md) を参照。

## 背景: Harness 0.1.2 の 3 つの破壊的変更

npm で公開されている 0.1.2 系列（alpha.2 以降すべて）には、iOS からの
直接接続を壊す変更が同時に入っている:

1. **起動 token 認証**（Jupyter 方式）— `dsh web` が起動時に
   `dsh web: http://127.0.0.1:<port>/?token=<secret>` を出力し、最初の
   `GET /?token=` が 30 日cookie を発行。以後の全リクエストは cookie 必須
   （token 無しのアクセスはすべて 401）
2. **RPC パスの改名** — `POST /api/session.list` → `POST /api/session/list`、
   payload は `{args: {...}}` 形式が必須
3. **ライブ経路と応答 shape の変更** — WebSocket が `events.mux` →
   `remote.mux` に移動、`workspace.list` / `session.history` /
   `session.models` / `host.describe` は廃止または再設計

401 が最初に見えるだけで、2・3 も隠れて壊れている。

## 対処方針

Harness の前に**翻訳 proxy を 1 つ挟む**。iOS アプリ・Android アプリは
無修正（Remote v1 契約を維持）。ブラウザは Harness 直接受けのまま。

| 用途 | エンドポイント | 認証 |
| --- | --- | --- |
| ブラウザ（Web UI） | Harness 直接受け | 初回だけ `?token=` 付き URL → 30 日 cookie（harness 再起動でも有効） |
| iPhone / Android | 翻訳 proxy（新設） | tailnet メンバーシップ（任意で bearer） |

## サーバー側の変更点

1. **ランチャー新設**: `scripts/standalone-web-remote.mjs`
   - `dsh web` を spawn → readiness 行から起動 token をキャプチャ（ログは自動 redact）
   - `scripts/lan-remote-proxy.mjs` を起動（token → cookie 交換 + Remote v1 → 0.1.2 翻訳を内蔵）
   - harness 再起動時に token を再取得して proxy を張り直す
   - ブラウザ用 bootstrap URL を起動時に 1 回だけ表示
2. **依存の固定**: Node.js 22.19+。ランチャーと proxy は `ws` に依存
   （このリポジトリの `npm ci` で揃う。最小構成でコピーする場合は
   scripts フォルダで `npm install ws`）
3. **Tailscale Serve を 2 本に**:

   ```bash
   tailscale serve --bg --https=443  http://127.0.0.1:8080   # ブラウザ → harness 直
   tailscale serve --bg --https=8443 http://127.0.0.1:8766   # iPhone  → proxy
   ```

4. **常駐化**: `docs/STANDALONE_WEB_REMOTE.md` の systemd unit 例を使用
5. **セキュリティ**: bootstrap URL（token 付き）はログに残さない。
   bearer で絞りたい場合は `--remote-token`（`STANDALONE_WEB_REMOTE.md` 参照）

## iOS 側の変更点

- **アプリの再ビルド・再配布は不要**
- 「HTTPS address」を proxy 側の Serve ポートに変更するだけ:
  `https://<machine>.ts.net:8443/`
- token 入力・QR スキャンとも不要

## 未検証事項

- 実 LLM プロバイダを接続した状態での承認（`respond`）フローと、
  承認/質問イベントの `agentId` ＝ `sessionId` の対応
  （実 provider 設定後に一度実機確認を推奨）
- リアル LLM ストリーミング中の chunk 展開（単体テストでは検証済み）

## デスクトップアプリ（macOS ビルド）を使う場合

Desktop 向けの一式（token 解析・翻訳 proxy 組み込み・依存の
`@deepseek-ai/dsh@0.1.2-rc.1` 更新）は本リポジトリに実装済み。
Desktop が内包する runtime と phone の両方が 0.1.2 対応になる。
