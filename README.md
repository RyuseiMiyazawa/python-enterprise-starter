# python-enterprise-starter

単なる CRUD テンプレートではなく、複雑な依存関係を持つ処理群を型安全に扱うための非同期ワークフロー実行基盤です。

## Features

- 非同期 DAG 実行
- トポロジカル検証と循環検出
- タスク単位のリトライポリシー
- 実行中イベントのストリーム購読
- TTL 付きメモリキャッシュ
- SQLite 実行履歴永続化
- プラグイン式ワークフロー登録
- JSON DSL コンパイル
- 標準ライブラリだけで動く HTTP 制御プレーン
- `Bearer` トークン認可
- 保存済み実行からのトレースエクスポート
- 静的 Web UI ダッシュボード
- 実行結果のサマリとクリティカルパス計測
- CLI デモ付き

## Quick Start

```bash
cd /Users/rm/python-enterprise-starter
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'
PYTHONPATH=src python3 -m enterprise_starter workflows
PYTHONPATH=src python3 -m enterprise_starter run advanced-analytics
PYTHONPATH=src python3 -m enterprise_starter dsl-run examples/analytics_workflow.json
PYTHONPATH=src python3 -m enterprise_starter serve --port 8080 --api-token secret-token
PYTHONPATH=src python3 -m enterprise_starter history
```

HTTP API:

- `GET /workflows`
- `GET /runs?limit=10`
- `GET /traces/<run_id>`
- `GET /ui`
- `POST /runs/<workflow_name>`
- `POST /dsl/runs`

認可を使う場合は `Authorization: Bearer <token>` を付与します。
ブラウザ UI / SSE では `?access_token=<token>` も使えます。

## Architecture

- `domain`: ワークフロー定義、イベント、例外
- `application`: オーケストレーションエンジン
- `application.dsl`: JSON DSL から `WorkflowSpec` を構築
- `infrastructure`: キャッシュやイベントバスなどの実装
- `interfaces`: CLI と HTTP API

詳細は [docs/architecture.md](./docs/architecture.md) を参照。
