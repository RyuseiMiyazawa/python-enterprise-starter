# Architecture

## Core idea

このリポジトリの中心は、依存グラフを持つ複数タスクを非同期に実行する `WorkflowEngine` です。
各タスクは名前、依存先、リトライ設定、TTL キャッシュ設定、そして `ExecutionContext` を受け取る実行関数を持ちます。

## Execution model

1. ワークフロー定義を検証し、循環や未知の依存先を拒否します。
2. 依存関係から indegree を構築し、実行可能なタスクをキュー化します。
3. 同時実行数をセマフォで制御しながら、依存が満たされたタスクを順次起動します。
4. 実行イベントを `EventBus` に流し、結果と失敗を収集します。
5. キャッシュ対象タスクは成功結果を TTL 付きで保存します。
6. 実行結果サマリは `SQLiteWorkflowRunStore` に保存でき、CLI から後追い確認できます。

## Plugin model

`WorkflowRegistry` はファクトリを登録し、`register_workflows(registry)` を持つモジュールを動的ロードできます。
このためワークフロー定義を本体から切り離して追加でき、エンジンとワークフロー設計を疎結合に保てます。

## JSON DSL

`JsonWorkflowCompiler` は JSON から `WorkflowSpec` を生成します。
各タスクは `operation` 名と `params` を持ち、`OperationRegistry` がそれを実行可能な非同期ハンドラへ変換します。

これにより、ワークフロー定義を Python コードから分離しつつ、コンパイル後は同じエンジンで実行できます。

## HTTP control plane

`ControlPlaneHandler` は標準ライブラリの `ThreadingHTTPServer` 上で動作します。
ワークフロー一覧参照、履歴参照、登録済みワークフロー実行、DSL 経由のアドホック実行を HTTP から操作できます。

## Auth and trace model

制御プレーンは任意の `Bearer` トークンで保護できます。
保存済み `payload_json` から `RunTraceExporter` が擬似スパン列を再構成し、`/traces/<run_id>` で返せます。

この方式は本物の OpenTelemetry SDK 依存を持たず、ローカルだけでトレースの考え方をコードに落とし込む設計です。

## Why this is intentionally complex

- 非同期制御とグラフアルゴリズムを同居させている
- エラー時にワークフロー全体の整合性を保つ
- 実行中タスクと未実行タスクの状態遷移を明示している
- 結果サマリにクリティカルパスと所要時間統計を含めている
