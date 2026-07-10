# 製造仕様書（詳細設計書）

|項目|内容|
|------|------|
|プロジェクト名|流通・小売業向け購買分析・需要予測基盤構築|
|システム名|Retail Analytics Platform|
|サブシステム|販売データETL基盤|
|プログラムID|ETL-SILVER-001|
|プログラム名|販売トランザクション統合処理|
|ジョブ名|job_sales_transaction_etl|
|実行環境|Databricks Runtime 14.3 LTS|
|開発言語|Python 3.11 / PySpark|
|実行方式|Databricks Workflow|
|作成日|2026/07/10|
|作成者|Data Engineering Team|

---

# 1. プログラム概要

## 1.1 目的

AWS S3へ連携された販売データおよび各種マスタデータをDatabricksへ取り込み、
データクレンジング・マスタ結合・売上計算・データ品質チェックを実施し、
Delta Lake Silver Layerへ登録する。

本プログラムは後続のBI分析および機械学習モデルで利用される基礎データを生成する。

---

# 2. システム構成

```text
POS
E-Commerce
CRM
ERP
Web Log
      │
      ▼
AWS S3 Raw Layer
      │
      ▼
Databricks Auto Loader
      │
      ▼
Bronze Layer
      │
      ▼
PySpark ETL
      │
      ▼
Silver Layer
      │
      ▼
Gold Layer
      │
      ├── Power BI
      ├── Tableau
      └── ML Forecast
```

---

# 3. データレイク構成

```text
s3://retail-data-lake/

└── raw/
    ├── pos/
    │   └── transactions/
    │       └── pos_transactions_YYYYMMDD.csv
    │
    ├── ecommerce/
    │   └── online_orders_YYYYMMDD.json
    │
    ├── web/
    │   └── clickstream/
    │       └── web_clickstream_YYYYMMDD.log
    │
    ├── crm/
    │   ├── customer_master.json
    │   ├── loyalty_members.csv
    │   └── coupon_dispatches.json
    │
    ├── erp/
    │   ├── product_master.csv
    │   ├── category_master.csv
    │   ├── supplier_master.csv
    │   ├── store_master.csv
    │   └── inventory_balance.parquet
    │
    └── reference/
        ├── retail_calendar.csv
        └── holiday_calendar.csv
```

---

# 4. 入力ファイル一覧

|No|ファイル|形式|更新頻度|説明|
|---|---------|------|---------|----------------|
|1|pos_transactions|CSV|毎日|POS販売履歴|
|2|customer_master|JSON|毎日|顧客マスタ|
|3|product_master|CSV|毎日|商品マスタ|
|4|store_master|CSV|毎日|店舗マスタ|
|5|inventory_balance|Parquet|毎日|在庫情報|
|6|coupon_dispatches|JSON|毎日|クーポン配信履歴|
|7|retail_calendar|CSV|月次|営業日カレンダー|

---

# 5. 入力パラメータ

|パラメータ|型|必須|説明|
|------------|------|------|----------------|
|job_date|DATE|○|処理対象日|
|environment|STRING|○|dev/test/prod|
|input_path|STRING|○|S3入力パス|
|output_database|STRING|○|Unity Catalog Database|
|batch_id|STRING|○|バッチID|

---

# 6. 入力テーブル仕様

## POS販売データ

|項目名|型|NULL|PK|説明|
|------|------|------|------|----------------|
|transaction_id|STRING|×|○|取引ID|
|customer_id|STRING|○||顧客ID|
|product_id|STRING|×||商品ID|
|store_id|STRING|×||店舗ID|
|quantity|INT|×||数量|
|unit_price|DECIMAL(10,2)|×||単価|
|discount_amount|DECIMAL(10,2)|○||値引額|
|transaction_time|TIMESTAMP|×||購入日時|

---

# 7. 出力テーブル

## silver.fact_sales

|項目|型|説明|
|------|------|----------------|
|transaction_id|STRING|取引ID|
|customer_id|STRING|顧客ID|
|customer_name|STRING|顧客名|
|product_id|STRING|商品ID|
|product_name|STRING|商品名|
|category_name|STRING|カテゴリ|
|store_name|STRING|店舗|
|quantity|INT|数量|
|unit_price|DECIMAL|単価|
|discount_amount|DECIMAL|値引額|
|sales_amount|DECIMAL|売上金額|
|transaction_date|DATE|購入日|
|etl_timestamp|TIMESTAMP|ETL日時|

---

# 8. 処理フロー

```text
開始

↓

ジョブパラメータ取得

↓

S3接続確認

↓

販売データ読込

↓

顧客マスタ読込

↓

商品マスタ読込

↓

店舗マスタ読込

↓

スキーマ検証

↓

NULLチェック

↓

重複チェック

↓

データ型変換

↓

マスタ結合

↓

売上金額算出

↓

監査項目付与

↓

Silver保存

↓

監査ログ登録

↓

終了
```

---

# 9. データ品質チェック

|No|チェック内容|判定条件|エラー時|
|---|----------------|----------------------|----------------|
|1|transaction_id|NULL不可|Reject|
|2|product_id|NULL不可|Reject|
|3|quantity|0以下不可|Reject|
|4|unit_price|マイナス不可|Reject|
|5|transaction_time|未来日時不可|Reject|
|6|重複取引|transaction_id重複|Reject|

---

# 10. データクレンジング

|項目|処理|
|------|----------------|
|前後空白|TRIM|
|文字コード|UTF-8統一|
|NULL文字列|NULLへ変換|
|メールアドレス|小文字変換|
|電話番号|ハイフン除去|

---

# 11. マスタ結合

|結合先|キー|JOIN|
|--------|----------------|-----------|
|customer_master|customer_id|LEFT|
|product_master|product_id|LEFT|
|store_master|store_id|LEFT|
|retail_calendar|transaction_date|LEFT|

---

# 12. 変換仕様

|No|出力項目|変換内容|
|---|-----------|-----------------------------|
|1|sales_amount|(quantity × unit_price) - discount_amount|
|2|transaction_date|DATE(transaction_time)|
|3|year|YEAR(transaction_time)|
|4|month|MONTH(transaction_time)|
|5|year_month|yyyy-MM|
|6|etl_timestamp|current_timestamp()|

---

# 13. カラムマッピング

|入力項目|出力項目|変換|
|-----------|-------------|----------------|
|transaction_id|transaction_id|そのまま|
|customer_id|customer_id|そのまま|
|customer_name|customer_name|マスタ参照|
|product_name|product_name|マスタ参照|
|category_name|category_name|マスタ参照|
|store_name|store_name|マスタ参照|
|quantity|quantity|そのまま|
|unit_price|unit_price|そのまま|
|discount_amount|discount_amount|そのまま|
|quantity × unit_price - discount_amount|sales_amount|計算|

---

# 14. Spark処理仕様

|処理|内容|
|------|----------------|
|Partition|transaction_date|
|Shuffle Partition|200|
|Broadcast Join|product_master|
|Cache|customer_master|
|AQE|有効|
|Optimize Write|有効|

---

# 15. Notebook構成

|Notebook|役割|
|------------------------|----------------------|
|00_initialize|初期設定|
|01_read_sales|販売データ読込|
|02_read_master|マスタ読込|
|03_validation|入力チェック|
|04_transformation|データ変換|
|05_join_master|マスタ結合|
|06_write_silver|Silver保存|
|07_audit_log|監査ログ|

---

# 16. モジュール一覧

|モジュールID|名称|概要|
|------------|----------------|----------------|
|M001|ReadSales|販売データ読込|
|M002|ReadMaster|マスタ読込|
|M003|Validation|入力チェック|
|M004|Transformation|データ変換|
|M005|MasterJoin|マスタ結合|
|M006|WriteDelta|Delta保存|
|M007|AuditLog|監査ログ|

---

# 17. エラーコード

|コード|内容|処理|
|--------|----------------------|----------------|
|E001|S3接続失敗|ジョブ終了|
|E002|入力ファイル不存在|ジョブ終了|
|E003|スキーマ不一致|ジョブ終了|
|E004|データ品質エラー|Reject登録|
|E005|Delta書込失敗|3回リトライ|
|E006|マスタデータ不存在|Warning出力|

---

# 18. Rejectテーブル

|項目|説明|
|------|----------------|
|transaction_id|取引ID|
|error_code|エラーコード|
|error_message|エラー内容|
|source_file|入力ファイル|
|reject_timestamp|Reject日時|

---

# 19. 監査ログ

|項目|説明|
|------|----------------|
|batch_id|バッチID|
|job_name|ジョブ名|
|start_time|開始日時|
|end_time|終了日時|
|input_count|入力件数|
|output_count|出力件数|
|reject_count|Reject件数|
|status|SUCCESS / FAILED|

---

# 20. ログ出力

|レベル|出力内容|
|--------|----------------|
|INFO|ジョブ開始・終了|
|DEBUG|SQL・DataFrame件数|
|WARN|マスタ未存在|
|ERROR|例外情報|

---

# 21. パフォーマンス要件

|項目|値|
|------|----------------|
|想定データ件数|5,000,000件/日|
|処理時間|20分以内|
|Executor数|8|
|Executor Memory|16GB|
|Worker|Auto Scaling|
|Photon|有効|

---

# 22. ジョブ依存関係

|前処理|本処理|後処理|
|------------|----------------------|----------------------|
|POSデータ取込|販売ETL|売上集計|
|顧客マスタ同期|販売ETL|顧客分析|
|商品マスタ同期|販売ETL|需要予測|

---

# 23. 単体テスト

|No|テスト内容|期待結果|
|---|----------------|----------------|
|UT001|正常データ|正常終了|
|UT002|transaction_id NULL|Reject|
|UT003|商品マスタ未登録|Warning|
|UT004|重複transaction_id|Reject|
|UT005|1000万件データ|性能要件達成|

---

# 24. デプロイ手順

|No|作業|
|---|----------------|
|1|Notebook配置|
|2|Unity Catalog登録|
|3|Secret Scope設定|
|4|Cluster作成|
|5|Workflow作成|
|6|単体テスト|
|7|結合テスト|
|8|本番リリース|

---

# 25. 備考

- Bronze Layerには入力データを加工せず保存する。
- Silver Layerではデータ品質チェック、マスタ結合、業務ルール適用を実施する。
- Gold Layerでは売上集計、需要予測用特徴量を生成する。
- Rejectデータは `silver.reject_sales` に保存する。
- 監査ログは `audit.etl_job_log` に登録する。
- Delta Lakeの `OPTIMIZE` および `VACUUM` は別ジョブで定期実行する。
