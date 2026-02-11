# Databricks 學習與實作紀錄

此儲存庫整合了所有 Microsoft Databricks 的練習與技術實作，包含 ETL 流水線、Spark SQL、Delta Lake 及機器學習等單元。

---

## 目錄結構

- `notebooks/`: 存放所有 Notebook 實作（建議導出為 .py 格式）
  - `01_basics/`: 基礎操作、Cluster 配置與環境建置
  - `02_data_processing/`: Spark DataFrame API 與 SQL 練習
  - `03_delta_lake/`: Delta Lake ACID 特性、版本切換 (Time Travel)
  - `04_etl_pipelines/`: 資料清理與轉化流程
  - `05_machine_learning/`: MLflow 實驗紀錄與模型訓練
- `configs/`: 存放作業設定與環境參數檔案
- `data/`: 存放練習用的樣品資料集 (Sample Data)

---

## 學習進度表

| 章節 | 實作主題 | 狀態 | 核心技術 |
| :--- | :--- | :--- | :--- |
| 01 | 基礎環境建置 | ✅ 完成 | Workspace, Cluster |
| 02 | 資料讀取與處理 | 🚧 進行中 | PySpark, DataFrame API |
| 03 | Delta Lake 實作 | 📅 待辦 | ACID, Time Travel |
| 04 | 定期作業調度 | 📅 待辦 | Databricks Jobs |

---

## 開發規範與注意事項

### 1. 版本控制
為了在 GitHub 上獲得更好的 Diff 檢視效果，請將 Notebook 以 **Source File (.py)** 格式同步，而非 `.ipynb`。

### 2. 安全性建議 (Security)
* **嚴禁** 將任何 Azure 原生密鑰、存取碼 (Access Keys) 寫死在程式碼中。
* 請統一使用 Databricks Secrets 進行管理：
  ```python
  # 正確用法範例
  storage_key = dbutils.secrets.get(scope="my-scope", key="storage-key")
  ```

### 3. 環境需求
- Databricks Runtime: 12.2 LTS 或更高版本
- Python: 3.9+
- Spark: 3.3+

### 4. 參考資源
[azure-databricks](https://learn.microsoft.com/zh-tw/azure/databricks/)
  
