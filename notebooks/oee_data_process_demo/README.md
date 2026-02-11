# Manufacturing OEE Demo – Delta Lake / Medallion Architecture

## 目的 (Objective)
本 Notebook 用於模擬製造業產線資料，並以 **Bronze → Silver → Gold** 的 Medallion Architecture
示範如何在 Databricks / Spark 環境中計算 **OEE (Overall Equipment Effectiveness)** 指標。

資料為全模擬資料（Mock Data），邏輯參考：
https://www.kaggle.com/code/dubltap/factory-oee-downtime-a-beginner-s-guide-with-s/notebook

---

## 架構概覽 (Architecture Overview)
```md
flowchart LR
    subgraph Source["Data Source (Simulated)"]
        S1["Machine Sensors<br/>(Per-minute data)"]
    end

    subgraph Bronze["Bronze Layer – Raw Data"]
        B1["mfg_bronze_sensor<br/><br/>• timestamp<br/>• machine<br/>• product_id<br/>• is_running<br/>• units / good_units / scrap<br/>• cause_* (wide columns)"]
    end

    subgraph Silver["Silver Layer – Cleaned Events"]
        S2["mfg_silver_events<br/><br/>• day<br/>• shift (A/B/C)<br/>• machine<br/>• product_id<br/>• is_running<br/>• units / good_units / scrap<br/>• downtime_reason"]
    end

    subgraph Dim["Dimension Tables"]
        D1["mfg_dim_products<br/><br/>• product_id<br/>• product_name<br/>• base_rate<br/>• scrap_rate"]
    end

    subgraph Gold["Gold Layer – Business Metrics"]
        G1["mfg_gold_oee<br/><br/>• day / shift<br/>• machine / product<br/>• availability<br/>• performance<br/>• quality<br/>• oee"]
    end

    S1 --> B1
    B1 -->|Clean & Normalize| S2
    S2 -->|Aggregate & KPI Calc| G1
    D1 -->|Join| G1
```

### Bronze Layer – Raw Sensor Data
- 粒度：**每分鐘 / 每台機器**
- 內容：
  - 機器狀態 (is_running)
  - 生產數量 (units, good_units, scrap)
  - 停機原因（寬表：cause_Mechanical, cause_Electrical, ...）
  - 產品別 (product_id)
- 特性：
  - 僅做最小處理
  - 保留模擬時的結構與雜訊

Table:
- `mfg_bronze_sensor`

---

### Silver Layer – Cleaned Production Events
- 粒度：**每分鐘 / 每台機器 / 班次**
- 轉換內容：
  - 停機原因寬表 → 單一欄位 `downtime_reason`
  - 衍生欄位：
    - `day`
    - `shift` (A / B / C)
- 特性：
  - 結構標準化
  - 可直接作為分析或聚合輸入

Table:
- `mfg_silver_events`

---

### Gold Layer – OEE Aggregation
- 粒度：**每日 / 班次 / 機器 / 產品**
- 指標：
  - Availability
  - Performance
  - Quality
  - OEE
- 維度：
  - Machine
  - Product (Join `mfg_dim_products`)

Table:
- `mfg_gold_oee`

---

## OEE 計算公式

- Availability = Runtime Minutes / Total Minutes
- Performance = min(1, Actual Output / (Runtime Minutes × Standard Rate))
- Quality = Good Units / Total Units
- OEE = Availability × Performance × Quality

---

## 維度表 (Dimension Tables)

### Product Dimension
- `mfg_dim_products`
- 包含：
  - 標準產能速率 (base_rate)
  - 廢品率 (scrap_rate)
  - 產品名稱

---

## 使用情境 (Use Cases)
- 製造資料管線示範 (ETL / ELT)
- Delta Lake Medallion 架構 Demo
- OEE 指標計算 PoC
- Databricks Notebook 教學

---

## 注意事項
- 所有資料皆為模擬資料
- 生產與停機邏輯已刻意加入隨機性與噪音
- 不代表任何真實工廠數據

