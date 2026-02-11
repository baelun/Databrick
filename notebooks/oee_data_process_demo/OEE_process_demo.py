# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 0 – Environment Reset
# MAGIC
# MAGIC 清除既有的 Bronze / Silver / Gold Tables  
# MAGIC 確保 Notebook 可重複執行（Idempotent）

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS default.mfg_bronze_sensor")
spark.sql("DROP TABLE IF EXISTS default.mfg_silver_events")
spark.sql("DROP TABLE IF EXISTS default.mfg_gold_oee")
spark.sql("DROP TABLE IF EXISTS default.mfg_sliver_events")


# COMMAND ----------

# MAGIC %md
# MAGIC Mockdata logic ref : https://www.kaggle.com/code/dubltap/factory-oee-downtime-a-beginner-s-guide-with-s/notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Product Dimension Table
# MAGIC
# MAGIC 定義產品主檔：
# MAGIC - 標準生產速率 (base_rate)
# MAGIC - 廢品率 (scrap_rate)
# MAGIC - 產品名稱
# MAGIC
# MAGIC 此表將在 Gold Layer 用於：
# MAGIC - OEE Performance 計算
# MAGIC - 分析產品別生產效率

# COMMAND ----------

# 1. 建立維度表 (Dim Table)
PRODUCT_CONFIG = {
    "PROD_A": {"base_rate": 10, "scrap_rate": 0.01, "name": "Widget High-Speed"},
    "PROD_B": {"base_rate": 8,  "scrap_rate": 0.05, "name": "Complex Gadget"},
    "PROD_C": {"base_rate": 6,  "scrap_rate": 0.02, "name": "Standard Part"}
}
# 2. 建立維度表時直接引用
dim_data = [(k, v["base_rate"], v["scrap_rate"], v["name"]) for k, v in PRODUCT_CONFIG.items()]
df_dim_products = spark.createDataFrame(dim_data, ["product_id", "base_rate", "scrap_rate", "product_name"])
df_dim_products.write.mode("overwrite").saveAsTable("mfg_dim_products")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Synthetic Sensor Data Generation (Bronze)
# MAGIC
# MAGIC 模擬條件：
# MAGIC - 時間粒度：每分鐘
# MAGIC - 期間：30 天
# MAGIC - 機器數：4 台 (M1–M4)
# MAGIC - 每 8 小時切換一次產品（對應班次）
# MAGIC
# MAGIC 包含：
# MAGIC - 機器運轉狀態
# MAGIC - 停機原因（Mechanical / Electrical / Changeover / …）
# MAGIC - 生產數量與良率（依產品參數動態變化）
# MAGIC
# MAGIC 此層僅模擬「原始感測資料」，不做商業邏輯聚合

# COMMAND ----------

# 
# 模擬資料
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# Simulation horizon: 3 days, per-minute resolution (fixed: use "min" not "T")
DAYS = 30
FREQ = "min"  # minute frequency; future-proof
MACHINES = ["M1", "M2","M3","M4"]
DOWNTIME_CAUSES = ["Mechanical", "Electrical", "Changeover", "Blocked", "Starved", "Quality"]


start = pd.Timestamp.now().floor("D") - pd.Timedelta(days=DAYS)
time_index = pd.date_range(start, periods=DAYS*1440, freq=FREQ)

def shift_name(ts):
    h = ts.hour
    if 6 <= h < 14: return "A"
    if 14 <= h < 22: return "B"
    return "C"

def synth_machine_v2(machine):
    df = pd.DataFrame({"timestamp": time_index, "machine": machine, "is_running": 1})
    # 模擬：每個班次隨機更換一種產品生產
    # 1. 建立停機原因欄位
    for c in DOWNTIME_CAUSES: 
        df[f"cause_{c}"] = 0
    # 2. 班次與產品分配
    # 使用 floor("8H") 確保每 8 小時換一次產品
    df["shift_id"] = df["timestamp"].dt.floor("8H") 
    unique_shifts = df["shift_id"].unique()
    shift_prod_map = {s: np.random.choice(list(PRODUCT_CATALOG.keys())) for s in unique_shifts}
    df["product_id"] = df["shift_id"].map(shift_prod_map)
    
    # 3. 映射產品參數 (不再依賴外部參數，而是依賴 PRODUCT_CATALOG)
    df["prod_base_rate"] = df["product_id"].map(lambda x: PRODUCT_CATALOG[x][0])
    df["prod_scrap_rate"] = df["product_id"].map(lambda x: PRODUCT_CATALOG[x][1])

    # 停機邏輯 
    i, n = 0, len(df)
    while i < n:
        if np.random.rand() < 0.005 and df.loc[i, "is_running"] == 1:
            L = np.random.randint(5, 40)
            cause = np.random.choice(DOWNTIME_CAUSES)
            j = min(i + L, n)
            df.loc[i:j, "is_running"] = 0
            df.loc[i, f"cause_{cause}"] = 1
            i = j
        else: i += 1

    # 5. 生產量與良率計算 (修正點：使用 df 內的動態參數)
    # 注意：np.random.poisson 可以接受一個 Series 作為參數      
  
    df["units"] = np.where(
        df["is_running"] == 1, 
        np.random.binomial(n=df["prod_base_rate"], p=0.95), 
        0
    )

# 3. 執行並寫入 Bronze
   # 修正：根據不同產品的廢品率產生隨機值
    random_vals = np.random.rand(n)
    df["scrap"] = (random_vals < df["prod_scrap_rate"]).astype(int) * (df["units"] > 0) # scrap 欄位的內容會等於隨機值與 units 欄位的比較結果
    df["good_units"] = df["units"] - df["scrap"]


    return df.drop(columns=["prod_base_rate", "prod_scrap_rate", "shift_id"])

# 6. 使用 list comprehension 產生所有機器的資料並合併
all_data = [synth_machine_v2(m) for m in MACHINES]
raw_pandas = pd.concat(all_data, ignore_index=True)

spark_df = spark.createDataFrame(raw_pandas)
spark_df.write.format("delta").mode("overwrite").saveAsTable("mfg_bronze_sensor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Bronze to Silver Transformation
# MAGIC
# MAGIC 轉換重點：
# MAGIC - 停機原因欄位：
# MAGIC   - 寬表 (cause_xxx) → 單一欄位 (downtime_reason)
# MAGIC - 新增分析用欄位：
# MAGIC   - day
# MAGIC   - shift (A / B / C)
# MAGIC
# MAGIC Silver Layer 目的：
# MAGIC - 提供乾淨、可重複使用的事件資料
# MAGIC - 作為 OEE 與其他 KPI 的基礎

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("mfg_bronze_sensor")

# 整合停機原因欄位
cause_cols = [f"cause_{c}" for c in ["Mechanical", "Electrical", "Changeover", "Blocked", "Starved", "Quality"]]

silver_raw = bronze_df.withColumn(
    # 整合停機原因：將寬表轉為單一原因欄位
    "downtime_reason",
    F.expr(f"CASE {' '.join([f'WHEN {c}=1 THEN \"{c[6:]}\"' for c in cause_cols])} ELSE NULL END")
    ).withColumn(
        "day", F.to_date("timestamp")
    ).withColumn(
        # 根據小時定義班次 (A: 06-14, B: 14-22, C: 22-06)
        "shift", 
        F.expr("""
            CASE WHEN hour(timestamp) >= 6 AND hour(timestamp) < 14 THEN 'A'
                WHEN hour(timestamp) >= 14 AND hour(timestamp) < 22 THEN 'B'
                ELSE 'C' END
        """)
    ).select(
        "timestamp", 
        "day", 
        "shift", 
        "machine", 
        "product_id",  # 務必保留，以便後續 Join 維度表
        "is_running", 
        "units", 
        "good_units", 
        "scrap", 
        "downtime_reason"
    )


# 3. 直接寫入 (不要再用 createDataFrame)
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("mfg_silver_events")

# 查看結果
display(spark.table("mfg_silver_events").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 – Silver to Gold Aggregation (OEE)
# MAGIC
# MAGIC 資料處理：
# MAGIC - Join 產品維度表
# MAGIC - 依 day / shift / machine / product 聚合
# MAGIC
# MAGIC 計算指標：
# MAGIC - Availability
# MAGIC - Performance
# MAGIC - Quality
# MAGIC - OEE
# MAGIC
# MAGIC Gold Layer 特性：
# MAGIC - 商業指標導向
# MAGIC - 可直接供 Dashboard / BI 使用
# MAGIC
# MAGIC ![image_1770769213669.png](./img/image_1770769213669.png "image_1770769213669.png")
# MAGIC ![image_1770769255419.png](./img/image_1770769255419.png "image_1770769255419.png")

# COMMAND ----------


# 讀取 Silver 與 維度表
silver = spark.table("mfg_silver_events")
products = spark.table("mfg_dim_products")

# Join 並計算指標
gold_oee = silver.join(products, "product_id") \
    .groupBy("day", "shift", "machine", "product_id", "product_name") \
    .agg(
        F.sum("is_running").alias("runtime_mins"),
        F.count("*").alias("total_mins"),
        F.sum("units").alias("actual_output"),
        F.sum("good_units").alias("good_output"),
        F.first("base_rate").alias("std_rate")
    ).withColumn(
        "availability", F.col("runtime_mins") / F.col("total_mins")
    ).withColumn(
        "performance", F.least(F.lit(1.0), F.col("actual_output") / (F.col("runtime_mins") * F.col("std_rate")))
    ).withColumn(
        "quality", F.col("good_output") / F.col("actual_output")
    ).withColumn(
        "oee", F.col("availability") * F.col("performance") * F.col("quality")
    )

display(gold_oee.limit(10))

gold_oee.write.format("delta").mode("overwrite").saveAsTable("mfg_gold_oee")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 – Set BI Dashboard (OEE)
# MAGIC
# MAGIC 資料處理：
# MAGIC - 依 day / shift / machine / product 聚合的結果，選擇對應圖表
# MAGIC
# MAGIC 計算指標：
# MAGIC - Availability
# MAGIC - Performance
# MAGIC - Quality
# MAGIC - OEE
# MAGIC
# MAGIC 步驟(以databrick 內部dashboard為例)：
# MAGIC 1. 點選 左側 Dashboards 工具新建 Dashboard
# MAGIC 2. Data 部分選擇剛剛計算完的 mfg_gold_oee 表
# MAGIC 3. 接下來就可以在Dashboard中針對常見的指標進行設定
# MAGIC
# MAGIC ![image_1770770257780.png](./image_1770770257780.png "image_1770770257780.png")
