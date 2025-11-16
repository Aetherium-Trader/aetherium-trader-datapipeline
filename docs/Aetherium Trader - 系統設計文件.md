# Aetherium Trader - 系統設計文件 (SDD)

* **版本:** 1.1
* **日期:** 2025-11-12
* **專案名稱:** `Aetherium Trader`
* **目標:** 設計一個高效能的日内交易模擬系統，初期專注於提供「手動練習」功能，並為未來的「自動回測」和「即時模擬」打下基礎。

## 1. 核心架構

本系統採用**多語言 (Polyglot)** 和 **DDD (Domain-Driven Design)** 的**事件驅動架構**。

* **DDD:** 交易的核心邏輯（倉位、訂單、損益）被封裝在一個獨立的 `Trading` 邊界上下文中。
* **Polyglot:** 為了平衡效能和開發效率：
    * **Rust (`TradingCore`)**: 負責高效能、記憶體安全的核心領域邏輯計算。
    * **Kotlin/Spring Boot (`SimulationBackend`)**: 負責應用程式層、I/O（WebSockets, DB）和協調。
* **架構模式:**
    * 為了支援分散式系統設計，`SimulationBackend` (Kotlin) 服務被設計為**無狀態 (Stateless)**。
    * `TradingCore` (Rust) 服務同樣是**無狀態的**（僅作為計算引擎）。
    * **gRPC** 用於兩個後端服務之間的通訊。
    * **Redis** 被用作外部 Session 儲存，集中管理高頻變動的 `PortfolioState`。

## 2. 技術選型

| 元件 | 技術 | 職責 |
| :--- | :--- | :--- |
| **資料擷取** | **Rust (with C++ FFI)** | 透過 FFI 呼叫 IB C++ API 擷取 Tick。 |
| **原始儲存** | **Parquet (Hourly)** | 每小時滾動一個 `.parquet` 檔案。 |
| **資料庫** | ClickHouse | 儲存原始 Tick；支援即時 K 棒聚合。 |
| **ETL** | Batch Script | 將 Parquet 檔案定期載入 ClickHouse。 |
| **交易核心** | Rust (gRPC Server) | 實作 DDD `Trading` 領域的計算引擎。 |
| **應用後端** | Kotlin + Spring Boot | 實作 REST/WebSocket API、協調者。 |
| **Session 儲存** | **Redis** | 儲存高頻變動的 `PortfolioState`，使後端無狀態。 |
| **前後端通訊** | gRPC | `Backend` (Kotlin) <-> `Core` (Rust) 之間。 |
| **用戶通訊** | WebSocket | `Backend` (Kotlin) <-> `Frontend` (UI) 之間。 |
| **即時 (未來)** | RocketMQ | 用於 v2 的「即時模擬」事件匯流排。 |

## 3. 專案原始碼管理

* **Multi-repo:** `Aetherium Trader` 將由多個獨立的 Git 儲存庫 (Repo) 組成（例如：`aetherium-trader-backend`, `aetherium-trader-core`）。
* **API 契約:** 一個獨立的 `protos` 儲存庫將用來存放 `trading_core.v1.proto` 檔案。
* **`git submodule`:** `Backend` 和 `Core` 專案將使用 `git submodule` 來引入 `protos` 儲存庫，確保 API 契約在所有服務間的絕對一致性。

## 4. 系統元件 (邊界上下文)

### 4.1. `Ingestion` (資料擷取)
* **技術:** Rust (獨立執行檔/Daemon)。
* **職責:**
    * 透過 **C++ FFI (Foreign Function Interface)** 穩定地呼叫 IB 官方 TWS API。
    * 訂閱 NQ 的 Tick 資料。
    * 內建重連與錯誤處理邏輯。
    * 將 Tick 批次序列化並寫入**每小時滾動**的 `.parquet` 靜態檔案 (例如：`NQ_20251112_15.parquet`)。

### 4.2. `ETL` (資料載入)
* **技術:** Batch Script
* **職責:**
    * 由 `cron` 定期 (例如每日凌晨) 觸發。
    * 偵測新的 `.parquet` 檔案。
    * 使用 `clickhouse-client` 將 Parquet 資料高效載入 `AetheriumTrader.ticks` 表。

### 4.3. `MarketData` (ClickHouse 資料庫設計)
* **職責:** 儲存所有原始 Tick，並提供即時 K 棒聚合查詢。
* **`AetheriumTrader.ticks` 表結構:**
    * **核心欄位:** `timestamp` (DateTime64), `symbol` (LowCardinality(String))。
    * **價格:** `bid_price`, `ask_price`, `last_price` (建議使用 `Decimal(10, 4)` 避免浮點數誤差)。
    * **量:** `bid_size`, `ask_size`, `last_size` (UInt32)。
    * **引擎:** `MergeTree()`。
    * **分區鍵 (`PARTITION BY`):** `toYYYYMMDD(timestamp)` (按日分區)。
    * **排序鍵 (`ORDER BY`):** `(symbol, timestamp)` **(效能關鍵)**。
* **K 棒聚合:**
    * 系統**不**儲存預先計算的 K 棒。
    * K 棒將透過 ClickHouse 的**即時聚合** (On-the-fly Aggregation) 產生。
    * 使用 `tumble()` (滾動時間窗口) 搭配 `argMin()` (Open), `max()` (High), `min()` (Low), `argMax()` (Close) 函數來動態生成任何時間週期的 K 棒。

### 4.4. `TradingCore` (交易核心 - Rust)
* **技術:** Rust (gRPC 伺服器，例如 `tonic`)。
* **職責:**
    * **[核心領域]** 實作 DDD `Portfolio` 聚合根的**無狀態**計算。
    * 接收 `PortfolioState` 和 `Tick` / `Order`。
    * 執行領域邏輯（保證金、滑價、LMT 觸發）。
    * 傳回**新的** `PortfolioState` 和 `Fill` / `Rejection` 事件。
* **介面:** `tradingcore.v1` (詳見 6. API 定義)。

### 4.5. `SimulationBackend` (模擬後端 - Kotlin)
* **技術:** Kotlin + Spring Boot (搭配 `spring-boot-starter-data-redis`)。
* **職責:**
    * **[應用程式層]** (`SimulationControl` Context) 協調者。
    * **無狀態服務:** 自身不儲存 Session。
    * 透過 `WebSocket` 與 `Frontend` 通訊。
    * 透過 `gRPC Client` 呼叫 `TradingCore` 服務。
    * 透過 `JDBC` 查詢 `ClickHouse` (`MarketData` Context) 獲取 Tick 和 K 棒。
    * **狀態管理:** 注入 `RedisTemplate`，從 `Redis` 讀取/寫入每個 `SessionID` 對應的 `PortfolioState`。

### 4.6. `Frontend` (使用者介面)
* **技術:** 任何現代前端框架 (React, Vue, etc.)。
* **職責:**
    * 渲染 K 線圖。
    * 提供回放控制（播放、暫停、速度）。
    * 提供下單面板。
    * 透過 WebSocket 接收 `OnTick`, `OnFill`, `OnPositionUpdate`。
    * 透過 WebSocket 發送 `RequestOrder`。

## 5. 關鍵資料流程 (練習模式)

1.  **啟動 (Init):** `Frontend` 呼叫 `Backend (Kotlin)` 的 REST API。`Backend` 呼叫 `Core (Rust)` 的 `Initialize` gRPC 取得 `initialState`。`Backend` 將 `initialState` **寫入 Redis** (Key: `SessionID`)。
2.  **資料流 (Tick Flow):** `Backend` 開啟一個 `ClickHouse` 查詢，並在一個獨立的協程 (Coroutine) 中按設定的速度讀取 `Tick`。
3.  **Tick 處理 (Tick Processing):**
    a. `Backend` 將 `Tick` 透過 WebSocket 推送給 `Frontend` (用於畫圖)。
    b. `Backend` 從 **Redis** 讀取 `currentState` (Key: `SessionID`)。
    c. `Backend` 呼叫 `Core (Rust)` 的 `ApplyTick(currentState, tick)` gRPC。
    d. `Core` 傳回 `(newState, fills)`。
    e. `Backend` 將 `newState` **寫回 Redis** (Key: `SessionID`)。
    f. 如果 `fills` 有內容，`Backend` 將 `OnFill` 和 `OnPositionUpdate` 推送給 `Frontend`。
4.  **下單 (Order Flow):**
    a. `Frontend` 透過 WebSocket 發送 `RequestOrder`。
    b. `Backend` 收到請求，從 **Redis** 讀取 `currentState` (Key: `SessionID`)。
    c. `Backend` 呼叫 `Core (Rust)` 的 `RequestOrder(currentState, currentTick, order)` gRPC。
    d. `Core` 傳回 `(newState, fill/rejection/placed_order)`。
    e. `Backend` 將 `newState` **寫回 Redis**，並將結果推送給 `Frontend`。

## 6. API 定義 (gRPC: `trading_core.v1.proto`)

`TradingCore` (Rust) 服務暴露的 gRPC API (`trading_core.v1.proto`)。

```protobuf
syntax = "proto3";

// 服務 package
package tradingcore.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

// 服務定義: TradingCore (Rust 計算引擎)
service TradingCore {
  // 1. 初始化 PortfolioState
  rpc Initialize (InitializeRequest) returns (InitializeResponse);

  // 2. 應用 Tick (檢查 LMT/STP 觸發)
  rpc ApplyTick (ApplyTickRequest) returns (ApplyTickResponse);

  // 3. 請求下單 (MKT, LMT)
  rpc RequestOrder (RequestOrderRequest) returns (RequestOrderResponse);

  // 4. 請求取消訂單
  rpc RequestCancelOrder (RequestCancelOrderRequest) returns (RequestCancelOrderResponse);
}

// ------------------------------------
// 核心資料結構
// ------------------------------------

// PortfolioState (聚合根)
// 由 Kotlin 持有, 每次呼叫時傳遞
message PortfolioState {
  double cash = 1;
  map<string, Position> positions = 2; // key: symbol
  map<string, Order> orders = 3; // key: order_id
  double realized_pnl = 4;
}

// Position (倉位)
message Position {
  string symbol = 1;
  int32 quantity = 2; // + for long, - for short
  double average_entry_price = 3;
}

// Order (訂單)
message Order {
  string order_id = 1;
  string symbol = 2;
  OrderSide side = 3;
  OrderType type = 4;
  int32 requested_quantity = 5;
  int32 filled_quantity = 6;
  google.protobuf.DoubleValue limit_price = 7;
  OrderStatus status = 8;
}

// Tick (市場資料)
message Tick {
  google.protobuf.Timestamp timestamp = 1;
  string symbol = 2;
  double bid_price = 3;
  int32 bid_size = 4;
  double ask_price = 5;
  int32 ask_size = 6;
  double last_price = 7;
  int32 last_size = 8;
}

// Fill (成交)
message Fill {
  string order_id = 1;
  string symbol = 2;
  OrderSide side = 3;
  int32 quantity = 4;
  double price = 5;
  google.protobuf.Timestamp timestamp = 6;
}

// OrderRejection (拒絕)
message OrderRejection {
  string order_id = 1;
  string reason = 2;
}

// ------------------------------------
// 枚舉 (Enums)
// ------------------------------------
enum OrderSide { SIDE_UNSPECIFIED = 0; BUY = 1; SELL = 2; }
enum OrderType { TYPE_UNSPECIFIED = 0; MARKET = 1; LIMIT = 2; }
enum OrderStatus {
  STATUS_UNSPECIFIED = 0;
  PENDING = 1;
  FILLED = 2;
  PARTIALLY_FILLED = 3;
  CANCELLED = 4;
  REJECTED = 5;
}

// ------------------------------------
// API 請求/回應
// ------------------------------------

message InitializeRequest {
  double initial_cash = 1;
}
message InitializeResponse {
  PortfolioState state = 1;
}

message ApplyTickRequest {
  PortfolioState state = 1; // [In] 當前狀態
  Tick tick = 2;            // [In] 新 Tick
}
message ApplyTickResponse {
  PortfolioState state = 1; // [Out] 更新後狀態
  repeated Fill fills = 2;  // [Out] 此 Tick 觸發的成交
}

message RequestOrderRequest {
  PortfolioState state = 1; // [In] 當前狀態
  Tick current_tick = 2;    // [In] 當前 Tick (MKT 成交用)
  string symbol = 3;
  OrderSide side = 4;
  OrderType type = 5;
  int32 quantity = 6;
  google.protobuf.DoubleValue limit_price = 7;
}
message RequestOrderResponse {
  PortfolioState state = 1; // [Out] 更新後狀態
  oneof result {
    Fill fill = 2;               // MKT 立即成交
    Order placed_order = 3;    // LMT 已掛單
    OrderRejection rejection = 4; // 訂單被拒絕
  }
}

message RequestCancelOrderRequest {
    PortfolioState state = 1;
    string order_id_to_cancel = 2;
}
message RequestCancelOrderResponse {
    PortfolioState state = 1;
    bool success = 2;
    string reason = 3;
}