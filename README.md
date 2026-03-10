# OpenData Weather UA

以桌面 UI 管理測站清單與 API 金鑰，並提供本機 OPC UA 服務給 SCADA/UAExpert 讀取。

## 功能

- 桌面 UI（Tkinter）
- 測站新增、編輯、刪除
- 顯示中央氣象署開放資料欄位
- 啟動時自動帶起 OPC UA Server
- 視窗最小化時縮到系統列（tray）
- 右上角 `X` 會跳出置中確認視窗
- 關閉主程式時會一併停止 OPC UA Server
- 不產生 `log / err / out / pid` 類暫存檔
- `config.json` 不存在時自動產生內建預設設定

## 專案結構

`main.py`：CLI 入口（`ui` / `server`）

`ui/desktop_ui.py`：桌面 UI、tray、啟停 server

`server/opcua_server.py`：OPC UA 服務與資料同步

`config.json`：資料來源、測站與 OPC UA 端點設定

## 環境需求

- Windows 10/11（建議）
- Python 3.12+

## 安裝

```powershell
cd E:\py\opendata_weather_ua
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv pip install --python .\.venv\Scripts\python.exe -r requirements.txt
```

## 執行

啟動桌面 UI（預設）：

```powershell
cd E:\py\opendata_weather_ua
.\.venv\Scripts\python.exe .\main.py
```

或明確指定：

```powershell
.\.venv\Scripts\python.exe .\main.py ui
```

只啟動 OPC UA Server：

```powershell
.\.venv\Scripts\python.exe .\main.py server
```

## OPC UA 設定

預設 endpoint 來自 `config.json`：

`opc.tcp://127.0.0.1:48480`（可在 UI 的 Config 變更）

UAExpert 建議：

- Security Mode: `None`
- Security Policy: `None`
- User: `Anonymous`

遠端鏡射（固定啟用）：

- 端點：`opc.tcp://lioil.ddnsfree.com:48484`
- 測站映射：`466900->W466900`、`466920->W466920`、`467050->W467050`、`467571->W467571`、`467441->W467441`

## 設定檔說明

`config.json` 主要欄位：

- `openData.address`：氣象資料 API 主機
- `openData.api`：資料集代碼（例如 `O-A0003-001`）
- `openData.auth_key`：授權碼
- `openData.stations`：測站陣列（`id`, `name`）
- `opcUA.url`：OPC UA endpoint
- `opcUA.bind_ip`：可選，指定綁定 IP

## 常見問題

若最小化沒有進系統列：

- 請確認已安裝 `pystray` 與 `Pillow`
- 某些系統可能把 tray icon 折疊在隱藏圖示中

若 OPC UA 啟動失敗：

- 檢查 `48480` 是否被其他程式占用
- 確認 `config.json` 的 `opcUA.url` 格式正確

## 開發備註

- 本專案目前為桌面版流程，不包含 web UI。
