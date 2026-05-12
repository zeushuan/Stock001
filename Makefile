# Stock001 — 驗證測試 targets
.PHONY: test-rs test-rs-cov test-rs-snapshot all-tests

# 跑全部 RS 驗證測試
test-rs:
	pytest tests/rs_validation/ -v

# 跑測試並產出覆蓋率報告
test-rs-cov:
	pytest tests/rs_validation/ -v --cov=rs_line --cov=sepa_vcp --cov-report=html --cov-report=term

# 重新下載基準快照（yfinance）
test-rs-snapshot:
	python scripts/snapshot_baseline_data.py

# 全部測試
all-tests:
	pytest tests/ -v
