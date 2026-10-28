-- ─────────────────────────────────────────────────────────────
-- hangyodb: qcom_sales / qcom_stock_on_hand / qcom_po
-- PK was the SOURCE id (varchar). Now:
--   rec_code   = source record id, NOT NULL, UNIQUE (the ETL upsert key)
--   <pk>       = int unsigned AUTO_INCREMENT surrogate PK (the ETL never writes it)
-- Existing rows keep their source id in rec_code; updated_at is preserved.
-- Run once (applied 2026-10-01). Backups: <table>_bak_20261001.
-- ─────────────────────────────────────────────────────────────

-- ── qcom_sales ──
CREATE TABLE qcom_sales_bak_20261001 LIKE qcom_sales;
INSERT INTO qcom_sales_bak_20261001 SELECT * FROM qcom_sales;
UPDATE qcom_sales SET rec_code = qcom_sales_id, updated_at = updated_at;
ALTER TABLE qcom_sales DROP PRIMARY KEY, CHANGE COLUMN qcom_sales_id old_source_id varchar(50) NULL;
ALTER TABLE qcom_sales
  ADD COLUMN qcom_sales_id int unsigned NOT NULL AUTO_INCREMENT COMMENT 'Surrogate PK' FIRST,
  ADD PRIMARY KEY (qcom_sales_id),
  MODIFY COLUMN rec_code varchar(100) NOT NULL COMMENT 'Source record id (unique, upsert key)' AFTER qcom_sales_id,
  DROP COLUMN old_source_id;                       -- uk_qcom_rec_code (UNIQUE) already exists

-- ── qcom_stock_on_hand ──
CREATE TABLE qcom_stock_on_hand_bak_20261001 LIKE qcom_stock_on_hand;
INSERT INTO qcom_stock_on_hand_bak_20261001 SELECT * FROM qcom_stock_on_hand;
UPDATE qcom_stock_on_hand SET rec_code = qcom_stock_id, updated_at = updated_at;
ALTER TABLE qcom_stock_on_hand DROP PRIMARY KEY, CHANGE COLUMN qcom_stock_id old_source_id varchar(50) NULL;
ALTER TABLE qcom_stock_on_hand
  ADD COLUMN qcom_stock_id int unsigned NOT NULL AUTO_INCREMENT COMMENT 'Surrogate PK' FIRST,
  ADD PRIMARY KEY (qcom_stock_id),
  MODIFY COLUMN rec_code varchar(100) NOT NULL COMMENT 'Source record id (unique, upsert key)' AFTER qcom_stock_id,
  ADD UNIQUE KEY uk_qcom_stock_rec_code (rec_code),
  DROP COLUMN old_source_id;

-- ── qcom_po ──
CREATE TABLE qcom_po_bak_20261001 LIKE qcom_po;
INSERT INTO qcom_po_bak_20261001 SELECT * FROM qcom_po;
UPDATE qcom_po SET rec_code = qcom_po_id, updated_at = updated_at;
ALTER TABLE qcom_po DROP PRIMARY KEY, CHANGE COLUMN qcom_po_id old_source_id varchar(50) NULL;
ALTER TABLE qcom_po
  ADD COLUMN qcom_po_id int unsigned NOT NULL AUTO_INCREMENT COMMENT 'Surrogate PK' FIRST,
  ADD PRIMARY KEY (qcom_po_id),
  MODIFY COLUMN rec_code varchar(100) NOT NULL COMMENT 'Source record id (unique, upsert key)' AFTER qcom_po_id,
  ADD UNIQUE KEY uk_qcom_po_rec_code (rec_code),
  DROP COLUMN old_source_id;

-- when everything is verified:
-- DROP TABLE qcom_sales_bak_20261001, qcom_stock_on_hand_bak_20261001, qcom_po_bak_20261001;
