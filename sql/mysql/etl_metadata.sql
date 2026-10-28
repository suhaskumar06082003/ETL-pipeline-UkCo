-- ─────────────────────────────────────────────────────────────
-- ETL metadata tables (tracking / lineage) for the new blob->MySQL ETL.
-- These are NEW to this ETL (not part of the retailverse schema), so they must
-- be created inside each client database that the pipeline writes to.
--
-- Run this against the target database, e.g. on the dev server:
--   USE ullasnirantaradb;   -- (or: mysql -h <MYSQL_HOST> -u <MYSQL_USER> -p ullasnirantaradb < etl_metadata.sql)
--
-- Both pipelines share these 2 tables; pipeline_type tells them apart:
--   MYSQL     blob -> MySQL ETL (uploads, qcom)      file_type = OUTLETDUMP, ...
--   BIGQUERY  MySQL -> BigQuery sync (4 AM DAG)      file_type = BIGQUERY_SYNC
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `etl_task` (
  `task_id`              VARCHAR(32)  NOT NULL,
  `pipeline_type`       VARCHAR(20)   NOT NULL DEFAULT 'MYSQL' COMMENT 'MYSQL / BIGQUERY',
  `client_code`         VARCHAR(50)   DEFAULT NULL,
  `file_type`           VARCHAR(50)   DEFAULT NULL,
  `ext_erp_rec_code`    VARCHAR(100)  DEFAULT NULL,
  `original_filename`   VARCHAR(512)  DEFAULT NULL,
  `size_bytes`          BIGINT        DEFAULT NULL,
  `status`              VARCHAR(30)   DEFAULT NULL,   -- PENDING/INGESTED/PROCESSING/COMPLETED/FAILED
  `current_stage`       VARCHAR(50)   DEFAULT NULL,
  `cutoff_date`         DATETIME(6)   DEFAULT NULL COMMENT 'BIGQUERY: rows updated on/after this date+time were synced (NULL = full load)',
  `row_count`           INT           DEFAULT NULL,
  `column_count`        INT           DEFAULT NULL,
  `valid_record_count`  INT           DEFAULT NULL,
  `invalid_record_count` INT          DEFAULT NULL,
  `stored_format`       VARCHAR(20)   DEFAULT NULL,   -- e.g. parquet
  `blob_name`           VARCHAR(1024) DEFAULT NULL,
  `blob_url`            TEXT          DEFAULT NULL,
  `error`               TEXT          DEFAULT NULL,
  `created_by`          INT           DEFAULT NULL,
  `updated_by`          INT           DEFAULT NULL,
  `created_at`          DATETIME(6)   DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at`          DATETIME(6)   DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`task_id`),
  KEY `idx_task_ft_status` (`file_type`, `status`),   -- incremental watermark lookup
  KEY `idx_task_client` (`client_code`),
  KEY `idx_task_pipeline` (`pipeline_type`, `client_code`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `etl_task_stage` (
  `id`         BIGINT       NOT NULL AUTO_INCREMENT,
  `task_id`    VARCHAR(32)  NOT NULL,
  `pipeline_type` VARCHAR(20) NOT NULL DEFAULT 'MYSQL' COMMENT 'MYSQL / BIGQUERY',
  `file_type`  VARCHAR(50)  DEFAULT NULL COMMENT 'etl_task.file_type of this task',
  `stage`      VARCHAR(50)  DEFAULT NULL,
  `status`     VARCHAR(30)  DEFAULT NULL,
  `load_mode`  VARCHAR(20)  DEFAULT NULL COMMENT 'BIGQUERY: FULL / INCREMENTAL',
  `row_count`  INT          DEFAULT NULL COMMENT 'rows at this stage; LOAD_<table> = records sent to insert/upsert',
  `error`      TEXT         DEFAULT NULL,
  `created_at` DATETIME(6)  DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  KEY `idx_stage_task` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- ─────────────────────────────────────────────────────────────
-- Upgrade a DB that already has the old etl_task / etl_task_stage
-- (existing rows get pipeline_type = MYSQL)
-- ─────────────────────────────────────────────────────────────
-- ALTER TABLE etl_task
--   ADD COLUMN `pipeline_type` VARCHAR(20) NOT NULL DEFAULT 'MYSQL' COMMENT 'MYSQL / BIGQUERY' AFTER `task_id`,
--   ADD COLUMN `cutoff_date` DATE DEFAULT NULL COMMENT 'BIGQUERY: rows changed on/after this day were synced (NULL = full load)' AFTER `current_stage`,
--   ADD KEY `idx_task_pipeline` (`pipeline_type`, `client_code`, `status`);
-- ALTER TABLE etl_task_stage
--   ADD COLUMN `pipeline_type` VARCHAR(20) NOT NULL DEFAULT 'MYSQL' COMMENT 'MYSQL / BIGQUERY' AFTER `task_id`,
--   ADD COLUMN `load_mode` VARCHAR(20) DEFAULT NULL COMMENT 'BIGQUERY: FULL / INCREMENTAL' AFTER `status`;
-- 2026-10-01: BigQuery cutoff is date + time (was DATE)
-- ALTER TABLE etl_task MODIFY COLUMN `cutoff_date` DATETIME(6) DEFAULT NULL
--   COMMENT 'BIGQUERY: rows updated on/after this date+time were synced (NULL = full load)';
