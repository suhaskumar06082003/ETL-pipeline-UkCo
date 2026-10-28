"""Raw SQL for the ETL metadata store (etl_task / etl_task_stage).

Used by ``api.repositories`` (the MySQL metadata tracker). The SQL
text is kept identical to what the tracker executed inline before; it is
extracted here so all metadata SQL lives in one place.
"""

INSERT_TASK = (
    "INSERT INTO etl_task "
    "(task_id, client_code, file_type, ext_erp_rec_code, original_filename, size_bytes, "
    " status, current_stage, created_by, updated_by) "
    "VALUES (:task_id, :client_code, :file_type, :ext_erp_rec_code, :original_filename, :size_bytes, "
    " :status, :current_stage, :created_by, :updated_by)"
)

INSERT_TASK_STAGE = (
    "INSERT INTO etl_task_stage (task_id, file_type, stage, status) "
    "VALUES (:task_id, :file_type, :stage, :status)"
)

# file_type is copied from the task itself; row_count = rows at this stage
# (LOAD_<table>: records sent to insert/upsert into that table)
INSERT_TASK_STAGE_WITH_ERROR = (
    "INSERT INTO etl_task_stage (task_id, file_type, stage, status, row_count, error) "
    "SELECT :task_id, t.file_type, :stage, :status, :row_count, :error "
    "FROM etl_task t WHERE t.task_id = :task_id"
)


def update_task_sql(set_clause: str) -> str:
    return (
        f"UPDATE etl_task SET {set_clause}, updated_at = CURRENT_TIMESTAMP(6) "
        "WHERE task_id = :task_id"
    )


UPDATE_TASK_STATUS = (
    "UPDATE etl_task SET status = :status, updated_at = CURRENT_TIMESTAMP(6) "
    "WHERE task_id = :task_id"
)

SELECT_TASK = "SELECT * FROM etl_task WHERE task_id = :t"

SELECT_TASK_STAGES = (
    "SELECT stage, status, row_count, error, created_at FROM etl_task_stage "
    "WHERE task_id = :t ORDER BY id"
)

SELECT_ALL_TASKS = "SELECT * FROM etl_task ORDER BY created_at DESC"

SELECT_ALL_STAGES = (
    "SELECT task_id, stage, status, row_count, error, created_at FROM etl_task_stage "
    "ORDER BY id"
)
