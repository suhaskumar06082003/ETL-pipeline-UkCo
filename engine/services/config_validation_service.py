"""Config-driven validation + schema application — ONE copy shared by every
source (file upload API and DB-sourced pipelines like q-commerce).

Ported from retailverse, same order as its outlet POST flow
(config_validation_service.ETLPipelineService.perform_etl_validation):

    read_data  ->  DataPreprocessor.clean_file  ->  validate_required_columns
               ->  apply_schema (select + RENAME ONCE + cast)  ->  serializer rules

  * DataPreprocessor.clean_file   header normalization + blank -> None
  * validate_required_columns     every CLIENT_REQUIREMENTS source column present
  * apply_schema                  select the config columns, rename each ONCE to
                                  its canonical name, cast by datatype
                                  (retailverse DataIngestionService._apply_schema)

Framework-agnostic (pandas only) so it runs in the API container and in Airflow.
"""
import logging

import pandas as pd

from config.clients_config import CLIENT_REQUIREMENTS
from config.etl_enums import ConfigKey
from core.utils.exceptions import ValidationError

logger = logging.getLogger("ETL_VALIDATION")
preprocessor_logger = logging.getLogger("DATA_PREPROCESSOR")
schema_logger = logging.getLogger("ETL_DATALOADER")


# ─────────────────────────────────────────────────────────────
# Config-validation service — required-column validation.
# ─────────────────────────────────────────────────────────────
def validate_required_columns(df: pd.DataFrame, file_type: str, client_code: str) -> pd.DataFrame:
    """Validate required columns for the client + file type; return the subset."""
    # 1. Validate client
    if client_code not in CLIENT_REQUIREMENTS:
        allowed_clients = list(CLIENT_REQUIREMENTS.keys())
        logger.error(
            f"----> [COLUMN VALIDATION ERROR]: Unsupported client\n"
            f" Reason: Client '{client_code}' not found in CLIENT_REQUIREMENTS\n"
            f" Allowed Clients: {allowed_clients}"
        )
        raise ValidationError("Unsupported client. Please contact your administrator.")

    client_reqs = CLIENT_REQUIREMENTS[client_code]

    # 2. Validate file type
    if file_type not in client_reqs:
        allowed_types = list(client_reqs.keys())
        logger.error(
            f"----> [COLUMN VALIDATION ERROR]: Unsupported file type\n"
            f" Client: {client_code}, Provided File Type: '{file_type}'\n"
            f" Allowed File Types: {allowed_types}"
        )
        raise ValidationError("File type not supported. Please contact your administrator.")

    # 3. Required column config
    required_columns_mapping = client_reqs[file_type].get(ConfigKey.REQUIRED_COLUMNS)
    if not required_columns_mapping:
        raise ValidationError("Column configuration missing. Please contact your administrator.")

    required_columns = list(required_columns_mapping.keys())
    logger.info(f"----> [COLUMN_VALIDATION] : Expected schema columns: {required_columns}")

    df_columns = df.columns.tolist()

    # 4. Missing columns
    missing_columns = [col for col in required_columns if col not in df_columns]
    if missing_columns:
        missing_str = ", ".join(missing_columns)
        logger.error(
            f"----> [COLUMN VALIDATION ERROR]: Missing columns\n"
            f" Client: {client_code}, File Type: {file_type}\n"
            f" Missing Columns: [{missing_str}]"
        )
        raise ValidationError(
            f"Missing required columns:[ {missing_str} ]. Please check the file template."
        )

    # 5. Subset only required columns
    df = df[required_columns]
    logger.info("----> [COLUMN VALIDATION] : Successful.")
    return df


# ─────────────────────────────────────────────────────────────
# Data-preprocessing service — header normalization / null cleaning.
# ─────────────────────────────────────────────────────────────
class DataPreprocessor:

    @staticmethod
    def clean_file(df: pd.DataFrame) -> pd.DataFrame:
        """Clean the DataFrame by:
        - Replacing missing values (NaN, NaT, empty/whitespace strings) with None
        - Replacing special characters in column names with underscores
        - Collapsing multiple underscores and trimming them
        """
        try:
            # Replace empty strings and NaNs with None
            df = df.replace(r"^\s*$", None, regex=True).infer_objects(copy=False)
            df = df.where(pd.notna(df), None)

            # Normalize column names
            df.columns = (
                df.columns
                .str.strip()
                .str.replace(r"([^\w])", "_", regex=True)
                .str.replace(r"_+", "_", regex=True)
                .str.strip("_")
            )

            preprocessor_logger.info(
                f"[ETL_PHASE: CLEANING_COMPLETED] --> Data cleaned successfully "
                f"[COLUMNS] --> retained: {list(df.columns)}"
            )
            return df

        except (AttributeError, TypeError) as e:
            preprocessor_logger.error(f"----> DataFrame cleaning failed: {e}", exc_info=True)
            raise ValidationError("Invalid file structure. Please upload a valid tabular file (CSV, Excel, etc).")

        except Exception as e:
            preprocessor_logger.error(f"----> Unexpected error during DataFrame cleaning: {e}", exc_info=True)
            raise ValidationError("An unexpected error occurred during file cleaning. Please contact support.")


# ─────────────────────────────────────────────────────────────
# Schema application — select + rename ONCE + cast (retailverse _apply_schema).
# ─────────────────────────────────────────────────────────────
def apply_schema(df: pd.DataFrame, client_code: str, file_type: str) -> pd.DataFrame:
    """Apply schema rules: select, rename, cast datatypes.

    Every source column in the CLIENT_REQUIREMENTS config is renamed ONCE to its
    canonical ``rename`` name; everything downstream (serializer rules, flow
    config, loader) uses only those canonical names.
    """
    try:
        config = CLIENT_REQUIREMENTS[client_code][file_type][ConfigKey.REQUIRED_COLUMNS]
        selected_cols = {}
        for original_col, rules in config.items():
            rename = rules.get("rename", original_col)
            dtype = rules.get("datatype", "str")

            if original_col not in df.columns:
                schema_logger.warning(f"[ETL_SCHEMA] Missing column in file: {original_col}")
                selected_cols[rename] = pd.Series([None] * len(df), index=df.index)
                continue

            series = df[original_col]

            # type casting
            if dtype == "str":
                series = series.astype(str).str.strip()
            elif dtype == "float":
                series = pd.to_numeric(series, errors="coerce").astype(float)
            elif dtype == "int":
                series = pd.to_numeric(series, errors="coerce").astype("Int64")
            else:
                series = series.astype(str).str.strip()
            selected_cols[rename] = series

        cleaned_df = pd.DataFrame(selected_cols)
        schema_logger.info(f"----> [ETL_SCHEMA] : Applied schema for client code : {client_code} and file type : {file_type}")
        return cleaned_df

    except Exception as e:
        schema_logger.error(f"[ETL_SCHEMA ERROR] Schema transformation failed: {e}", exc_info=True)
        raise ValidationError("Schema transformation failed. Please check column mappings or data format.")
