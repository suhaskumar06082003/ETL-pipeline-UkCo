"""EXTRACT stage: read a CSV/Excel file into a pandas DataFrame.

Ported from retailverse ETL `DataIngestionService` — same reading logic
(chardet encoding detection for CSV, openpyxl for Excel), reading from bytes
so it works for both an uploaded file (API) and a blob download (Airflow).
"""
import io
import logging
import os

import chardet
import pandas as pd

from core.utils.exceptions import ExtractError

logger = logging.getLogger("ETL_EXTRACT")


class FileReader:
    @staticmethod
    def _detect_encoding(data: bytes) -> str:
        """Detects encoding of file bytes using chardet."""
        try:
            result = chardet.detect(data[:10000])
            encoding = result.get("encoding")
            confidence = result.get("confidence", 0)

            if not encoding or encoding.lower() in ["ascii", "unknown"] or confidence < 0.7:
                logger.warning(
                    f"----> [Encoding] Low confidence ({confidence}) or unknown "
                    f"encoding: '{encoding}'. Using fallback: ISO-8859-1"
                )
                return "ISO-8859-1"

            logger.info(f"----> [Encoding] Detected encoding: {encoding} (confidence: {confidence})")
            return encoding
        except Exception as e:
            logger.error(f"----> [Encoding Error] Failed to detect file encoding: {e}", exc_info=True)
            raise ExtractError("Encoding detection failed. File may be unreadable or corrupted.")

    @staticmethod
    def _read_excel(data: bytes) -> pd.DataFrame:
        try:
            df = pd.read_excel(io.BytesIO(data), engine="openpyxl", dtype=str)
            logger.info("----> [READ_EXCEL_FILE] : Excel file read successfully")
            return df
        except ValueError as e:
            logger.error(f"----> [READ_EXCEL_FILE] : ValueError reading Excel: {e}", exc_info=True)
            raise ExtractError("Failed to read Excel file. Invalid content or formatting.")
        except Exception as e:
            logger.error(f"----> [READ_EXCEL_FILE] : Unexpected error reading Excel: {e}", exc_info=True)
            raise ExtractError("Excel file could not be read. Please check the format or corruption.")

    @staticmethod
    def _read_csv(data: bytes) -> pd.DataFrame:
        try:
            encoding = FileReader._detect_encoding(data)
            df = pd.read_csv(
                io.BytesIO(data),
                encoding=encoding,
                sep=",",
                engine="python",
                on_bad_lines="skip",
                dtype=str,
            )
            df.columns = [col.strip().replace("﻿", "") for col in df.columns]
            logger.info("----> [READ_CSV_FILE] : CSV file read successfully")
            return df
        except UnicodeDecodeError as e:
            logger.error(f"---->[READ_CSV_FILE] : Encoding issue: {str(e)}", exc_info=True)
            raise ExtractError("Encoding issue: Could not decode CSV. Try saving as UTF-8.")
        except Exception as e:
            logger.error(f"---->[READ_CSV_FILE] : Failed to read CSV file: {str(e)}", exc_info=True)
            raise ExtractError("CSV file could not be read. Please check formatting or corruption.")

    @staticmethod
    def read(filename: str, data: bytes) -> pd.DataFrame:
        """Read file bytes into a DataFrame based on the file extension."""
        _, ext = os.path.splitext(filename)
        ext = ext.lower()

        if ext == ".xlsx":
            df = FileReader._read_excel(data)
        elif ext == ".csv":
            df = FileReader._read_csv(data)
        else:
            logger.error(f"----> [Read File Error] Unsupported file format: {ext}")
            raise ExtractError(f"Unsupported format '{ext}'. Only .xlsx and .csv supported.")

        if df.empty:
            logger.warning("----> [Read File Warning] Empty file")
            raise ExtractError("The uploaded file contains no data. Please check the file.")

        return df
