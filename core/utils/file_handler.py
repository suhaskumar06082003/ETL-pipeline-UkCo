"""File export helpers.

Ported from retailverse `core/utils/file_handler.py`. Same logic; the only
adaptation is returning a FastAPI ``Response`` instead of Django's
``HttpResponse`` (identical Content-Type and Content-Disposition, same
``xlsxwriter`` engine).
"""
from io import BytesIO

import pandas as pd
from fastapi import Response
from fastapi.responses import JSONResponse

XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class FileHandler:

    @staticmethod
    def write_to_excelfile(df, file_name="Download", sheet_name=None):
        try:
            # Excel worksheet names are capped at 31 chars; file_name (used for
            # the download filename) can be much longer, so default the sheet
            # name to a truncated file_name rather than passing it through as-is.
            sheet_name = (sheet_name or file_name)[:31]
            with BytesIO() as b_io:
                with pd.ExcelWriter(b_io, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
                b_io.seek(0)
                data = b_io.getvalue()
            return Response(
                content=data,
                media_type=XLSX_CONTENT_TYPE,
                headers={"Content-Disposition": f'attachment; filename="{file_name}.xlsx"'},
            )
        except ModuleNotFoundError:
            return JSONResponse(
                status_code=500,
                content={
                    "data": {},
                    "success": False,
                    "message": "Excel export failed: xlsxwriter library is not installed.",
                },
            )
