"""Core data validator service — resolves the client + ERP ids from the master
DB (user_db), ported from retailverse pipeline_launcher.CoreDataValidatorService.

Given a client_code + ext_erp_rec_code, returns (client_id, ext_erp_id,
ext_erp_name) by reading masterdb.mclient (shortcode -> client_id) and
masterdb.external_erp (rec_code -> erp_id/erp_name). The load engine injects
these ids into every row so the destination records + parent-id lookups are
scoped exactly like retailverse.
"""
import logging

from engine.repositories import CoreDataValidatorRepository

logger = logging.getLogger("ETL_VALIDATION")


class CoreDataValidatorService:

    def __init__(self, client_code: str, ext_erp_rec_code: str):
        self.client_code = client_code
        self.ext_erp_rec_code = ext_erp_rec_code

    def get_client_and_erp_ids(self) -> tuple[int, int, str]:
        """(client_id, ext_erp_id, ext_erp_name) from the master DB."""
        client_id = CoreDataValidatorRepository.get_client_id(self.client_code)
        ext_erp_id, ext_erp_name = CoreDataValidatorRepository.get_erp_details(self.ext_erp_rec_code)
        logger.info("----> [ID RESOLUTION] client=%s -> client_id=%s | rec_code=%s -> ext_erp_id=%s (%s)",
                    self.client_code, client_id, self.ext_erp_rec_code, ext_erp_id, ext_erp_name)
        return client_id, ext_erp_id, ext_erp_name
