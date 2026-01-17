import copy
from typing import Iterable, Tuple

from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import Column, TableType
from metadata.generated.schema.entity.services.connections.database.clickhouseConnection import (
    ClickhouseConnection, ClickhouseType, ClickhouseScheme,
)
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import \
    CustomDatabaseConnection
from metadata.generated.schema.type import basic
from metadata.ingestion.api.models import Either
from metadata.ingestion.source.database.clickhouse.metadata import ClickhouseSource
from metadata.utils.execution_time_tracker import calculate_execution_time_generator
from metadata.utils.logger import ingestion_logger

from .gs_integration.main import ClickhouseGSInfo

logger = ingestion_logger()


class ClickhouseCustomIngestor(ClickhouseSource):

    def __init__(self, config, metadata,):
        super().__init__(config, metadata)
        self.gs_info = ClickhouseGSInfo('1W_WLS0chOvaxl_Cejt8pgF6cnUm8-4QkUdlYWPn1KGA')
        self.get_info_about_tables_in_gs()
    
    def get_info_about_tables_in_gs(self) -> dict[str, dict[str, str]] | None:
        if not self.gs_info.metadata_need_update:
            return None
        
        tables_data = self.gs_info.get_info_about_tables_in_gs()
        return tables_data or None

    @classmethod
    def create(cls, config_dict, metadata, pipeline_name=None):

        cfg = copy.deepcopy(config_dict)

        custom_conn = CustomDatabaseConnection.model_validate(cfg["serviceConnection"]["config"])
        raw_config = cfg["serviceConnection"]["config"]

        click_conn = ClickhouseConnection.model_validate({
            "type": ClickhouseType.Clickhouse,
            "scheme": ClickhouseScheme.clickhouse_http,
            "hostPort": custom_conn.hostPort, # pyright: ignore[reportAttributeAccessIssue]
            "username": custom_conn.username, # pyright: ignore[reportAttributeAccessIssue]
            "password": custom_conn.password, # pyright: ignore[reportAttributeAccessIssue]
            "databaseName": custom_conn.databaseName, # pyright: ignore[reportAttributeAccessIssue]
            "databaseSchema": None,
            "connectionOptions": raw_config['connectionOptions'],
            "schemaFilterPattern": custom_conn.schemaFilterPattern,
            "tableFilterPattern": custom_conn.tableFilterPattern,
            "databaseFilterPattern": custom_conn.databaseFilterPattern,
            "supportsMetadataExtraction": custom_conn.supportsMetadataExtraction,
        })

        cfg["serviceConnection"]["config"] = click_conn.model_dump(mode="json")
        return super().create(cfg, metadata, pipeline_name)
    
    def _description(self, table_name: str, column: Column) -> None:
        base_comment = column.description.root if column.description else "Комментарий отсутствует"
        gs_comment = self.gs_info.tables_data.get(table_name, {}).get(column.name.root, "Комментарий отсутствует")
        column.description = basic.Markdown(f"Описание из БД:\n"
                                            f"{base_comment}\n\n"
                                            f"Описание из GS:\n"
                                            f"{gs_comment}")
    
    @calculate_execution_time_generator()
    def yield_table(
        self, table_name_and_type: Tuple[str, TableType]
    ) -> Iterable[Either[CreateTableRequest]]:
        
        table_name = table_name_and_type[0]
        
        for either in super().yield_table(table_name_and_type):

            right = either.right
            assert right
            for column in right.columns or []:
                self._description(table_name, column)
            either.right = right
            yield either