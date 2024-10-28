import os
import uuid
from copy import deepcopy
from typing import Dict, Iterator, List, Optional, cast

from letta.agent_store.storage import StorageConnector, TableType
from letta.config import LettaConfig
from letta.constants import MAX_EMBEDDING_DIM
from letta.data_types import Passage, Record, RecordType
from letta.utils import datetime_to_timestamp, timestamp_to_datetime

TEXT_PAYLOAD_KEY = "text_content"
METADATA_PAYLOAD_KEY = "metadata"


class WeaviateStorageConnector(StorageConnector):
    """Storage via Weaviate"""

    def __init__(self, table_type: str, config: LettaConfig, user_id, agent_id=None):
        super().__init__(table_type=table_type, config=config, user_id=user_id, agent_id=agent_id)
        try:
            import weaviate
            import weaviate.classes.config as wvcc
        except ImportError as e:
            raise ImportError("'weaviate-cient' not installed. Run `pip install weaviate`.") from e
        assert table_type in [TableType.ARCHIVAL_MEMORY, TableType.PASSAGES], "Weaviate only supports archival memory"
        if config.archival_storage_uri and len(config.archival_storage_uri.split(":")) == 2:
            host, port = config.archival_storage_uri.split(":")
            self.weaviate_client = WeaviateClient(host=host, port=port, api_key=os.getenv("WEAVIATE_API_KEY"))
        elif config.archival_storage_path:
            self.weaviate_client = WeaviateClient(path=config.archival_storage_path)
        else:
            raise ValueError("Weaviate storage requires either a URI or a path to the storage configured")
        if not self.weaviate_client.collections.exists(self.table_name):
            self.weavate_client.create_collections.create(
                collection_name=self.table_name,
                vectors_config=wvcc.Configure.Vectorizer()
            )
        self.uuid_fields = ["id", "user_id", "agent_id", "source_id", "file_id"]

    def get_all_paginated(self, filters: Optional[Dict] = {}, page_size: int = 10) -> Iterator[List[RecordType]]:
        filters = self.get_weaviate_filters(filters)
        collection = weaviate_client.collections.get(self.table_name)
        next_offset = None
        stop_scrolling = False
        while not stop_scrolling:
            results, next_offset = collection.query.fetch_objects(
                scroll_filter=filters,
                limit=page_size,
                offset=next_offset,
            )
            stop_scrolling = next_offset is None or (
                isinstance(next_offset, grpc.PointId) and next_offset.num == 0 and next_offset.uuid == ""
            )
            yield self.to_records(results)

    def get_all(self, filters: Optional[Dict] = {}, limit=10) -> List[RecordType]:
        if self.size(filters) == 0:
            return []
        filters = self.get_weaviate_filters(filters)
        collection = self.weaviate_client.collections.get(self.table_name)
        results, _ = collection.query.fetch_objects(
            scroll_filter=filters,
            limit=limit,
        )
        return self.to_records(results)

    def get(self, id: str) -> Optional[RecordType]:
        collection = self.weaviate_client.collections.get(self.table_name)
        results = collection.query.fetch_object_by_id(id)
        if not results:
            return None
        return self.to_records(results)[0]

    def insert(self, record: Record):
        points = self.to_points([record])
        collection = weaviate_client.collections.get(self.table_name)
        collection.insert(
            properties=points
        )

    def insert_many(self, records: List[RecordType], show_progress=False):
        points = self.to_points(records)
        self.weaviate_client.insert(self.table_name, points=points)

    def delete(self, filters: Optional[Dict] = {}):
        filters = self.get_weaviate_filters(filters)
        self.weaviate_client.delete(self.table_name, points_selector=filters)

    def delete_table(self):
        self.weaviate_client.delete_collection(self.table_name)
        self.weaviate_client.close()

    def size(self, filters: Optional[Dict] = {}) -> int:
        filters = self.get_weaviate_filters(filters)
        return self.weaviate_client.count(collection_name=self.table_name, count_filter=filters).count

    def close(self):
        self.weaviate_client.close()

    def query(
        self,
        query: str,
        query_vec: List[float],
        top_k: int = 10,
        filters: Optional[Dict] = {},
    ) -> List[RecordType]:
        filters = self.get_filters(filters)
        collection = weaviate_client.collections.get(self.table_name)
        results = collectin.query.hybrid(
            query=query
            limit=top_k,
            include_vector=True
        )
        return self.to_records(results)

    def to_records(self, records: list) -> List[RecordType]:
        parsed_records = []
        for record in records:
            record = deepcopy(record)
            #metadata = record.payload[METADATA_PAYLOAD_KEY]
            #text = record.payload[TEXT_PAYLOAD_KEY]
            _id = record.uuid
            embedding = record.vector
            for key, value in metadata.items():
                if key in self.uuid_fields:
                    metadata[key] = uuid.UUID(value)
                elif key == "created_at":
                    metadata[key] = timestamp_to_datetime(value)
            parsed_records.append(
                cast(
                    RecordType,
                    self.type(
                        text=text,
                        embedding=embedding,
                        id=uuid.UUID(_id),
                        **metadata,
                    ),
                )
            )
        return parsed_records

    def get_weaviate_filters(filters: Optional[Dict[str, Any]] = None) -> _Filters:
        """
        Constructs a Weaviate filter based on provided conditions, using 'equal' operator for all conditions,
        and including the hard-coded metadata property key in the property names.
    
        Args:
            filters (Dict): Dictionary of field names to values for filtering.
    
        Returns:
            _Filters: A Weaviate filter object that can be used for queries.
        """
        must_conditions = []
    
        for key, value in (filters or {}).items():
            # Construct the full property path including the metadata key
            property_name = f"{METADATA_PROPERTY_KEY}.{key}"
    
            # Create the filter condition using 'equal' operator
            filter_condition = Filter.by_property(property_name).equal(value)
            must_conditions.append(filter_condition)
    
        if len(must_conditions) == 1:
            # If only one filter, return it directly
            return must_conditions[0]
        elif must_conditions:
            # Combine conditions with AND logic
            return Filter.all_of(must_conditions)

