from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from datetime import datetime
import uuid
import os


class ElasticsearchLogStorage:
    def __init__(self, hosts=None, index_prefix='distributed_logs'):
        hosts = hosts or [os.environ.get('ES_HOST', 'http://localhost:9200')]
        """
        Initialize asynchronous Elasticsearch connection with flexible configuration.
        """
        self.es_client = AsyncElasticsearch(hosts)
        self.index_prefix = index_prefix

    async def _create_index_template(self):
        """
        Create an asynchronous reusable index template for structured log storage.
        """
        template = {
            "index_patterns": [f"{self.index_prefix}-*"],
            "template": {
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 1,
                    "max_ngram_diff": 50
                },
                "mappings": {
                    "properties": {
                        "node_id": {"type": "keyword"},
                        "message_type": {"type": "keyword"},
                        "service_name": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "log_id": {"type": "keyword"},
                        "log_level": {"type": "keyword"},
                        "message": {"type": "text"},
                        "response_time_ms": {"type": "integer"},
                        "threshold_limit_ms": {"type": "integer"},
                        "registration_details": {"type": "object", "enabled": True},
                        "error_details": {
                            "type": "object",
                            "properties": {
                                "error_code": {"type": "keyword"},
                                "error_message": {"type": "text"}
                            }
                        }
                    }
                }
            }
        }
        try:
            await self.es_client.indices.put_index_template(
                name=f"{self.index_prefix}_template",
                body=template
            )
            print(f"Index template {self.index_prefix}_template created/updated successfully.")
        except Exception as e:
            print(f"Error creating/updating index template: {e}")

    def _get_daily_index_name(self):
        return f"{self.index_prefix}-{datetime.utcnow().strftime('%Y.%m.%d')}"

    async def store_logs(self, logs):
        """
        Store logs in Elasticsearch using asynchronous bulk indexing.
        Supports multiple log types: Registration, INFO, WARN, ERROR.
        """
        try:
            # Ensure logs is a list
            if not isinstance(logs, list):
                logs = [logs]

            index_name = self._get_daily_index_name()

            if not await self.es_client.indices.exists(index=index_name):
                await self.es_client.indices.create(index=index_name)

            def log_generator():
                for log in logs:
                    if 'log_id' not in log:
                        log['log_id'] = str(uuid.uuid4())
                    yield {
                        "_index": index_name,
                        "_id": log['log_id'],
                        "_source": log
                    }

            # Asynchronous bulk index
            success, failed = await async_bulk(self.es_client, log_generator())
            print(f"Successfully indexed {success} logs, {failed} failed.")

        except Exception as e:
            print(f"Error in log indexing: {e}")

    async def close_connection(self):
        """
        Close the Elasticsearch connection gracefully.
        """
        await self.es_client.close()
