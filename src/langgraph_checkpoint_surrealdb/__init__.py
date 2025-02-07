import random
import threading
from contextlib import contextmanager
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import ChannelProtocol
from surrealdb import Surreal


class SurrealSaver(BaseCheckpointSaver[str]):
    is_setup: bool

    def __init__(
        self,
        url: str,
        namespace: str,
        database: str,
        user: str,
        password: str,
        *,
        serde: Optional[SerializerProtocol] = None,
    ) -> None:
        super().__init__(serde=serde)
        self.url = url
        self.namespace = namespace
        self.database = database
        self.user = user
        self.password = password
        self.jsonplus_serde = JsonPlusSerializer()
        self.is_setup = False
        self.lock = threading.Lock()

    @contextmanager
    def db_connection(self):
        db = Surreal(self.url)
        db.signin({"username": self.user, "password": self.password})
        db.use(self.namespace, self.database)
        yield db

    def setup(self) -> None:
        self.is_setup = True

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        with self.db_connection() as connection:
            try:
                thread_id = str(config["configurable"]["thread_id"])

                query = f"""
                SELECT thread_id, checkpoint_id, parent_checkpoint_id, type, 
                checkpoint, metadata 
                FROM checkpoint WHERE 
                thread_id = '{thread_id}' AND checkpoint_ns = '{checkpoint_ns}'
                """

                if checkpoint_id := get_checkpoint_id(config):
                    query += f" AND checkpoint_id = '{checkpoint_id}'"
                else:
                    query += " ORDER BY checkpoint_id DESC limit 1"

                result = connection.query(query)

                if len(result) > 0:
                    result_dict = result[0]
                    thread_id = result_dict["thread_id"]
                    checkpoint_id = result_dict["checkpoint_id"]
                    parent_checkpoint_id = result_dict["parent_checkpoint_id"]
                    type = result_dict["type"]
                    checkpoint = result_dict["checkpoint"]
                    metadata = result_dict["metadata"]
                    if not get_checkpoint_id(config):
                        config = {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                            }
                        }

                    # find any pending writes
                    query = f"SELECT task_id, channel, type, value, idx FROM write WHERE thread_id = '{thread_id}' AND checkpoint_ns = '{checkpoint_ns}' AND checkpoint_id = '{checkpoint_id}' ORDER BY task_id, idx"
                    results = connection.query(query)

                    return CheckpointTuple(
                        config,
                        self.serde.loads_typed((type, checkpoint)),
                        self.jsonplus_serde.loads(metadata)
                        if metadata is not None
                        else {},
                        (
                            {
                                "configurable": {
                                    "thread_id": thread_id,
                                    "checkpoint_ns": checkpoint_ns,
                                    "checkpoint_id": parent_checkpoint_id,
                                }
                            }
                            if parent_checkpoint_id
                            else None
                        ),
                        [
                            (
                                r["task_id"],
                                r["channel"],
                                self.serde.loads_typed((type, r["value"])),
                            )
                            for r in results
                        ],
                    )
                else:
                    return None
            except:
                raise

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        thread_id = (
            str(config.get("configurable", {}).get("thread_id", "")) if config else ""
        )
        checkpoint_ns = (
            config.get("configurable", {}).get("checkpoint_ns", "") if config else ""
        )
        # todo: parameter filtering
        # where, param_values = search_where(config, filter, before)
        query = f"""SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata
        FROM checkpoint WHERE thread_id = '{thread_id}' AND checkpoint_ns = '{checkpoint_ns}' ORDER BY checkpoint_id DESC"""
        if limit:
            query += f" LIMIT {limit}"

        with self.db_connection() as connection:
            results = connection.query(query)
            for r in results:
                thread_id = r["thread_id"]
                checkpoint_ns = r["checkpoint_ns"]
                checkpoint_id = r["checkpoint_id"]
                parent_checkpoint_id = r["parent_checkpoint_id"]
                type = r["type"]
                checkpoint = r["checkpoint"]
                metadata = r["metadata"]

                query = f"SELECT task_id, channel, type, value, idx FROM write WHERE thread_id = '{thread_id}' AND checkpoint_ns = '{checkpoint_ns}' AND checkpoint_id = '{checkpoint_id}' ORDER BY task_id, idx"
                task_results = connection.query(query)

                yield CheckpointTuple(
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                        }
                    },
                    self.serde.loads_typed((type, checkpoint)),
                    self.jsonplus_serde.loads(metadata) if metadata is not None else {},
                    (
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": parent_checkpoint_id,
                            }
                        }
                        if parent_checkpoint_id
                        else None
                    ),
                    [
                        (
                            tr["task_id"],
                            tr["channel"],
                            self.serde.loads_typed((type, tr["value"])),
                        )
                        for tr in task_results
                    ],
                )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        type_, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
        serialized_metadata = self.jsonplus_serde.dumps(metadata)
        with self.db_connection() as connection:
            existing_query = connection.query(
                f"SELECT id FROM checkpoint WHERE thread_id = '{thread_id}' AND checkpoint_ns = '{checkpoint_ns}' and checkpoint_id='{checkpoint['id']}'"
            )
            if existing_query:
                record_id = existing_query[0]["id"]
            else:
                record_id = "checkpoint"

            merge_data = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
                "parent_checkpoint_id": config["configurable"].get("checkpoint_id"),
                "type": type_,
                "checkpoint": serialized_checkpoint,
                "metadata": serialized_metadata,
            }

            connection.upsert(record_id, merge_data)
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        for idx, (channel, value) in enumerate(writes):
            type_, serialized_value = self.serde.dumps_typed(value)
            merge_data = {
                "thread_id": config["configurable"]["thread_id"],
                "checkpoint_ns": config["configurable"]["checkpoint_ns"],
                "checkpoint_id": config["configurable"]["checkpoint_id"],
                "task_id": task_id,
                "idx": WRITES_IDX_MAP.get(channel, idx),
                "channel": channel,
                "type": type_,
                "value": serialized_value,
                "task_path": task_path,
            }
            with self.db_connection() as connection:
                connection.upsert("write", merge_data)

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database asynchronously.

        Note:
            This async method is not supported by the SqliteSaver class.
            Use get_tuple() instead, or consider using [AsyncSqliteSaver][langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver].
        """
        raise NotImplementedError("not implemented")

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints from the database asynchronously.

        Note:
            This async method is not supported by the SqliteSaver class.
            Use list() instead, or consider using [AsyncSqliteSaver][langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver].
        """
        raise NotImplementedError("not implemented")
        yield

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to the database asynchronously.

        Note:
            This async method is not supported by the SqliteSaver class.
            Use put() instead, or consider using [AsyncSqliteSaver][langgraph.checkpoint.sqlite.aio.AsyncSqliteSaver].
        """
        raise NotImplementedError("not implemented")

    def get_next_version(self, current: Optional[str], channel: ChannelProtocol) -> str:
        """Generate the next version ID for a channel.

        This method creates a new version identifier for a channel based on its current version.

        Args:
            current (Optional[str]): The current version identifier of the channel.
            channel (BaseChannel): The channel being versioned.

        Returns:
            str: The next version identifier, which is guaranteed to be monotonically increasing.
        """
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"
