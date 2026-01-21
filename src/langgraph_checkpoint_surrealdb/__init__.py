import json
import logging
import random
import threading
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Sequence, Tuple, cast
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _load_metadata_with_fallback(
    serde: "SerializerProtocol",
    metadata: Any,
    metadata_type: str,
) -> "CheckpointMetadata":
    """Load metadata with fallback for legacy format.

    Args:
        serde: The serializer to use.
        metadata: The metadata to deserialize.
        metadata_type: The type of the metadata serialization.

    Returns:
        CheckpointMetadata: The deserialized metadata.
    """
    if metadata is None:
        return {}

    # Try new format first (msgpack bytes)
    try:
        return serde.loads_typed((metadata_type, metadata))
    except Exception:
        pass

    # Fallback for legacy format (JSON string)
    try:
        if isinstance(metadata, str):
            return json.loads(metadata)
        elif isinstance(metadata, bytes):
            return json.loads(metadata.decode("utf-8"))
    except Exception:
        pass

    # Return empty dict if all attempts fail
    logger.warning("Failed to deserialize metadata, returning empty dict")
    return {}

# Type alias for clarity
JsonDict = Dict[str, Any]


class CheckpointError(Exception):
    """Base exception for checkpoint operations."""

    pass


class CheckpointReadError(CheckpointError):
    """Raised when there's an error reading checkpoint data."""

    pass


class CheckpointSaveError(CheckpointError):
    """Raised when there's an error saving checkpoint data."""

    pass


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
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.types import ChannelProtocol
from surrealdb import AsyncSurreal, Surreal


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
        self.is_setup = False
        self.lock = threading.Lock()

    @contextmanager
    def db_connection(self):
        db = Surreal(self.url)
        db.signin({"username": self.user, "password": self.password})
        db.use(self.namespace, self.database)
        scheme = urlparse(self.url).scheme.lower()
        try:
            yield db
        finally:
            # Only close the connection for websocket protocols.
            if scheme in ("ws", "wss"):
                db.close()

    @asynccontextmanager
    async def adb_connection(self):
        db = AsyncSurreal(self.url)
        await db.signin({"username": self.user, "password": self.password})
        await db.use(self.namespace, self.database)
        scheme = urlparse(self.url).scheme.lower()
        try:
            yield db
        finally:
            # Only close the connection for websocket protocols.
            if scheme in ("ws", "wss"):
                await db.close()

    def setup(self) -> None:
        self.is_setup = True

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database.

        Args:
            config: The configuration containing thread and checkpoint information.

        Returns:
            Optional[CheckpointTuple]: The checkpoint tuple if found, None otherwise.

        Raises:
            CheckpointReadError: If there's an error reading from the database.
        """
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        with self.db_connection() as connection:
            try:
                thread_id = str(config["configurable"]["thread_id"])

                query = """
                SELECT thread_id, checkpoint_id, parent_checkpoint_id, type,
                checkpoint, metadata, metadata_type
                FROM checkpoint WHERE
                thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns
                """

                vars = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}

                if checkpoint_id := get_checkpoint_id(config):
                    vars["checkpoint_id"] = checkpoint_id
                    query += " AND checkpoint_id = $checkpoint_id"
                else:
                    query += " ORDER BY checkpoint_id DESC limit 1"

                try:
                    result = connection.query(query, vars)
                except Exception as e:
                    logger.error(
                        "Failed to query checkpoint data",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "error": str(e),
                        },
                    )
                    raise CheckpointReadError(
                        f"Unable to retrieve checkpoint data: {str(e)}"
                    ) from e

                if len(result) > 0:
                    result_dict = result[0]
                    thread_id = result_dict["thread_id"]
                    checkpoint_id = result_dict["checkpoint_id"]
                    parent_checkpoint_id = result_dict["parent_checkpoint_id"]
                    type_ = result_dict["type"]
                    checkpoint = result_dict["checkpoint"]
                    metadata = result_dict["metadata"]
                    metadata_type = result_dict.get("metadata_type", type_)
                    if not get_checkpoint_id(config):
                        config = {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                            }
                        }

                    # find any pending writes
                    query = """
                    SELECT task_id, channel, type, value, idx
                    FROM write
                    WHERE thread_id = $thread_id
                    AND checkpoint_ns = $checkpoint_ns
                    AND checkpoint_id = $checkpoint_id
                    ORDER BY task_id, idx
                    """

                    vars = {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                    }

                    try:
                        results = connection.query(query, vars)
                    except Exception as e:
                        logger.error(
                            "Failed to query write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to retrieve write data: {str(e)}"
                        ) from e

                    try:
                        checkpoint_data = self.serde.loads_typed((type_, checkpoint))
                        metadata_dict = cast(
                            CheckpointMetadata,
                            _load_metadata_with_fallback(
                                self.serde, metadata, metadata_type
                            ),
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to deserialize checkpoint data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to deserialize checkpoint data: {str(e)}"
                        ) from e

                    try:
                        writes = [
                            (
                                r["task_id"],
                                r["channel"],
                                self.serde.loads_typed((type_, r["value"])),
                            )
                            for r in results
                        ]
                    except Exception as e:
                        logger.error(
                            "Failed to deserialize write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to deserialize write data: {str(e)}"
                        ) from e

                    return CheckpointTuple(
                        config,
                        checkpoint_data,
                        metadata_dict,
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
                        writes,
                    )
                else:
                    logger.debug(
                        "No checkpoint found",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                        },
                    )
                    return None
            except Exception as e:
                if not isinstance(e, CheckpointReadError):
                    logger.error(
                        "Unexpected error retrieving checkpoint",
                        extra={
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )
                    raise CheckpointReadError(
                        f"Unexpected error retrieving checkpoint: {str(e)}"
                    ) from e
                raise

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints from the database.

        Args:
            config: Optional configuration containing thread and checkpoint information.
            filter: Optional filter criteria.
            before: Optional configuration to list checkpoints before.
            limit: Optional maximum number of checkpoints to return.

        Returns:
            Iterator[CheckpointTuple]: Iterator of checkpoint tuples.

        Raises:
            CheckpointReadError: If there's an error reading from the database.
        """
        thread_id = (
            str(config.get("configurable", {}).get("thread_id", "")) if config else ""
        )
        checkpoint_ns = (
            config.get("configurable", {}).get("checkpoint_ns", "") if config else ""
        )
        query = """
        SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata, metadata_type
        FROM checkpoint
        WHERE thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns
        ORDER BY checkpoint_id DESC
        """

        vars = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
        }

        if limit:
            vars["limit"] = limit
            query += " LIMIT $limit"

        with self.db_connection() as connection:
            try:
                results = connection.query(query, vars)
            except Exception as e:
                logger.error(
                    "Failed to query checkpoints",
                    extra={
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "error": str(e),
                    },
                )
                raise CheckpointReadError(
                    f"Unable to retrieve checkpoints: {str(e)}"
                ) from e

            for r in results:
                try:
                    thread_id = r["thread_id"]
                    checkpoint_ns = r["checkpoint_ns"]
                    checkpoint_id = r["checkpoint_id"]
                    parent_checkpoint_id = r["parent_checkpoint_id"]
                    type_ = r["type"]
                    checkpoint = r["checkpoint"]
                    metadata = r["metadata"]
                    metadata_type = r.get("metadata_type", type_)

                    query = """
                    SELECT task_id, channel, type, value, idx
                    FROM write
                    WHERE thread_id = $thread_id
                    AND checkpoint_ns = $checkpoint_ns
                    AND checkpoint_id = $checkpoint_id
                    ORDER BY task_id, idx
                    """

                    vars = {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                    }

                    try:
                        task_results = connection.query(query, vars)
                    except Exception as e:
                        logger.error(
                            "Failed to query write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to retrieve write data: {str(e)}"
                        ) from e

                    try:
                        checkpoint_data = self.serde.loads_typed((type_, checkpoint))
                        metadata_dict = cast(
                            CheckpointMetadata,
                            _load_metadata_with_fallback(
                                self.serde, metadata, metadata_type
                            ),
                        )
                        writes = [
                            (
                                tr["task_id"],
                                tr["channel"],
                                self.serde.loads_typed((type_, tr["value"])),
                            )
                            for tr in task_results
                        ]
                    except Exception as e:
                        logger.error(
                            "Failed to deserialize checkpoint or write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to deserialize checkpoint data: {str(e)}"
                        ) from e

                    yield CheckpointTuple(
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                            }
                        },
                        checkpoint_data,
                        metadata_dict,
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
                        writes,
                    )
                except Exception as e:
                    if not isinstance(e, CheckpointReadError):
                        logger.error(
                            "Unexpected error processing checkpoint",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                                "error_type": type(e).__name__,
                            },
                        )
                        raise CheckpointReadError(
                            f"Unexpected error processing checkpoint: {str(e)}"
                        ) from e
                    raise

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to the database.

        Args:
            config: The configuration containing thread and checkpoint information.
            checkpoint: The checkpoint data to save.
            metadata: Metadata associated with the checkpoint.
            new_versions: Version information for channels.

        Returns:
            RunnableConfig: Updated configuration.

        Raises:
            CheckpointSaveError: If there's an error saving to the database.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        try:
            type_, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
            metadata_type, serialized_metadata = self.serde.dumps_typed(
                get_checkpoint_metadata(config, metadata)
            )
        except Exception as e:
            logger.error(
                "Failed to serialize checkpoint data",
                extra={
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "error": str(e),
                },
            )
            raise CheckpointSaveError(
                f"Unable to serialize checkpoint data: {str(e)}"
            ) from e

        with self.db_connection() as connection:
            try:
                query = """
                SELECT id FROM checkpoint
                WHERE thread_id = $thread_id
                AND checkpoint_ns = $checkpoint_ns
                AND checkpoint_id = $checkpoint_id
                """

                vars = {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint["id"],
                }

                existing_query = connection.query(query, vars)
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
                    "metadata_type": metadata_type,
                }

                connection.upsert(record_id, merge_data)
            except Exception as e:
                logger.error(
                    "Failed to save checkpoint",
                    extra={
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint["id"],
                        "error": str(e),
                    },
                )
                raise CheckpointSaveError(
                    f"Unable to save checkpoint data: {str(e)}"
                ) from e

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
        """Save writes to the database.

        Args:
            config: The configuration containing thread and checkpoint information.
            writes: Sequence of writes to save.
            task_id: ID of the task.
            task_path: Optional path of the task.

        Raises:
            CheckpointSaveError: If there's an error saving to the database.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = config["configurable"]["checkpoint_id"]

        for idx, (channel, value) in enumerate(writes):
            try:
                type_, serialized_value = self.serde.dumps_typed(value)
            except Exception as e:
                logger.error(
                    "Failed to serialize write data",
                    extra={
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                        "task_id": task_id,
                        "channel": channel,
                        "error": str(e),
                    },
                )
                raise CheckpointSaveError(
                    f"Unable to serialize write data: {str(e)}"
                ) from e

            merge_data = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "idx": WRITES_IDX_MAP.get(channel, idx),
                "channel": channel,
                "type": type_,
                "value": serialized_value,
                "task_path": task_path,
            }

            with self.db_connection() as connection:
                try:
                    connection.upsert("write", merge_data)
                except Exception as e:
                    logger.error(
                        "Failed to save write data",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                            "task_id": task_id,
                            "channel": channel,
                            "error": str(e),
                        },
                    )
                    raise CheckpointSaveError(
                        f"Unable to save write data: {str(e)}"
                    ) from e

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database asynchronously.

        Args:
            config: The configuration containing thread and checkpoint information.

        Returns:
            Optional[CheckpointTuple]: The checkpoint tuple if found, None otherwise.

        Raises:
            CheckpointReadError: If there's an error reading from the database.
        """
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        async with self.adb_connection() as connection:
            try:
                thread_id = str(config["configurable"]["thread_id"])

                query = """
                SELECT thread_id, checkpoint_id, parent_checkpoint_id, type,
                checkpoint, metadata, metadata_type
                FROM checkpoint WHERE
                thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns
                """

                vars = {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns}

                if checkpoint_id := get_checkpoint_id(config):
                    vars["checkpoint_id"] = checkpoint_id
                    query += " AND checkpoint_id = $checkpoint_id"
                else:
                    query += " ORDER BY checkpoint_id DESC limit 1"

                try:
                    result = await connection.query(query, vars)
                except Exception as e:
                    logger.error(
                        "Failed to query checkpoint data",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "error": str(e),
                        },
                    )
                    raise CheckpointReadError(
                        f"Unable to retrieve checkpoint data: {str(e)}"
                    ) from e

                if len(result) > 0:
                    result_dict = result[0]
                    thread_id = result_dict["thread_id"]
                    checkpoint_id = result_dict["checkpoint_id"]
                    parent_checkpoint_id = result_dict["parent_checkpoint_id"]
                    type_ = result_dict["type"]
                    checkpoint = result_dict["checkpoint"]
                    metadata = result_dict["metadata"]
                    metadata_type = result_dict.get("metadata_type", type_)
                    if not get_checkpoint_id(config):
                        config = {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                            }
                        }

                    # find any pending writes
                    query = """
                    SELECT task_id, channel, type, value, idx
                    FROM write
                    WHERE thread_id = $thread_id
                    AND checkpoint_ns = $checkpoint_ns
                    AND checkpoint_id = $checkpoint_id
                    ORDER BY task_id, idx
                    """

                    vars = {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                    }

                    try:
                        results = await connection.query(query, vars)
                    except Exception as e:
                        logger.error(
                            "Failed to query write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to retrieve write data: {str(e)}"
                        ) from e

                    try:
                        checkpoint_data = self.serde.loads_typed((type_, checkpoint))
                        metadata_dict = cast(
                            CheckpointMetadata,
                            _load_metadata_with_fallback(
                                self.serde, metadata, metadata_type
                            ),
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to deserialize checkpoint data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to deserialize checkpoint data: {str(e)}"
                        ) from e

                    try:
                        writes = [
                            (
                                r["task_id"],
                                r["channel"],
                                self.serde.loads_typed((type_, r["value"])),
                            )
                            for r in results
                        ]
                    except Exception as e:
                        logger.error(
                            "Failed to deserialize write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to deserialize write data: {str(e)}"
                        ) from e

                    return CheckpointTuple(
                        config,
                        checkpoint_data,
                        metadata_dict,
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
                        writes,
                    )
                else:
                    logger.debug(
                        "No checkpoint found",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                        },
                    )
                    return None
            except Exception as e:
                if not isinstance(e, CheckpointReadError):
                    logger.error(
                        "Unexpected error retrieving checkpoint",
                        extra={
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )
                    raise CheckpointReadError(
                        f"Unexpected error retrieving checkpoint: {str(e)}"
                    ) from e
                raise

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints from the database asynchronously.

        Args:
            config: Optional configuration containing thread and checkpoint information.
            filter: Optional filter criteria.
            before: Optional configuration to list checkpoints before.
            limit: Optional maximum number of checkpoints to return.

        Returns:
            AsyncIterator[CheckpointTuple]: Iterator of checkpoint tuples.

        Raises:
            CheckpointReadError: If there's an error reading from the database.
        """
        thread_id = (
            str(config.get("configurable", {}).get("thread_id", "")) if config else ""
        )
        checkpoint_ns = (
            config.get("configurable", {}).get("checkpoint_ns", "") if config else ""
        )
        query = """
        SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata, metadata_type
        FROM checkpoint
        WHERE thread_id = $thread_id AND checkpoint_ns = $checkpoint_ns
        ORDER BY checkpoint_id DESC
        """

        vars = {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
        }

        if limit:
            vars["limit"] = limit
            query += " LIMIT $limit"

        async with self.adb_connection() as connection:
            try:
                results = await connection.query(query, vars)
            except Exception as e:
                logger.error(
                    "Failed to query checkpoints",
                    extra={
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "error": str(e),
                    },
                )
                raise CheckpointReadError(
                    f"Unable to retrieve checkpoints: {str(e)}"
                ) from e

            for r in results:
                try:
                    thread_id = r["thread_id"]
                    checkpoint_ns = r["checkpoint_ns"]
                    checkpoint_id = r["checkpoint_id"]
                    parent_checkpoint_id = r["parent_checkpoint_id"]
                    type_ = r["type"]
                    checkpoint = r["checkpoint"]
                    metadata = r["metadata"]
                    metadata_type = r.get("metadata_type", type_)

                    query = """
                    SELECT task_id, channel, type, value, idx
                    FROM write
                    WHERE thread_id = $thread_id
                    AND checkpoint_ns = $checkpoint_ns
                    AND checkpoint_id = $checkpoint_id
                    ORDER BY task_id, idx
                    """

                    vars = {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                    }

                    try:
                        task_results = await connection.query(query, vars)
                    except Exception as e:
                        logger.error(
                            "Failed to query write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to retrieve write data: {str(e)}"
                        ) from e

                    try:
                        checkpoint_data = self.serde.loads_typed((type_, checkpoint))
                        metadata_dict = cast(
                            CheckpointMetadata,
                            _load_metadata_with_fallback(
                                self.serde, metadata, metadata_type
                            ),
                        )
                        writes = [
                            (
                                tr["task_id"],
                                tr["channel"],
                                self.serde.loads_typed((type_, tr["value"])),
                            )
                            for tr in task_results
                        ]
                    except Exception as e:
                        logger.error(
                            "Failed to deserialize checkpoint or write data",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                            },
                        )
                        raise CheckpointReadError(
                            f"Unable to deserialize checkpoint data: {str(e)}"
                        ) from e

                    yield CheckpointTuple(
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_id,
                            }
                        },
                        checkpoint_data,
                        metadata_dict,
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
                        writes,
                    )
                except Exception as e:
                    if not isinstance(e, CheckpointReadError):
                        logger.error(
                            "Unexpected error processing checkpoint",
                            extra={
                                "thread_id": thread_id,
                                "checkpoint_id": checkpoint_id,
                                "error": str(e),
                                "error_type": type(e).__name__,
                            },
                        )
                        raise CheckpointReadError(
                            f"Unexpected error processing checkpoint: {str(e)}"
                        ) from e
                    raise

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to the database asynchronously.

        Args:
            config: The configuration containing thread and checkpoint information.
            checkpoint: The checkpoint data to save.
            metadata: Metadata associated with the checkpoint.
            new_versions: Version information for channels.

        Returns:
            RunnableConfig: Updated configuration.

        Raises:
            CheckpointSaveError: If there's an error saving to the database.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        try:
            type_, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
            metadata_type, serialized_metadata = self.serde.dumps_typed(
                get_checkpoint_metadata(config, metadata)
            )
        except Exception as e:
            logger.error(
                "Failed to serialize checkpoint data",
                extra={
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "error": str(e),
                },
            )
            raise CheckpointSaveError(
                f"Unable to serialize checkpoint data: {str(e)}"
            ) from e

        async with self.adb_connection() as connection:
            try:
                query = """
                SELECT id FROM checkpoint
                WHERE thread_id = $thread_id
                AND checkpoint_ns = $checkpoint_ns
                AND checkpoint_id = $checkpoint_id
                """

                vars = {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint["id"],
                }

                existing_query = await connection.query(query, vars)
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
                    "metadata_type": metadata_type,
                }

                await connection.upsert(record_id, merge_data)
            except Exception as e:
                logger.error(
                    "Failed to save checkpoint",
                    extra={
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint["id"],
                        "error": str(e),
                    },
                )
                raise CheckpointSaveError(
                    f"Unable to save checkpoint data: {str(e)}"
                ) from e

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Save writes to the database asynchronously.

        Args:
            config: The configuration containing thread and checkpoint information.
            writes: Sequence of writes to save.
            task_id: ID of the task.
            task_path: Optional path of the task.

        Raises:
            CheckpointSaveError: If there's an error saving to the database.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = config["configurable"]["checkpoint_id"]

        async with self.adb_connection() as connection:
            for idx, (channel, value) in enumerate(writes):
                try:
                    type_, serialized_value = self.serde.dumps_typed(value)
                except Exception as e:
                    logger.error(
                        "Failed to serialize write data",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                            "task_id": task_id,
                            "channel": channel,
                            "error": str(e),
                        },
                    )
                    raise CheckpointSaveError(
                        f"Unable to serialize write data: {str(e)}"
                    ) from e

                merge_data = {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                    "task_id": task_id,
                    "idx": WRITES_IDX_MAP.get(channel, idx),
                    "channel": channel,
                    "type": type_,
                    "value": serialized_value,
                    "task_path": task_path,
                }

                try:
                    await connection.upsert("write", merge_data)
                except Exception as e:
                    logger.error(
                        "Failed to save write data",
                        extra={
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                            "task_id": task_id,
                            "channel": channel,
                            "error": str(e),
                        },
                    )
                    raise CheckpointSaveError(
                        f"Unable to save write data: {str(e)}"
                    ) from e

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
