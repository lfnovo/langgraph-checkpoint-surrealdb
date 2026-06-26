"""Run conformance capabilities against AsyncSurrealSaver."""

from __future__ import annotations
from contextlib import asynccontextmanager
import os

from dotenv import load_dotenv
import pytest
from langgraph.checkpoint.conformance import validate
from langgraph.checkpoint.conformance.initializer import checkpointer_test

from langgraph_checkpoint_surrealdb import SurrealSaver

load_dotenv()

SURREALDB_URL = os.environ.get("SURREALDB_URL", "ws://localhost:8018/rpc")


@asynccontextmanager
async def surreal():
    saver = SurrealSaver(
        url=SURREALDB_URL,
        user="root",
        password="root",
        namespace="ns",
        database="db",
    )
    async with saver.adb_connection() as conn:
        await conn.query("DEFINE TABLE IF NOT EXISTS checkpoint SCHEMALESS")
        await conn.query("DEFINE TABLE IF NOT EXISTS `write` SCHEMALESS")
    saver.setup()
    yield saver
    async with saver.adb_connection() as conn:
        await conn.close()


@pytest.mark.asyncio
async def test_async_conformance():

    @checkpointer_test(name="SurrealSaver")
    async def sqlite_saver():
        async with surreal() as saver:
            yield saver

    report = await validate(sqlite_saver)
    for cap, result in report.results.items():
        if result.passed is False:
            details = "\n".join(result.failures or [])
            pytest.fail(f"Capability {cap} failed:\n{details}")
