import asyncio
import operator
import uuid
from typing import cast
from uuid import uuid4

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

from langgraph_checkpoint_surrealdb import SurrealSaver

load_dotenv()


# Define event_loop fixture at session scope to resolve scope mismatch
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Global memory instance for stress tests (can be reused or reinitialized per test if needed)
memory = SurrealSaver(
    url="ws://localhost:8018/rpc",
    user="root",
    password="root",
    namespace="ns",
    database="db",
)


class ThreadState(BaseModel):
    messages: list = Field(default_factory=list)


def get_model_answer(state: ThreadState, config: RunnableConfig) -> dict:
    # For stress testing, use a fixed AI response.
    message = AIMessage(content="This is a repeated AI response")
    return {"messages": [message]}


@pytest.fixture(scope="session")
def compiled_graph_sync():
    agent_state = StateGraph(ThreadState)
    agent_state.add_node("get_model_answer", get_model_answer)
    agent_state.add_edge(START, "get_model_answer")
    agent_state.add_edge("get_model_answer", END)
    compiled_graph = agent_state.compile(checkpointer=memory)
    return compiled_graph


@pytest_asyncio.fixture(scope="session")
async def compiled_graph_async():
    # Assuming the same graph can support async invocation via ainvoke.
    agent_state = StateGraph(ThreadState)
    agent_state.add_node("get_model_answer", get_model_answer)
    agent_state.add_edge(START, "get_model_answer")
    agent_state.add_edge("get_model_answer", END)
    compiled_graph = agent_state.compile(checkpointer=memory)
    return compiled_graph


class TestSyncStress:
    @pytest.mark.parametrize(
        "iterations,expected_total",
        [
            (10, 1),
            (20, 1),
        ],
    )
    def test_sequential_invocations(
        self, compiled_graph_sync, iterations, expected_total
    ):
        thread_id = str(uuid4())
        # Explicitly cast to RunnableConfig.
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
        # Using a simple message for each invocation.
        message = HumanMessage(content="Stress test message")
        for _ in range(iterations):
            compiled_graph_sync.invoke(input={"messages": message}, config=config)
        # Since the graph overwrites state per invocation, final state should contain 1 message.
        result_state = compiled_graph_sync.get_state(config=config)
        total_messages = len(result_state.values.get("messages", []))
        assert total_messages == expected_total, (
            f"Expected {expected_total} messages, got {total_messages}"
        )


class TestAsyncStress:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("concurrent_tasks", [10, 20])
    async def test_concurrent_invocations(self, compiled_graph_async, concurrent_tasks):
        async def invoke_once():
            thread_id = str(uuid4())
            config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
            message = HumanMessage(content="Async stress test message")
            await compiled_graph_async.ainvoke(
                input={"messages": message}, config=config
            )
            state = compiled_graph_async.get_state(config=config)
            return len(state.values.get("messages", []))

        results = await asyncio.gather(
            *(invoke_once() for _ in range(concurrent_tasks))
        )
        for count in results:
            assert count == 1, (
                f"Each async invocation should return 1 message, got {count}"
            )
