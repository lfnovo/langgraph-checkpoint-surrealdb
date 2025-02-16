import operator
import uuid
from typing import Annotated

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

from langgraph_checkpoint_surrealdb import SurrealSaver

load_dotenv()


class ThreadState(BaseModel):
    messages: Annotated[list, operator.add] = Field(default_factory=list)


def get_model_answer(state: ThreadState, config: RunnableConfig) -> dict:
    if state.messages == [] or state.messages == [""]:
        return {"messages": []}

    model = ChatOpenAI(model="gpt-4")
    sys_prompt = "You are a helpful assistant"
    ai_message = model.invoke([sys_prompt] + state.messages)
    return {"messages": [ai_message]}


@pytest_asyncio.fixture(scope="function")
async def memory():
    saver = SurrealSaver(
        url="ws://localhost:8018/rpc",
        user="root",
        password="root",
        namespace="ns",
        database="db",
    )
    yield saver
    # Clean up connections
    try:
        async with saver.adb_connection() as conn:
            await conn.close()
    except:
        pass


@pytest_asyncio.fixture(scope="function")
async def graph(memory):
    agent_state = StateGraph(ThreadState)
    agent_state.add_node("get_model_answer", get_model_answer)
    agent_state.add_edge(START, "get_model_answer")
    agent_state.add_edge("get_model_answer", END)
    compiled_graph = agent_state.compile(checkpointer=memory)
    yield compiled_graph
    # Clean up any remaining connections
    try:
        if hasattr(memory, "_connection"):
            with memory.db_connection() as conn:
                conn.close()
    except:
        pass


@pytest.fixture
def input_data():
    return {"messages": [HumanMessage(content="Oi")]}


@pytest.mark.asyncio
async def test_sync_invocation(graph, input_data):
    thread = str(uuid.uuid4())
    result = graph.invoke(
        input=input_data, config=dict(configurable={"thread_id": thread})
    )

    # Check the state
    result_state = graph.get_state(config=dict(configurable={"thread_id": thread}))
    messages = result_state.values["messages"]
    assert messages, "Messages should not be empty"


@pytest.mark.asyncio
async def test_async_invocation(graph, input_data):
    thread = str(uuid.uuid4())
    result = await graph.ainvoke(
        input=input_data, config=dict(configurable={"thread_id": thread})
    )

    # Check the state
    result_state = graph.get_state(config=dict(configurable={"thread_id": thread}))
    messages = result_state.values["messages"]
    assert messages, "Messages should not be empty"
