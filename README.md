# Surreal Saver v1.4.0

A SurrealDB Checkpointer for Langgraph with both synchronous and asynchronous support.

## Installation

```sh
pip install langgraph-checkpoint-surrealdb
```

## Usage

### Synchronous Usage

```python
from langgraph_checkpoint_surrealdb import SurrealSaver
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field
from typing import Annotated
import operator

# Initialize SurrealDB connection
memory = SurrealSaver(
    url="ws://localhost:8000/rpc",
    user="root",
    password="root",
    namespace="ns",
    database="db"
)

class ThreadState(BaseModel):
    messages: Annotated[list, operator.add] = Field(default_factory=list)

def get_model_answer(state: ThreadState, config: RunnableConfig) -> dict:
    if state.messages == [] or state.messages == [""]:
        return {"messages": []}

    model = ChatOpenAI(model="gpt-4")
    sys_prompt = "You are a helpful assistant"
    ai_message = model.invoke([sys_prompt] + state.messages)
    return {"messages": [ai_message]}

agent_state = StateGraph(ThreadState)
agent_state.add_node("get_model_answer", get_model_answer)
agent_state.add_edge(START, "get_model_answer")
agent_state.add_edge("get_model_answer", END)
graph = agent_state.compile(checkpointer=memory)

# Use synchronously
result = graph.invoke(
    input={"messages": [HumanMessage(content="Hello")]},
    config={"configurable": {"thread_id": "123"}}
)
```

### Asynchronous Usage

```python
# Same setup as above, but use async methods

# Use asynchronously
result = await graph.ainvoke(
    input={"messages": [HumanMessage(content="Hello")]},
    config={"configurable": {"thread_id": "123"}}
)

# Async methods available:
# - aget_tuple: Get a checkpoint tuple asynchronously
# - alist: List checkpoints asynchronously
# - aput: Save a checkpoint asynchronously
# - aput_writes: Save writes asynchronously
```

The SurrealSaver provides both synchronous and asynchronous methods for all operations, allowing you to choose the appropriate approach based on your application's needs.
