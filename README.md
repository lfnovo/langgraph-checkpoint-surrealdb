
# Surreal Saver

A work in progress implementation of a SurrealDB Checkpointer for Langgraph.


## TODO:

- Write tests
- Implement Async
- Support older versions of langgraph

## USAGE

```python
# Initialize SurrealDB connection
memory = SurrealSaver(url="ws://localhost:8000/rpc", user="root", password="root", namespace="ns", database="db")

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
```