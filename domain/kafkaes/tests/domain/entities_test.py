import pytest
from kafkaes.domain.entities import *

async def test_event_aggregate_mismatch():
    aggregate = NamedAggregate(id='123', name='ddd', value=123)
    event = NameUpdated(id='456', name='abc')
    with pytest.raises(AggregateEventIdMismatch):
        aggregate.mutate(event)

async def test_name_update():
    aggregate = NamedAggregate(id='123', name='ddd', value=123)
    event = NameUpdated(id='123', name='abc')
    aggregate.mutate(event)
    assert aggregate.name == event.name

async def test_value_update():
    aggregate = NamedAggregate(id='123', name='ddd', value=123)
    event = ValueUpdated(id='123', value=662)
    aggregate.mutate(event)
    assert aggregate.value == event.value

async def test_created():
    event = Created(id='123', name='abc', value=123)
    aggregate = NamedAggregate.from_event(event)
    assert aggregate.id == event.id
    assert aggregate.name == event.name
    assert aggregate.value == event.value
