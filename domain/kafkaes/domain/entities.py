import faust
from faust.models.fields import FieldDescriptor
from typing import List
from functools import singledispatchmethod

class AggregateEventIdMismatch(Exception):
    aggregate_id: str
    event_id: str

    def __init__(self, aggregate_id: str, event_id: str) -> None:
        self.aggregate_id = aggregate_id
        self.event_id = event_id

class EntityEvent(faust.Record, polymorphic_fields=True):
    id: str

class Created(EntityEvent):
    name: str
    value: int

class NameUpdated(EntityEvent):
    name: str

class ValueUpdated(EntityEvent):
    value: int

class Aggregate(faust.Record, polymorphic_fields=True):
    id: str
    _events: list = FieldDescriptor(required=False, exclude=True, default=list())

    @property
    def uncommited_events(self):
        return tuple(self._events)

    def mutate(self, e:EntityEvent):
        if self.id != e.id:
            raise AggregateEventIdMismatch(aggregate_id=self.id, event_id=e.id)
        self._apply(e)
        self._events.append(e)
        return self

    def _apply(self, e: EntityEvent):
        raise NotImplementedError

class NamedAggregate(Aggregate):
    name: str
    value: int

    @classmethod
    def from_event(cls, e: Created):
        return NamedAggregate(id=e.id, name=e.name, value=e.value)

    @singledispatchmethod
    def _apply(self, e: EntityEvent):
        return super().apply(e)

    @_apply.register(Created)
    def _(self, e: Created):
        self.id = e.id
        self.name = e.name
        self.value = e.value

    @_apply.register(NameUpdated)
    def _(self, e: NameUpdated):
        self.name = e.name

    @_apply.register(ValueUpdated)
    def _(self, e: ValueUpdated):
        self.value = e.value


class NameTotalUpdated(faust.Record):
    name: str
    value: int