import faust
from typing import List

class Entity(faust.Record, polymorphic_fields=True):
    id: str
    events: list

class NamedEntity(Entity):
    name: str
    value: int

class EntityEvent(faust.Record, polymorphic_fields=True):
    id: str

    def mutate(self, e: Entity):
        e = self.apply(e)
        e.events.append(self)
        return e

    def apply(self, e: NamedEntity):
        raise NotImplementedError

class Created(EntityEvent):
    name: str
    value: int

    def apply(self, e: NamedEntity):
        return NamedEntity(id=self.id, name=self.name, value=self.value, events=[self])

    

class NameUpdated(EntityEvent):
    name: str
    old_name: str

    def apply(self, e: NamedEntity):
        e.name = self.name
        return e


class ValueUpdate(EntityEvent):
    value: int

    def apply(self, e: NamedEntity):
        e.value = self.value
        return e

class NameTotalUpdate(faust.Record):
    name: str
    value: int