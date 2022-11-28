import faust
import json
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

    def apply(self, e: NamedEntity):
        e.name = self.name
        return e


class ValueUpdate(EntityEvent):
    value: int

    def apply(self, e: NamedEntity):
        e.value = self.value
        return e


app = faust.App('consumer-app', broker='kafka_es://9092')
app_events = app.topic('app-events', key_type=str, value_type=EntityEvent)
entity_states = app.Table('entity-states', default=None)
name_totals = app.Table('name-totals', default=int)

@app.agent(app_events)
async def observe_app_events(events):
    async for event in events.group_by(EntityEvent.id):
        print(f'received event {type(event)} {event.id}')
        existing = entity_states.get(event.id)
        entity_states[event.id] = event.mutate(existing)

@app.agent(app_events)
async def observe_created_name_totals(events):
    async for event in events.filter(lambda e: type(e) in [Created]).group_by(EntityEvent.id):
        print(f'received event affecting name {type(event)}')
        name_totals[event.name] += 1

@app.agent(app_events)
async def observe_name_update_totals(events):
    async for event in events.filter(lambda e: type(e) in [NameUpdated]).group_by(EntityEvent.id):
        print(f'received event affecting name {type(event)}')
        existing = entity_states.get(event.id)
        name_totals[existing.name] -= 1
        name_totals[event.name] += 1

@app.page('/entities')
async def get_entities(self, request):
    result = {}
    for k, v in entity_states.items():
        result[k] = {'name': v.name, 'value': v.value}
    return self.json(result)

@app.page('/entities/{id}')
@app.table_route(table=entity_states, match_info='id')
async def get_entity(self, request, id):
    v: NamedEntity = entity_states.get(id)
    if v:
        events = [{'type': str(type(e)) for e in v.events}]
        return self.json(
            {
                'name': v.name,
                'value': v.value,
                'events': events
            })
    return self.json(['Not found'])

@app.page('/names')
async def get_name_totals(self, request):
    result = {}
    for k, v in name_totals.items():
        result[k] = {k: v}
    return self.json(result)
