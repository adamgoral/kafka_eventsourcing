import faust
import json

class NamedEntity(faust.Record):
    id: str
    name: str
    value: int

class EntityEvent(faust.Record, polymorphic_fields=True):
    id: str

    def apply(self, e: NamedEntity):
        raise NotImplementedError

class Created(EntityEvent):
    name: str
    value: int

    def apply(self, e: NamedEntity):
        return NamedEntity(id=self.id, name=self.name, value=self.value)

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


@app.agent(app_events)
async def observe_app_events(events):
    async for event in events.group_by(EntityEvent.id):
        print(f'received event {type(event)} {event.id}')
        existing = entity_states.get(event.id)
        entity_states[event.id] = event.apply(existing)

@app.page('/entities')
async def get_entities(self, request):
    result = {}
    for k, v in entity_states.items():
        result[k] = {'name': v.name, 'value': v.value}
    return self.json(result)

@app.page('/entities/{id}')
@app.table_route(table=entity_states, match_info='id')
async def get_entity(self, request, id):
    v = entity_states.get(id)
    if v:
        return self.json({'name': v.name, 'value': v.value})
    return self.json(['Not found'])