import faust
import json
from typing import List

from kafkaes.domain.entities import *

app = faust.App('consumer-app', broker='kafka_es://9092')
app_events = app.topic('app-events', key_type=str, value_type=EntityEvent)
entity_states = app.Table('entity-states', default=None)
name_total_updates = app.topic('name-total-updates', key_type=str, value_type=NameTotalUpdate)
name_totals = app.Table('name-totals', default=int)

@app.agent(app_events)
async def observe_app_events(events):
    async for event in events.group_by(EntityEvent.id):
        print(f'received event {type(event)} {event.id}')
        existing = entity_states.get(event.id)
        entity_states[event.id] = event.mutate(existing)

@app.agent(app_events)
async def project_app_events_to_name_total_update(events):
    async for event in events.filter(lambda e: type(e) in [Created, NameUpdated]):
        print(f'received event affecting name {type(event)}')
        if type(event) == Created:
            await name_total_updates.send(key=event.name, value=NameTotalUpdate(name=event.name, value=1))
        elif type(event) == NameUpdated:
            await name_total_updates.send(key=event.old_name, value=NameTotalUpdate(name=event.old_name, value=-1))
            await name_total_updates.send(key=event.name, value=NameTotalUpdate(name=event.name, value=1))

@app.agent(name_total_updates)
async def observe_name_update_totals(events):
    async for event in events.group_by(NameTotalUpdate.name):
        print(f'received event affecting name total {type(event)}')
        name_totals[event.name] += event.value

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
    return self.json({k:v for k,v in name_totals.items()})
