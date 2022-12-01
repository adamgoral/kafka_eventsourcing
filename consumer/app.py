import faust
import json
from typing import List
import os

from kafkaes.domain.entities import *

app = faust.App('consumer-app', broker=os.environ['KAFKA_BROKER'])
app_events = app.topic('app-events', key_type=str, value_type=EntityEvent)
named_aggregates = app.Table('named-aggregates', default=None)
id_to_name = app.Table('id-name-lookup', default=None)
name_totals = app.Table('name-totals', default=int)

class UnexpectedEvent(Exception):
    expected_type: type
    actual: EntityEvent

    def __init__(self, expected_type: type, actual: EntityEvent) -> None:
        self.expected_type = expected_type
        self.actual = actual

def named_aggregate_default(event: Created):
    if type(event) != Created:
        raise UnexpectedEvent(expected_type=Created, actual=event)
    else:
        return NamedAggregate.from_event(event)

def get_or_default(table: faust.Table, key, default_factory):
    existing = table.get(key)
    if existing:
        return existing
    return default_factory()

@app.agent(app_events)
async def observe_app_events(events):
    async for event in events.group_by(EntityEvent.id):
        print(f'observe_app_events received event {type(event)} {event.id}')
        existing = get_or_default(named_aggregates, event.id, lambda: named_aggregate_default(event))
        named_aggregates[event.id] = existing.mutate(event)

@app.agent(app_events)
async def track_name_totals(events):
    async for event in events.filter(lambda e: type(e) in [Created, NameUpdated]).group_by(EntityEvent.id):
        print(f'track_name_totals received event affecting name {type(event)}')
        if type(event) == Created:
            id_to_name[event.id] = event.name
            name_totals[event.name] += 1
        elif type(event) == NameUpdated:
            existing = id_to_name.get(event.id)
            name_totals[existing] -= 1
            name_totals[event.name] += 1

@app.page('/entities')
async def get_entities(self, request):
    result = {}
    for k, v in named_aggregates.items():
        result[k] = {'name': v.name, 'value': v.value}
    return self.json(result)

@app.page('/entities/{id}')
@app.table_route(table=named_aggregates, match_info='id')
async def get_entity(self, request, id):
    v: NamedAggregate = named_aggregates.get(id)
    if v:
        return self.json(
            {
                'name': v.name,
                'value': v.value
            })
    return self.json(['Not found'])

@app.page('/names')
async def get_name_totals(self, request):
    return self.json({k:v for k,v in name_totals.items()})
