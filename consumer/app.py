import faust
import json
from typing import List
import os
import random as rnd
import asyncio

from kafkaes.domain.entities import *

app = faust.App('consumer-app', broker=os.environ['KAFKA_BROKER'])
app_events = app.topic('app-events', key_type=str, value_type=EntityEvent)
entity_states = app.Table('entity-states', default=None)
name_total_updates = app.topic('name-total-updates', key_type=str, value_type=NameTotalUpdate)
name_totals = app.Table('name-totals', default=int)

names = ['Tina', 'Bob', 'Francis', 'Tom', 'Rebeka']


@app.agent(app_events)
async def randomly_change_name_after_create(events):
    async for event in events.filter(lambda e: type(e) == Created):
        if rnd.random() < 0.5:
            new_name = rnd.choice(names)
            if event.name != new_name:
                entity_event = NameUpdated(id = event.id, name=new_name, old_name=event.name)
                await asyncio.sleep(1)
                print(f'sending event {type(entity_event)} {event.id}')
                await app_events.send(key=event.id, value=entity_event)

@app.agent(app_events)
async def randomly_change_value_after_create(events):
    async for event in events.filter(lambda e: type(e) == Created):
        if rnd.random() < 0.5:
            entity_event = ValueUpdate(id = event.id, value=rnd.randint(0, 100))
            await asyncio.sleep(1)
            print(f'sending event {type(entity_event)} {event.id}')
            await app_events.send(key=event.id, value=entity_event)
