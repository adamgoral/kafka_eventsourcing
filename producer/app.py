import faust
import random as rnd
import uuid
import asyncio

from kafkaes.domain.entities import *

names = ['Tina', 'Bob', 'Francis', 'Tom', 'Rebeka']

app = faust.App('producer-app', broker='kafka_es://9092')
app_events = app.topic('app-events', key_type=str, value_type=EntityEvent)

@app.timer(interval=1.0)
async def creator(app):
    name = rnd.choice(names)
    value = rnd.randint(1, 100)
    id = str(uuid.uuid4())
    entity_event = Created(id=id, name=name, value=value)
    print(f'sending event {type(entity_event)} {id}')
    await app_events.send(key=id, value=entity_event)

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