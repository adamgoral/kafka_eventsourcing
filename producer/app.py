import faust
import random as rnd
import uuid
import os

from kafkaes.domain.entities import *

names = ['Tina', 'Bob', 'Francis', 'Tom', 'Rebeka']

app = faust.App('producer-app', broker=os.environ['KAFKA_BROKER'])
app_events = app.topic('app-events', key_type=str, value_type=EntityEvent)

@app.timer(interval=1.0)
async def creator(app):
    name = rnd.choice(names)
    value = rnd.randint(1, 100)
    id = str(uuid.uuid4())
    entity_event = Created(id=id, name=name, value=value)
    print(f'creator sending event {type(entity_event)} {id}')
    await app_events.send(key=id, value=entity_event)
