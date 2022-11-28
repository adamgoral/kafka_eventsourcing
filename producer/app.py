import faust
import random as rnd
import uuid
import asyncio

names = ['Tina', 'Bob', 'Francis', 'Tom', 'Rebeka']

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