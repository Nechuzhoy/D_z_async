import asyncio
from pprint import pprint

import aiohttp
import datetime

from more_itertools import chunked

from models import init_db, Session, SwapiPeople

MAX_CHUNK = 5


async def get_person(person_id, session):
    http_response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    if http_response.status != 200:
        return
    else:
        person_json_data = await http_response.json()
        return person_json_data


async def get_deep_url(url, key, session):
    async with session.get(f'{url}') as response:
        data = await response.json()
        return data[key]


async def get_deep_urls(urls, key, session):
    tasks = (asyncio.create_task(get_deep_url(url, key, session)) for url in urls)
    for task in tasks:
        yield await task


async def get_deep_data(urls, key, session):
    result_list = []
    async for item in get_deep_urls(urls, key, session):
        result_list.append(item)
    return ', '.join(result_list)


async def insert_people_list(people_chunk, session):
    person_list = []
    for person_json in people_chunk:
        if person_json is not None:
            person_dir = {
                'birth_year': person_json['birth_year'],
                'eye_color': person_json['eye_color'],
                'films': await get_deep_data(person_json['films'], 'title', session),
                'gender': person_json['gender'],
                'hair_color': person_json['hair_color'],
                'height': person_json['height'],
                'homeworld': await get_deep_data([person_json['homeworld']], 'name', session),
                'mass': person_json['mass'],
                'name': person_json['name'],
                'skin_color': person_json['skin_color'],
                'species': await get_deep_data(person_json['species'], 'name', session),
                'starships': await get_deep_data(person_json['starships'], 'name', session),
                'vehicles': await get_deep_data(person_json['vehicles'], 'name', session)

            }

            person_list.append(person_dir)

    return person_list


async def insert_records(result, session):
    pprint(result)
    records = await insert_people_list(result, session)
    record = [SwapiPeople(**record) for record in records]
    async with Session() as session:
        session.add_all(record)
        await session.commit()


async def main():
    await init_db()
    session = aiohttp.ClientSession()
    for people_id_chunk in chunked(range(1, 90), MAX_CHUNK):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        await asyncio.create_task(insert_records(result, session))
    await session.close()
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
