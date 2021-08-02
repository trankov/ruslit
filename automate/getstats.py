# Check visits statistics for wikipedia articles

import sqlite3
import pathlib
import asyncio

from datetime import datetime, timedelta

from models import Dataset, Statistics



async def getstat(chunk):
    """
    Getting Wikipedia statistics in async mode via Statistics().async_wikistat()
    """

    tasks = [asyncio.create_task(Statistics(item, startdate='20150101').async_wikistat())
                for item in chunk]

    st = await asyncio.gather(*tasks)
    return st



def iter_writers():
    """
    Plucking writers step by step getting statistics then keeping it.
    Step width defined in Dataset.limit.
    Every call of iter_chunk() increase offset according to limit value.

    """

    global ds
    chunk = True

    while chunk:
        chunk = ds.iter_chunk()
        next_set = asyncio.run(getstat(chunk))
        ds.insert_stat(next_set)

        print ('\rCurrent offset: {}        '.format(ds.offset), end='')

    print()



# Dataset initial setup

ds = Dataset(pathlib.Path(__file__).parent / 'writers_list2.sqlite3')
ds.fields = 'writer_qid, article'
ds.limit = 90
# ds.offset = 10700


# Main cycle run

iter_writers()


# Close the database

ds.commit()
ds.close()
