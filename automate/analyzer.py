import sqlite3
import time

from datetime import datetime

from openpyxl import Workbook



visits_query = """
SELECT DISTINCT
    writers.writer,
    visits.writer_qid,
    sum(visits.visits),
    avg(visits.visits),
    writers.ethnicity,
    writers.language,
    writers.birthdate,
    writers.deathdate,
    writers.birthplace,
    writers.geo_lat,
    writers.geo_lon,
    writers.article
FROM writers, visits
WHERE writers.writer_qid = visits.writer_qid
GROUP BY writers.writer_qid
"""

def fetch_query(query):
    start = time.time()
    print("Starting query:\n", query)
    print('Execution is on the go...\n')

    con = sqlite3.connect('writers_list2.sqlite3')
    cur = con.cursor()

    conntime = time.time()
    print('Connect time: ', conntime-start)

    result = con.execute(query)
    executetime = time.time()
    print('Execute time: ', executetime-conntime)

    fetched = result.fetchall()
    fetchtime = time.time()

    print('Fetch time:   ', fetchtime-executetime)
    print('Total time:   ', fetchtime-start, '\n')

    con.close()

    return fetched


rows = fetch_query(visits_query)

wb = Workbook()
# wb.iso_dates = True

ws = wb.active
ws.title = "Writers info"

print('Writing XLSX file...')

start = time.time()

ws.append(('Автор', 'Wikidata ID', 'Посещений', 'Посещений в среднем',
            'Национальность', 'Язык', 'Дата рождения', 'Дата смерти',
            'Место рождения', 'Широта', 'Долгота', 'Wikipedia'))

def generate_date(item):
    try:
        parsedate = datetime.strptime(item, '%Y-%m-%d %H:%M:%S')
    except TypeError:
        return 'None'
    except ValueError:
        return 'Unknown'
    except Exception as e:
        return 'Undefined'

    return f'{parsedate.day:02d}.{parsedate.month:02d}.{parsedate.year}'

for num, row in enumerate(rows):
    ws.append((
        str(row[0]),
        int(row[1]),
        int(row[2]),
        float(row[3]),
        str(row[4]),
        str(row[5]),
        generate_date(row[6]),
        generate_date(row[7]),
        str(row[8]),
        float(row[9]),
        float(row[10]),
        str(row[11])
        ))
    print('Added row {:>5d}'.format(num), sep='', end='\r')

print ('\nAdded at', time.time()-start, 'seconds')

wb.save(filename = 'writers_stat.xlsx')

print ('File saved.')
