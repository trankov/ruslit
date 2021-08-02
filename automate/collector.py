# Export CSV wikidata to SQLite3 database after formatting

import csv

from models import Person, Database

db: object = Database('writers_list2.sqlite3')

table_fields = [n for n in dir(Person) if not n.startswith('_')]

with open ('writers.csv', newline='') as csvfile:
    writers = csv.DictReader(csvfile)

    for writer in writers:
        writer_glittered = Person(writer)
        table_row = {field : getattr(writer_glittered, field) for field in table_fields}
        try:
            db.insert_writer(table_row)
        except Exception as e:
            print('\n', e, '\n', table_row, sep='')
            break

db.commit()
db.close()

print('\nAll done')
