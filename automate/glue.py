# Utilities for organise the different raw data in one database

import sqlite3
import csv
import re

from pathlib import Path
from datetime import datetime


# Temporary
from pprint import pprint
import random


class Database(object):
    """
    A helper for easy database operations.
    Requires predefined database structure.

    """

    # It is assumed that ORM is not nessessary here, at least for a while.

    def __init__(self, dbname: Path) -> None:
        self._dbname = dbname
        self._con = sqlite3.connect(dbname)
        self._con.row_factory = sqlite3.Row
        self._cur = self._con.cursor()


    def commit(self) -> None:
        self._con.commit()


    def close(self) -> None:
        self._cur.close()
        self._con.close()


    @property
    def cursor(self) -> sqlite3.Cursor:
        return self._cur


    def select(self, query: str):
        _select = self._cur.execute(query)
        return _select.fetchall()


    def execute(self, query: str) -> dict:
        """
        Execute a `query` and return the result of operation as a dict,
        where 'result' is `True` (if successfull) or `False` (if not),
        and 'exception' is an `Exception` object (if raised).

        """

        try:
            self._cur.execute(query)
            return {'result' : True, 'exception' : None},
        except Exception as e:
            return {'result' : False, 'exception' : e}


    def insert(self, table: str, values, unique: bool = False):
        """
        Insert dict or list of dicts into the table.
        Keys have to be equal to columns headers.
          `values` must be a list of dicts or a dict.
          `table: str` is the name of the table.
          `unique: bool` provides how to handle duplicated values.

        """

        if all((type(values) is not list, type(values) is not dict)):
            raise TypeError('<values> must be a dict or a list of dicts')

        columns = list(values[0].keys()) if type(values) is list else list(values.keys())

        query = 'INSERT {} INTO {} ({}) VALUES ({})'.format(
                                        {False: '', True: 'OR IGNORE'}[unique],
                                        table,
                                        ', '.join(columns),
                                        ', '.join(f':{i}' for i in columns))

        if type(values) is dict:
            self._cur.execute(query, values)
        elif type(values) is list:
            self._cur.executemany(query, values)

        self._con.commit()


    def create_tables(self) -> None:
        """
        Create a tables that is nessesary for work.
        If table is exists, nothing happens.

        """

        tables = {

            # The person itself
            'persons' : 'id         INTEGER PRIMARY KEY AUTOINCREMENT,'
                        'qid        INTEGER UNIQUE NOT NULL,'
                        'name       TEXT NOT NULL,'
                        'article    TEXT',

            # Places where persons were born, die or buried (burials might be more than one)
            'places'  : 'id         INTEGER PRIMARY KEY AUTOINCREMENT,'
                        'qid        INTEGER UNIQUE NOT NULL,'
                        'name       TEXT NOT NULL,'
                        'lat        REAL,'
                        'lon        REAL',

            # Dates of births without reference to a specific person
            # Serves as a pattern for a similar table `deaths`
            'births'  : 'id         INTEGER PRIMARY KEY AUTOINCREMENT,'
                        'datestring TEXT UNIQUE NOT NULL',

            # List of mentioned nationalities
            # Serves as a pattern for a similar table `languages`
            'ethnicity': 'id        INTEGER PRIMARY KEY AUTOINCREMENT,'
                         'qid       INTEGER UNIQUE NOT NULL,'
                         'title     TEXT NOT NULL',

            # Manners of death
            # Serves as a pattern for a similar table `dcauses`
            'dmanners':  'id        INTEGER PRIMARY KEY AUTOINCREMENT,'
                         'qid       INTEGER UNIQUE NOT NULL,'
                         'defining  TEXT NOT NULL',

            # Wikipedia article visits by year/month
            'visits'  : 'id         INTEGER PRIMARY KEY AUTOINCREMENT,'
                        'person_id  INTEGER NOT NULL,'
                        'person_qid INTEGER NOT NULL,'
                        'datestring TEXT NOT NULL,'
                        'number     INTEGER NOT NULL',

            # Many-to-many relationships between Persons and Birth dates
            # Serves as a pattern for a similar table `ddates`
            'bdates'  : 'person_id  INTEGER NOT NULL,'
                        'person_qid INTEGER NOT NULL,'
                        'date_id    INTEGER NOT NULL',

            # Many to many relationships between Persons and Birth places
            # Serves as a pattern for a similar tables `dplaces` and `burials`
            'bplaces' : 'person_id  INTEGER NOT NULL,'
                        'person_qid INTEGER NOT NULL,'
                        'place_qid  INTEGER NOT NULL,'
                        'place_id   INTEGER NOT NULL',

            # Counts of mentiones for each person in acceptable WMF projects
            'ratings' : 'person_id  INTEGER NOT NULL,'
                        'person_qid INTEGER NOT NULL,'
                        'wikipedia  INTEGER,'
                        'wikisource INTEGER,'
                        'wikiquote  INTEGER',
        }

        # Assign the equal values for DRY
        for i in [('deaths', 'births'), ('language', 'ethnicity'),
                  ('dcauses', 'dmanners'), ('ddates', 'bdates'),
                  ('dplaces', 'bplaces'), ('burials', 'bplaces')]:

            tables[i[0]] = tables[i[1]]

        for k, v in tables.items():
            self._cur.execute ('CREATE TABLE IF NOT EXISTS {} ({});'.format(k, v))



class CSV(object):
    """
    Handle CSV import operations.

    """

    def __init__(self, filename: Path):
        self.source = open(filename, encoding='utf-8', newline='')
        self.rows = csv.DictReader(self.source)


    def close(self):
        """
        Must be called every time the work is done but
        StopIteration in nextrow() method doesn't reached.

        """

        if not self.source.closed: self.source.close()


    @property
    def closed(self) -> bool:
        return self.source.closed


    @property
    def nextrow(self):
        """
        Iterating trough the .csv file and closing it after the last line.
        Returns dict with columns headers as keys or None if the lines have ended.

        Example:

          while not my_csv_object.closed:
              print (my_csv_object.nextrow or 'Done')

        """

        try:
            row = next(self.rows)
        except StopIteration:
            self.source.close()
            return None
        return row


    def chew_qid(self, bite: str) -> int:
        """
        Returns last digits after "/Q" from urls like http://www.wikidata.org/.../Q315279

        """

        _qid = re.findall(r"^http.+Q(\d+)$", bite)
        return int(_qid[-1]) if _qid else -1


def persons():
    """
    Import info about authors from rawsrc and put it into the database.

    """

    writers = dblist.select('SELECT DISTINCT writer_qid, writer, article FROM writers')

    persons = [{
            'qid' : i['writer_qid'],
           'name' : i['writer'],
        'article' : i['article']
    } for i in writers]

    db.insert('persons', persons)


def places():  # sourcery skip: hoist-statement-from-loop
    """
    Import info about places from rawsrc and put it into the database.

    """

    # Read from table
    births = dblist.select('SELECT DISTINCT birthplace_qid, birthplace, geo_lat, geo_lon FROM writers')
    places = [{
         'qid' : i['birthplace_qid'],
        'name' : i['birthplace'],
         'lat' : i['geo_lat'],
         'lon' : i['geo_lon']
    } for i in births]

    # Read from CSV
    csv_src = CSV(Path('.')/'rawsrc'/'writers_deaths.csv')

    while not csv_src.closed:
        place = csv_src.nextrow
        if place:
            d_geo = place['dgeo'][6:-1].split()
            places.append({
                 'qid' : csv_src.chew_qid(place['dplace']),
                'name' : place['dplaceLabel'],
                 'lat' : float(d_geo[0]),
                 'lon' : float(d_geo[1])
            })

            if place['burial']: # Burial may not be presented
                br_geo = place['burialgeo'][6:-1].split() if place['burialgeo'] else (None, None)
                places.append({
                     'qid' : csv_src.chew_qid(place['burial']),
                    'name' : place['burialLabel'],
                     'lat' : float(br_geo[0]) if br_geo[0] is not None else None,
                     'lon' : float(br_geo[1]) if br_geo[1] is not None else None
                })

    csv_src.close()
    db.insert('places', places, unique=True)


def ethnicities():
    """
    Import info about ethnicity from rawsrc and put it into the database.

    """

    selection = dblist.select('SELECT DISTINCT ethnicity_qid, ethnicity FROM writers')

    ethnicities = [{
              'qid' : ethnicity['ethnicity_qid'],
            'title' : ethnicity['ethnicity']
    } for ethnicity in selection]

    db.insert('ethnicity', ethnicities)


def bd_dates(table: str, header: str):
    """
    Import info about dates of death or birth from rawsrc and put it into the database.
      table:  str     where to insert
      header: str     which column for reading

    """

    selection = dblist.select(f'SELECT DISTINCT {header} FROM writers WHERE {header} <> ""')
    dates = [{
            'datestring' : date[header]
    } for date in selection]
    db.insert(table, dates)


def death_info():
    """
    Import info about manners and causes of death from rawsrc and put it into the database.

    """
    # dmanner, dmannerLabel : dmanners
    # dcause,  dcauseLabel  : dcauses
    # fields = qid, defining

    csv_src = CSV(Path('.')/'rawsrc'/'writers_deaths.csv')
    manners, causes = [], []

    while not csv_src.closed:
        row = csv_src.nextrow

        if row is None: break

        if row['dmanner']:
            manners.append({
                'qid' : csv_src.chew_qid(row['dmanner']),
                'defining' : row['dmannerLabel']
            })

        if row['dcause']:
            causes.append({
                'qid' : csv_src.chew_qid(row['dcause']),
                'defining' : row['dcauseLabel']
            })

    db.insert('dmanners', manners, unique=True)
    db.insert('dcauses', causes, unique=True)


def many_to_many():
    pass




if __name__ == '__main__':

    dblist = Database(Path('.')/'db'/'writers_list2.sqlite3')
    db = Database(Path('.' )/'db'/'attempt03.sqlite3')

    # db.create_tables()
    # persons()
    # places()
    # ethnicities()
    # bd_dates('births', 'birthdate')
    # bd_dates('deaths', 'deathdate')
    # death_info()
