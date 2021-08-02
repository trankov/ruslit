import sqlite3
import pathlib
import re
import asyncio
import json

from datetime import datetime, timedelta

import aiohttp


# SELECT DISTINCT
# 	writers.writer,
# 	visits.writer_qid,
# 	sum(visits.visits),
# 	avg(visits.visits)
# FROM writers, visits
# WHERE writers.writer_qid = visits.writer_qid
# GROUP BY visits.writer_qid



class Dataset(object):
    """
    A helper for operating with datasets from SQLite for async requests.

    """

    def __init__(self, dbname: pathlib.Path):
        self.con = sqlite3.connect(dbname)
        self.con.row_factory = sqlite3.Row # Turn dict mode on
        self.cur = self.con.cursor()

        self.count: int = self.con.execute('SELECT Count(*) FROM writers').fetchone()[0]
        self.limit: int = 1
        self.offset: int = 0
        self.order_by: str = 'writer_qid'
        self.fields: str = '*'
        self.distinct: bool = True


    def insert_stat(self, stat: list):
        """
        Getting list of lists with dicts, then inserting them into the database.
        """

        if not stat: return None

        query = 'INSERT INTO visits (writer_qid, yearmonth, visits) VALUES (:writer_qid, :yearmonth, :visits)'
        try:
            for writer in stat:
                self.cur.executemany(query, writer)
            self.con.commit()
        except Exception as e:
            print('Inserter error: ', e)
            print('Getting "stat" as ', stat)


    def iter_chunk(self):
        """
        Returns next chunk from database according to self.limit value.
        Returns None if no next chunk due to self.count value is reached.

        """

        _distinct = {False: '', True: 'DISTINCT '}[self.distinct]
        _query = f'SELECT {_distinct}{self.fields} FROM writers ' + \
                f'ORDER BY {self.order_by} ' + \
                f'LIMIT {self.limit} OFFSET {self.offset}'
        self.offset += self.limit
        if self.offset >= self.count:
            return None
        return list(map(dict, self.cur.execute(_query).fetchall()))


    def close(self):
        self.cur.close()
        self.con.close()


    def commit(self):
        self.con.commit()





class Person(object):
    """
    Provides SQLite3 table values from CSV file with correct data types and right format.
    The tables are stored in `scheme.sql` file in the script's directory.

    """

    def __init__(self, person: dict):
        self._person = person
        self._geo = person.get('geo','')[6:-1].split()


    def _chew_qid(self, bite: str) -> int:
        """
        Returns last digits after "/Q" from urls like http://www.wikidata.org/.../Q315279
        """

        _qid = re.findall(r"^http.+Q(\d+)$", bite)
        return int(_qid[-1]) if _qid else -1


    def _parse_data(self, date_: str):
        """
        Parse wikidata date format to datetime.datetime if possible.
        Returns None if the date is not presented or 'UNKNOWN VALUE'
        in case of unknown value format.

        """

        if not date_: return None
        try:
            _date_parsed = datetime.strptime(date_, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            print ('{}, {}. Date value error: {}'.format(self.writer_qid, self.writer, date_))
            return 'UNKNOWN VALUE'
        return _date_parsed

    @property
    def writer_qid(self) -> int:
        return self._chew_qid(self._person.get('item', ''))

    @property
    def writer(self) -> str:
        return str(self._person.get('itemLabel', 'Undefined'))

    @property
    def birthdate(self):
        return self._parse_data(self._person.get('birthdate', ''))

    @property
    def deathdate(self):
        return self._parse_data(self._person.get('deathdate', ''))

    @property
    def birthplace_qid(self) -> int:
        return self._chew_qid(self._person.get('bplace', ''))

    @property
    def birthplace(self) -> str:
        return str(self._person.get('bplaceLabel', 'Undefined'))

    @property
    def geo_lat(self) -> float:
        _lat = self._geo[0] if self._geo else None
        return float(_lat) if _lat else .0

    @property
    def geo_lon(self) -> float:
        _lon = self._geo[1] if self._geo else None
        return float(_lon) if _lon else .0

    @property
    def ethnicity_qid(self) -> int:
        return self._chew_qid(self._person.get('ethnicity'))

    @property
    def ethnicity(self) -> str:
        return str(self._person.get('ethnicityLabel', 'Undefined'))

    @property
    def language_qid(self) -> int:
        return self._chew_qid(self._person.get('lang', ''))

    @property
    def language(self) -> str:
        return str(self._person.get('langLabel', ''))

    @property
    def article(self) -> str:
        return str(self._person.get('article', ''))





class Database(object):
    """
    Handles database connection for wrighting from CSV to SQLite process
    """

    def __init__(self, dbname: str):
        """
        Creates a database with a given name in the same dir with the script file.
        Tables descriptions must be in the `scheme.sql` file in the same directory.

        Parameters:
        dbname: str   Filename for the database

        """

        sqlite3.paramstyle = 'named'
        with open(pathlib.Path(__file__).parent / 'scheme.sql', 'r') as scheme:
            sql_query = scheme.read()

        self.con = sqlite3.connect(dbname)
        self.cur = self.con.cursor()
        [self.cur.execute(query) for query in sql_query.split('\n;\n')] # create tables if necessary


    def cursor(self) -> sqlite3.Cursor:
        return self.cur


    def insert_writer(self, table_row: dict):
        _sql_request = '''INSERT INTO writers ({}) VALUES ({})'''.format(
                ', '.join(table_row),
                ', '.join(f':{i}' for i in table_row)
            )
        self.cur.execute(_sql_request, table_row)


    def commit(self):
        self.con.commit()


    def close(self):
        self.cur.close()
        self.con.close()





class Statistics(object):
    """
    Provides access to wikipedia article statistics.
    Requires a dict object with k/v pairs of a current Dataset row item.

    """

    def __init__(self,
                 dataset: dict,
                 startdate: str = datetime.strftime(datetime.now() - timedelta(days=730), '%Y%m01'),
                 enddate: str = datetime.strftime(datetime.now(), '%Y%m01')
                ):

        self._template = \
                'https://wikimedia.org/api/rest_v1/metrics' + \
                '/pageviews/per-article/ru.wikipedia.org' + \
                '/all-access/user/{}/monthly/' + \
               f'{startdate}/{enddate}'  # should be like '20210601/20210701'

        self._article = dataset['article'][30:]
        self._wqid = dataset['writer_qid']


    async def async_wikistat(self):

        url = self._template.format(self._article)
        connector = aiohttp.TCPConnector(limit_per_host = 90)
        timeout = aiohttp.ClientTimeout(total=180, connect=10, sock_connect=10, sock_read=10)
        headers = {'Api-User-Agent' : 'Scientific literature project (trankov@gmail.com)'}

        async with aiohttp.ClientSession(connector=connector, headers=headers,
                                         trust_env=True, timeout=timeout) as session:
            try:
                response = await session.get(url)
                result = await response.text(encoding='utf-8')
            except Exception as e:
                print ('\nError in request: ', e, '\n', url, sep='')
                return None

        try:
            jsn_result = json.loads(result)
        except Exception as e:
            print ('\nJSON parse error: ', e)
            print (result)
            return None

        if 'items' not in jsn_result:
            print ('Returned unexpected response: ', jsn_result)
            return None

        try:
            return [
                {
                    'writer_qid' : self._wqid,
                    'yearmonth' : f"{int(x['timestamp'][:6])*.01:.2f}", # 20210801 -> 2021.08
                    'visits' : x['views']
                } for x in jsn_result['items']
            ]
        except Exception as e:
            print ('\nError during parsing response: ', e, '\nURL: ', url)
            return None
