# Тут мы просто асинхронно качаем данные.
# Всё достаточно просто, но есть нюанс: читаем из БД,
# поэтому коннекторы, если что, надо будет переуказать.


import asyncio
import time
import random

import aiohttp
import psycopg2 # Библиотека для работы с PostgreSQL
import orjson   # Сторонний JSON-движок, раз уж он тут и так установлен,
                # но в данном скрипте можно и родной (ответы по статистике
                # приходят в json)



# Это хардкодинг, т.к. по ТЗ не предполагалось дальнейшей смены диапазонов.
# Если в REST-запросе будете добавлять данные, допишите колонки здесь.
# Ну или потом можно функцию дописать. Если будет на то смысл заморачиваться.

MONTHS = '"2015.07","2015.08","2015.09","2015.10","2015.11","2015.12","2016.01","2016.02","2016.03","2016.04","2016.05","2016.06","2016.07","2016.08","2016.09","2016.10","2016.11","2016.12","2017.01","2017.02","2017.03","2017.04","2017.05","2017.06","2017.07","2017.08","2017.09","2017.10","2017.11","2017.12","2018.01","2018.02","2018.03","2018.04","2018.05","2018.06","2018.07","2018.08","2018.09","2018.10","2018.11","2018.12","2019.01","2019.02","2019.03","2019.04","2019.05","2019.06","2019.07","2019.08","2019.09","2019.10","2019.11","2019.12","2020.01","2020.02","2020.03","2020.04","2020.05","2020.06","2020.07","2020.08","2020.09","2020.10","2020.11","2020.12","2021.01","2021.02","2021.03","2021.04","2021.05","2021.06","2021.07"'


async def fetch(url, session, qid):
    '''
    Принимаем URL, асинхронный объект сессии и wd-значение,
    возвращаем его назад + то, что прислал нам сервер в ответ.
    Обычный асинхронный таск без наворотов.
    '''

    async with session.get(url) as response:
        result = await response.text(encoding='utf-8')

        try:
            result_json = orjson.loads(result)
        except Exception as e:
            result_json = {'error': e}

        return (qid, result_json)



async def run(queries):
    '''
    Основной блок, который собирает запросы в асинхронный пучок
    и запускает очередь задач. Внимание, много ручных параметров.
    Читайте внимательно комментарии.
    '''

    # Это главный URL-запрос к Wikimedia Stat API.
    #
    # 1. Параметр /en.wikipedia.org/ (6-й по счёту) ищет запросы в en-википедии,
    #    если нужна русская, должно стоять /ru.wikipedia.org/
    #
    # 2. Параметр /monthly/ (10-й) означает, что мы запрашиваем данные по месяцам.
    #    Если хотим по дням, то пишем /daily/
    #
    # 3. Два последних параметра это дата начала и дата конца временного промежутка
    #    в формате ГГГГММДД. Если меняете их, то соответственно отредактируйте
    #    константу MONTHS в строке 24 данного файла.
    #

    url = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{}/monthly/20150701/20210701"

    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.

    global limit
    connector = aiohttp.TCPConnector(limit_per_host = limit if limit <= 190 else 190)

    # Тут мы объявляем сессию с заголовком Api-User-Agent.
    # Пожалуйста, отредактируйте это значение в связи со своими
    # нуждами. Они просят тип проекта и адрес для обратной связи.
    # Значение в строке 82: впишите туда ваши данные между 2-х апострофов.

    async with aiohttp.ClientSession(connector=connector,
                                     headers={'Api-User-Agent' :
                                              'Scientific project (trankov@gmail.com)'},
                                     ) as session:
        for item in queries:
            task = asyncio.ensure_future(fetch(url.format(item[1]), session, item[0]))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # you now have all response bodies in this variable

        write_csv(responses)


def write_csv(responses):
    '''
    Эта функция разбирает ответ сервера на составляющие,
    и превращает древовидную запись в табличную. Файл с выходом
    статистики указывается вручную, внимательно читайте
    комментарии в коде!
    '''

    global global_counter
    global global_time
    global offset

    global output

    with open(output, 'a') as f:

        for element in responses:
            if 'items' not in element[1]: continue

            articles = element[1]['items']

            head = '","'.join([str(element[0]), articles[0]['article']])
            tail ='","'.join(str(article['views']) for article in articles)
            body = f'"{head}","{tail}"'

            f.write(body + '\n')
            global_counter += 1
            print('\r',
                f'Прошло {time.strftime("%H:%M:%S", time.gmtime(time.time()-global_time))}, ',
                f'записано строк {global_counter}, offset {offset}', end='', sep='')



if __name__ == "__main__":
    print('-'*80)

    global_counter = 0
    global_time = time.time()

    # С какой строки в базе данных начать. Полезно, если сеть не выдержала,
    # а мы не хотим стартовать с самого начала. Для этого мы выводим на экран
    # информацию о каждой записанной строке, её можно затем скопировать сюда
    offset = 0

    # Количество записей за 1 запрос. Есть в
    limit = 190

    # Захардкодил, чтобы можно было устанавливать offset как начальную строку,
    # total как конечную, и limit как количество строк за один проход. Технически
    # это значение легко можно получить автоматически из базы. Но если разносить
    # по разным машинам, то так руками поправить вполне удобно.
    total = 9114545

    # Тут законнектите к вашей базе. По идее все такие
    # модули используют один синтаксис, а sql там не сложный.

    conn = psycopg2.connect("dbname=wikidata user=user password=password port=5432")
    cur = conn.cursor()

    # Пропишите свой путь к файлу с результатом.
    # output = '...'
    # Сейчас он в домашнем каталоге ищет /wikidata/en-visits.csv,
    # если такого каталога нет, он упадёт с File Not Found Error.
    output = os.path.expanduser ('~/wikidata/en-visits.csv')

    # Если файла нет, то создадим, или если он есть, но пустой, - запишем туда заголовок CSV-таблицы.
    if not os.path.exists(output) or (os.path.exists(output) and not os.path.getsize(output)):
        with open(output, 'w') as f:
            f.write('"qid","article",' + MONTHS + '\n')


    complete = False
    while not complete:

        complete = (offset + limit) >= total

        cur.execute("SELECT qid, label FROM qindex LIMIT %s OFFSET %s;", (limit, offset))
        labels = cur.fetchall()

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run([i for i in labels]))
        loop.run_until_complete(future)

        offset += limit

    cur.close()
    conn.close()

    print()
    print('-'*80)
