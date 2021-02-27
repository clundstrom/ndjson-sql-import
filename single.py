import bz2
import jsonlines as jsonlines
import pymysql as db
from environs import Env
import os
import time
import logging as log

# Read ENVs
env = Env()
env.read_env('db.env')
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s %(name)-8s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M:%S')


def timeit(func):
    def wrapper(*arg, **kw):
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__

    return wrapper


def parseFile(filepath):
    file = bz2.open(filepath, mode='rt', encoding='utf-8')
    reader = jsonlines.Reader(file)
    return reader


@timeit
def execute(reader, table):
    """
    This function opens a connection to the database,
    executes and commits a supplied query.
    :param query: string SQL syntax
    :param args: parameters supplied with query. Parameterized queries prevents injection attacks.
    :return: Results of the query.
    """
    conn = db.connect(user=os.environ.get('MYSQL_USER'),
                      password=os.environ.get('MYSQL_PASSWORD'),
                      host=os.environ.get('MYSQL_HOST'),
                      port=int(os.environ.get('MYSQL_PORT')),
                      database=os.environ.get('MYSQL_DATABASE'))
    try:
        cur = conn.cursor()

        query1 = compileQuery('subreddit_no_constraints', 'INSERT_SUBREDDIT')
        query2 = compileQuery('posts_no_constraints', 'INSERT_POST')
        for entry in reader:
            cur.execute(query1, entry)
            cur.execute(query2, entry)
            conn.commit()

    except db.Error as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        reader.close()


def compileQuery(table, type):
    if type == 'INSERT_POST':
        query = f"""INSERT IGNORE INTO `{table}` 
                (id, parent_id, link_id, name, author, body, fk_subreddit_id, score, created_utc)
                VALUES(%(id)s, %(parent_id)s, %(link_id)s, %(name)s, %(author)s, %(body)s, %(subreddit_id)s, %(score)s, %(created_utc)s)
                """
    elif type == 'INSERT_SUBREDDIT':
        query = f"""INSERT IGNORE INTO `{table}` 
                        (subreddit_id, subreddit)
                        VALUES(%(subreddit_id)s, %(subreddit)s)
                        """
    return query


if __name__ == '__main__':
    table1 = 'posts_constraints'
    table2 = 'posts_no_constraints'
    reader = parseFile('small.bz2')
    log.info('Started import')
    t = time.time()
    execute(reader, table1)
    log.info('Finished %s', time.time()-t)