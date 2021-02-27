#!/usr/bin/env python3
import logging as log

log.basicConfig(level=log.DEBUG,
                format='%(asctime)s %(threadName)s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M:%S')
import bz2
import os.path
from threading import Thread
import pymysql as db
import jsonlines
import time
n_threads = 6*2
filename = 'small.bz2'
table1 = 'posts_constraints'
table2 = 'posts_no_constraints'


def worker(start, end):
    conn = db.connect(user=os.environ.get('MYSQL_USER'),
                      password=os.environ.get('MYSQL_PASSWORD'),
                      host=os.environ.get('MYSQL_HOST'),
                      port=int(os.environ.get('MYSQL_PORT')),
                      database=os.environ.get('MYSQL_DATABASE'))

    f = bz2.open(filename, mode='rb')
    f.seek(start)
    reader = jsonlines.Reader(f)
    try:
        total_len = 0

        query1 = compileQuery('subreddit_constraints', 'INSERT_SUBREDDIT')
        query2 = compileQuery('posts_constraints', 'INSERT_POST')

        cur = conn.cursor()
        conn.autocommit(False)
        counter = 0
        for line in reader:
            total_len += len(line)
            if (total_len + start) >= end:
                log.debug('Worker stopped at %d', total_len + start)
                break

            counter += 1
            cur.execute(query1, line)
            cur.execute(query2, line)
            if counter == 500:
                conn.commit()
                counter = 0



    except db.Error as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        reader.close()


def find_last_newline(reader, pos):
    """
    Find the first newline occurring newline before the end.
    :param reader:
    :param pos:
    :return:
    """
    reader.seek(pos)
    c = reader.read(1)
    while c != b'\n' and pos > 0:
        pos -= 1
        reader.seek(pos)
        c = reader.read(1)
    return (pos)


def init():
    fsize = os.path.getsize(filename)
    log.info('Filesize %d', fsize)
    initial_chunks = list(range(0, fsize, int(fsize / n_threads)))[:-1]
    log.info('Chunks to process %d', len(initial_chunks))
    file = bz2.open(filename, mode='rb')
    log.info('Finding newline positions..')
    pieces = sorted(set([find_last_newline(file, n) for n in initial_chunks]))
    pieces.append(fsize)
    args = zip([x + 1 if x > 0 else x for x in pieces], [x for x in pieces[1:]])
    return (args)


if __name__ == '__main__':
    log.info('Initializing')
    args = init()

    threads = [Thread(target=worker, args=(start, end)) for start, end in list(args)]

    log.info('Starting threads.')
    time = time.time()
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    log.info(f'Threads finished. Total time: {time.time()-time}')
