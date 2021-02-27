import logging as log
import time
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s %(threadName)s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M:%S',
                handlers=[
                    log.FileHandler("debug.log"),
                    log.StreamHandler()
                ])
from environs import Env
from jsonlines import jsonlines
import bz2
import pandas as pd
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert


# adds the word IGNORE after INSERT in sqlalchemy to avoid duplicate errors during initial db insert
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kw)


# Read ENVs
env = Env()
env.read_env('db.env')


def parseFile(filepath):
    file = bz2.open(filepath, mode='rt')
    return jsonlines.Reader(file)


def execute(file, constraints, CHUNK_SIZE=100000):
    """
    This function opens a connection to the database,
    executes and commits a supplied query in chunks.
    """
    try:
        log.info('Started import')
        t = time.time()
        lines = []
        for line in file:
            lines.append(line)

            if len(lines) == CHUNK_SIZE:
                submit(lines, constraints)
                lines = []

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Submit remaining chunk
        submit(lines, constraints)
        file.close()
        log.info('Finished %f', time.time()-t)


def submit(lines, constraints):
    uri = f"mysql+pymysql://dbtheory:dbtheory@localhost/reddit"
    df = pd.DataFrame.from_records(lines)
    posts = df.filter(
        items=['id', 'parent_id', 'link_id', 'name', 'author', 'body', 'subreddit_id', 'score',
               'created_utc'])
    posts = posts.rename(columns={"subreddit_id": "fk_subreddit_id"})
    subreddit = df.filter(items=['subreddit_id', 'subreddit'])

    if constraints:
        subreddit.drop_duplicates(subset=['subreddit_id'], keep='first', inplace=True)
        subreddit.to_sql('subreddit_constraints', con=uri, if_exists='append', index=False)
        posts.to_sql('posts_constraints', con=uri, if_exists='append', index=False)
    else:
        subreddit.to_sql('subreddit_no_constraints', con=uri, if_exists='append', index=False)
        posts.to_sql('posts_no_constraints', con=uri, if_exists='append', index=False)


if __name__ == '__main__':
    file = parseFile('medium.bz2')
    execute(file, True)
