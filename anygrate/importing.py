from os.path import basename, exists, getsize
from os import rename
import csv
from multiprocessing import Pool
from functools import partial

from .sql_commands import make_savepoint, get_db_connection

import logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def __run_fast_import(filepath, dsn=None, suffix=""):
    table = basename(filepath).rsplit('.', 2)[0] + suffix
    with get_db_connection(dsn=dsn) as connection:
        with open(filepath) as f, connection.cursor() as c:
            columns = ','.join(['"%s"' % col for col in csv.reader(f).next()])
            f.seek(0)
            copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                    % (table, columns))
            c.copy_expert(copy, f)
        LOG.info(u"SUCCESS importing %s" % table)


def update_from_csv(filepaths, connection, suffix=''):
    assert all([exists(p) for p in filepaths])
    with connection.cursor() as c:
        make_savepoint(c)
    for filepath in filepaths:
        table = basename(filepath).rsplit('.', 2)[0] + suffix
        try:
            with open(filepath) as f, connection.cursor() as c:
                columns = ','.join(['"%s"' % col for col in csv.reader(f).next()])
                f.seek(0)
                copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                        % (table, columns))
                c.copy_expert(copy, f)
            LOG.info(u"SUCCESS updating %s" % table)
        except Exception, e:
            msg = e.message
            LOG.error('Fast Update Error: %s', msg)
            cursor = connection.cursor()
            cursor.execute('ROLLBACK TO savepoint')
            cursor.close()
    return filepaths


def import_from_csv(filepaths, connection, drop_fk=False, suffix=''):
    """ Import the csv file using postgresql COPY
    """
    assert all([exists(p) for p in filepaths])
    with connection.cursor() as c:
        make_savepoint(c)
    LOG.info(u'No Foreign Key constraints so straight import :)')
    p = Pool(20)  # arbitrary convert to variable
    try:
        p.map(partial(__run_fast_import, dsn=connection.dsn, suffix=suffix),
              sorted(filepaths, key=getsize, reverse=True))
        return []
    except Exception, e:
        msg = e.message
        LOG.error('Fast Import Error: %s', msg)
        cursor = connection.cursor()
        cursor.execute('ROLLBACK TO savepoint')
        cursor.close()
    return filepaths

