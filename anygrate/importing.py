from os.path import basename, exists
from os import rename
import csv
from multiprocessing import Pool
from functools import partial

from .sql_commands import make_savepoint, get_db_connection

import logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def reorder_filepaths(filepaths):
    return filepaths[:]


def __run_slow_import(filepaths, connection, suffix=''):
    remaining = reorder_filepaths(filepaths)
    count = 1
    while remaining:
        LOG.info(u'%s\nBRUTE FORCE LOOP: LOOP #%d' % ('-+'*40, count))
        paths = remaining[:]
        for path in paths:
            table = basename(path).rsplit('.', 2)[0] + suffix
            with open(path) as f, connection.cursor() as c:
                try:
                    columns = ','.join(['"%s"' % col for col in csv.reader(f).next()])
                    f.seek(0)
                    copy = ("""COPY "%s" (%s) FROM STDOUT WITH CSV HEADER NULL ''"""
                            % (table, columns))
                    c.copy_expert(copy, f)
                    make_savepoint(c)
                    remaining.remove(path)
                    LOG.info("SUCCESS importing %s: " % table)
                except Exception, e:
                    LOG.error("ERROR importing %s: %s" % (table, e.message))

                    cursor = connection.cursor()
                    cursor.execute('ROLLBACK TO savepoint')
                    cursor.close()
        if len(paths) == len(remaining):
            LOG.error(u'\n\n***\n* Could not import remaining tables : %s :-( \n'
                      u'Hint Check Last Brute Force Loop\n***\n'
                      % ', '.join([basename(f).rsplit('.', 2)[0] + suffix for f in remaining]))
            # don't permit update for non imported files
            # note in current code sys.exit(1) is called anyway
            for update_file in [filename.replace('.target2.csv', '.update2.csv')
                                for filename in remaining]:
                rename(update_file, update_file + '.disabled')
            break
    return remaining


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


def import_from_csv(filepaths, connection, drop_fk=False, suffix=''):
    """ Import the csv file using postgresql COPY
    """
    assert all([exists(p) for p in filepaths])
    with connection.cursor() as c:
        make_savepoint(c)
    if drop_fk:
        LOG.info(u'No Foreign Key constraints so straight import :)')
        p = Pool(8)  # arbitrary convert to variable
        try:
            p.map(partial(__run_fast_import, dsn=connection.dsn, suffix=suffix), sorted(filepaths))
            return []
        except Exception, e:
            msg = e.message
            LOG.error('Fast Import Error: %s', msg)
            cursor = connection.cursor()
            cursor.execute('ROLLBACK TO savepoint')
            cursor.close()
        return filepaths
    else:
        return __run_slow_import(filepaths, connection, suffix=suffix)

