import sys
from os.path import basename, exists
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))

from .importing import csv_file_importer

def get_management_connection(db='postgres'):
    mgmt_connection = psycopg2.connect("dbname=%s" % db)
    mgmt_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return mgmt_connection


def kill_db_connections(cursor, datname):
    cursor.execute("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s;""", (datname,))


def create_new_db(source_db, target_db, new_db):
    if new_db == target_db:
        print "If you want a new database, best you give it a different name"
        sys.exit(1)

    # We need to set this as transaction blocks will fail
    mgmt_connection = get_management_connection(db=source_db)
    with mgmt_connection.cursor() as m:

        # CREATE won't accept parameterised inputs but need to sanitize psycopg2 gives no easy way
        # TODO: Actually find a better way of doing this
        if not all([n.isalnum() or n == '_' for n in new_db + target_db]):
            print
            sys.exit(1)
        kill_db_connections(m, new_db)
        kill_db_connections(m, target_db)
        drop_command = "DROP DATABASE IF EXISTS {0};".format(new_db)
        m.execute(drop_command)
    mgmt_connection.commit()
    with mgmt_connection.cursor() as m:
        print(u'Creating New Database')
        create_command = "CREATE DATABASE {0} TEMPLATE {1};".format(new_db, target_db)
        m.execute(create_command)
        print(u'New Database Created')
        target_db = new_db
    mgmt_connection.commit()
    mgmt_connection.close()
    return target_db


def drop_constraints(db=None, tables=False):
    if not db:
        print "Cannot drop constraints without knowing the database"
        return False
    mgmt_connection = get_management_connection(db=db)
    with mgmt_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as m:
        if tables is not False:
            where_clause = m.mogrify(" AND relname IN %s ", (tuple(tables),))
        else:
            where_clause = ""
        m.execute("""
SELECT
  relname,
  'ALTER TABLE "'||nspname||'"."'||relname||'" DROP CONSTRAINT "'||conname||'";' drop_command,
  'ALTER TABLE "'||nspname||'"."'||relname||'" ADD CONSTRAINT "'||conname||'" '||
     pg_get_constraintdef(pg_constraint.oid)||';' add_command
 FROM pg_constraint
 INNER JOIN pg_class ON conrelid=pg_class.oid
 INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
 WHERE contype='f'""" + where_clause + """
 ORDER BY contype,nspname,relname,conname""", (tuple(tables),))
        add = []
        drop = []
        for constraint in m.fetchall():
            add.append(constraint['add_command'])
            drop.append(constraint['drop_command'])
        add_command = '\n'.join(add)
        m.execute('\n'.join(drop))
    mgmt_connection.commit()
    return add_command


def setup_temp_table(cursor, temp_table, orig_table):
    if validate_identifiers(temp_table + orig_table):
        create_command = "CREATE TEMP TABLE {0} AS SELECT * FROM {1} LIMIT 0".format(temp_table, orig_table)
        cursor.execute(create_command)
        # We need to find the primary key using accepted postgres method
        cursor.execute('''
SELECT
  pg_attribute.attname,
  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
FROM pg_index, pg_class, pg_attribute, pg_namespace
WHERE
  pg_class.oid = %s::regclass AND
  indrelid = pg_class.oid AND
  nspname = 'public' AND
  pg_class.relnamespace = pg_namespace.oid AND
  pg_attribute.attrelid = pg_class.oid AND
  pg_attribute.attnum = any(pg_index.indkey)
 AND indisprimary;''', (orig_table,))
        pkey = cursor.fetchone()
        if pkey:
            pkey = pkey[0]
            idx_command = "CREATE INDEX {0}_id_idx ON {0}({1});".format(temp_table, pkey[0])
            cursor.execute(idx_command)
        else:
            pkey = None
        return pkey
    else:
        return None


def validate_identifiers(name, log_error=True):
    """
    This should only be used for non parameterizable
    SQL identifier input and is very restrictive
    :param name: proposed SQL identifier
    :param log_error:
    :return: True or False
    """
    if not name.isalnum():
        if not all([l.isalnum() or l == '_' for l in name]):
            log_error and LOG.error(
                "Only alphanumeric characters and _ allowed in SQL Identifiers (%s)" % name)
            return False
    return True


def make_savepoint(c, name='savepoint'):
    if validate_identifiers(name):
        try:
            c.execute('RELEASE SAVEPOINT savepoint')
        except Exception, e:
            LOG.debug("%s", e.message)
        finally:
            c.execute('SAVEPOINT savepoint')


def upsert(filepath, connection, temp_table, orig_table, columns, pkey):
    if all([validate_identifiers(x) for x in [temp_table, orig_table]]):
        csv_file_importer(filepath, connection, table_name=temp_table)
        update_command = "UPDATE {1} SET {2} FROM {0} WHERE {1}.{3}={0}.{3}".format(
            temp_table, orig_table, columns, pkey)
        with connection.cursor() as c:
            c.execute(update_command)
            make_savepoint(c)
    return