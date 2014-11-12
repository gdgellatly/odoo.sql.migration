import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_management_connection(db='postgres'):
    mgmt_connection = psycopg2.connect("dbname=%s" % db)
    mgmt_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return mgmt_connection


def create_new_db(source_db, target_db, new_db):
    if new_db == target_db:
        print "If you want a new database, best you give it a different name"
        sys.exit(1)

    # We need to set this as transaction blocks will fail
    mgmt_connection = get_management_connection(db=source_db)
    with mgmt_connection.cursor() as m:
        # Not sure CREATE accepts parameterised inputs but need to sanitize
        # Commented for now while we try
        # new_db = m.mogrify("%s", new_db)
        # target_db = m.mogrify("%s", new_db)
        # create_command = "CREATE DATABASE {0} TEMPLATE {1};".format(new_db, target_db)
        # m.execute(create_command)

        m.execute('DROP DATABASE %s IF EXISTS;', (new_db,))
        m.execute('CREATE DATABASE %s TEMPLATE %s;', (new_db, new_db, target_db))
        target_db = new_db
    mgmt_connection.close()
    return target_db


def drop_constraints(db=None, tables=False):
    if not db:
        print "Cannot drop constraints without knowing the database"
        return False
    mgmt_connection = get_management_connection(db=db)
    with mgmt_connection.cursor(cursor_factory=psycopg2.extra.DictCursor) as m:
        if tables is not False:
            where_clause = m.mogrify(" AND relname IN (%s) ", (tuple(tables),))
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
    return add_command
