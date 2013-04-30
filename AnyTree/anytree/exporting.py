from os.path import join
import psycopg2


def export_tables(source_tables, dest_dir, db=None, user='admin', pwd='admin'):
    """ Export the list of tables using postgresql COPY
    """
    connection = psycopg2.connect("dbname=%s" % db)
    for table in source_tables:
        with connection.cursor() as cursor, open(join(dest_dir, table + '.csv'), 'w') as f:
            cursor.copy_expert("COPY %s TO STDOUT WITH CSV HEADER" % table, f)
