from os.path import join
import psycopg2


def export_tables(source_tables, db=None, user='admin', pwd='admin', dest_dir=None):
    """ Export the list of tables using postgresql COPY
    """
    connection = psycopg2.connect("dbname=%s" % db)
    cursor = connection.cursor()
    for table in source_tables:
        with open(join(dest_dir, table + '.csv'), 'w') as f:
            cursor.copy_to(f, table)
