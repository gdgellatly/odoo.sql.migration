from os.path import join
import psycopg2


def export_tables(source_tables, dest_dir, db=None, user='admin', pwd='admin'):
    """ Export the list of tables using postgresql COPY
    """
    csv_filenames = []
    connection = psycopg2.connect("dbname=%s" % db)
    for table in source_tables:
        filename = join(dest_dir, table + '.csv')
        with connection.cursor() as cursor, open(filename, 'w') as f:
            cursor.copy_expert("COPY %s TO STDOUT WITH CSV HEADER" % table, f)
            csv_filenames.append(filename)
    return csv_filenames
