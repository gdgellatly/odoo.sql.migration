from os.path import join
import psycopg2.extras
import logging
from os.path import basename
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def export_to_csv(source_tables, dest_dir, connection):
    """ Export data using postgresql COPY
    """
    csv_filenames = []
    for table in source_tables:
        filename = join(dest_dir, table + '.csv')
        with connection.cursor() as cursor, open(filename, 'w') as f:
            cursor.copy_expert("COPY %s TO STDOUT WITH CSV HEADER" % table, f)
            csv_filenames.append(filename)
    return csv_filenames


def extract_existing(source_tables, discriminators, connection):
    """ Extract data from the target db,
    focusing only on discriminator columns.
    Extracted data is a dict whose values are lists of named tuples:
    {'table': [['value', 12}, ...], ...}
    It means you can get result['table'][0]['column']
    This function is used to get the list of data to update in the target db
    """
    result = {}
    key = 'id'
    for table in source_tables:
        result[table] = []
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            if table not in discriminators:
                LOG.warn(u'No discriminator defined for table %s', table)
                continue
            columns = discriminators[table] + [key]
            cursor.execute('select %s from %s order by %s' % (', '.join(columns), table, key))
            result[table] = cursor.fetchall()
    return result
