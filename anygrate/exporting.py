from os.path import join
import psycopg2.extras
import logging
from os.path import basename
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def export_to_csv(tables, dest_dir, connection):
    """ Export data using postgresql COPY
    """
    csv_filenames = []
    for table in tables:
        filename = join(dest_dir, table + '.csv')
        with connection.cursor() as cursor, open(filename, 'w') as f:
            cursor.copy_expert("COPY %s TO STDOUT WITH CSV HEADER NULL ''" % table, f)
            csv_filenames.append(filename)
    return csv_filenames


def extract_existing(tables, m2m_tables, discriminators, connection):
    """ Extract data from the target db,
    focusing only on discriminator columns.
    Extracted data is a dict whose values are lists of named tuples:
    {'table': [['value', 12}, ...], ...}
    It means you can get result['table'][0]['column']
    This function is used to get the list of data to update in the target db
    """
    result = {}
    for table in tables:
        result[table] = []
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            if table not in discriminators:
                LOG.info(u'No discriminator defined for table %s', table)
                continue
            columns = discriminators[table]
            id_column = ['id'] if table not in m2m_tables else []
            cursor.execute('select %s from %s' % (', '.join(columns + id_column), table))
            result[table] = cursor.fetchall()
    return result
