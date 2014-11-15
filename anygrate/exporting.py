from os.path import join
import psycopg2.extras
import logging
from os.path import basename
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))
from multiprocessing import Pool

import multiprocessing as mp


class Consumer(mp.Process):

    def __init__(self, task_queue, result_queue, database):
        mp.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.pyConn = psycopg2.connect(database=database)
        self.pyConn.set_isolation_level(0)


    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                print 'Tasks Complete'
                self.task_queue.task_done()
                break
            answer = next_task(connection=self.pyConn)
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return

class ExportCSV(object):
    def __init__(self, table):
        self.table = table

    def __call__(self, connection=None):
        pyConn = connection
        pyCursor1 = pyConn.cursor()

        procQuery = 'UPDATE city SET gid_fkey = gid FROM country  WHERE ST_within((SELECT the_geom FROM city WHERE city_id = %s), country.the_geom) AND city_id = %s' % (self.a, self.a)

        pyCursor1.execute(procQuery)
        print 'What is self?'
        print self.a

        return self.a

    def __str__(self):
        return 'ARC'
    def run(self):
        print 'IN'

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
    {'table': [{'value', 12}, ...], ...}
    It means you can get result['table'][0]['column']
    This function is used to get the list of data to update in the target db
    """
    result = {}
    for table in tables:
        result[table] = []
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            if table not in discriminators:
                continue
            columns = discriminators[table]
            id_column = ['id'] if table not in m2m_tables else []
            cursor.execute('select %s from %s' % (', '.join(columns + id_column), table))
            result[table] = cursor.fetchall()
    return result
