from os.path import basename, exists
from os import rename
import csv
import logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def import_from_csv(filepaths, connection):
    """ Import the csv file using postgresql COPY
    """
    # we try with brute force,
    # waiting for a pure sql implementation of get_dependencies
    remaining = list(filepaths)
    cursor = connection.cursor()
    cursor.execute('SAVEPOINT savepoint')
    cursor.close()
    while len(remaining) > 0:
        LOG.info(u'BRUTE FORCE LOOP')
        paths = list(remaining)
        for filepath in paths:
            if not exists(filepath):
                LOG.warn(u'Missing CSV for table %s', filepath.rsplit('.', 2)[0])
                continue
            with open(filepath) as f:
                columns = ','.join(csv.reader(f).next())
                f.seek(0)
                copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                        % (basename(filepath).rsplit('.', 2)[0], columns))
                try:
                    cursor = connection.cursor()
                    cursor.copy_expert(copy, f)
                    cursor.execute('SAVEPOINT savepoint')
                    LOG.info('Succesfully imported %s' % basename(filepath))
                    remaining.remove(filepath)
                except Exception, e:
                    LOG.warn('Error importing file %s:\n%s',
                             basename(filepath), e.message)
                    cursor = connection.cursor()
                    cursor.execute('ROLLBACK TO savepoint')
                    cursor.close()
        if len(paths) == len(remaining):
            LOG.error('\n\n***\n* Could not import remaining tables : %s :-( \n***\n'
                      % ', '.join([basename(f).rsplit('.', 2)[0] for f in remaining]))
            # don't permit update for non imported files
            for update_file in [filename.replace('.target2.csv', '.update2.csv')
                                for filename in remaining]:
                rename(update_file, update_file + '.disabled')
            break
    else:
            LOG.info('\n\n***\n* Successfully imported all csv files!! :-)\n***\n')
    return remaining
