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
    remaining = len(filepaths)
    while remaining:
        LOG.info(u'BRUTE FORCE LOOP')
        for filepath in filepaths:
            if not exists(filepath):
                LOG.warn(u'Missing CSV for table %s', filepath.rsplit('.', 2)[0])
                continue
            with connection.cursor() as cursor, open(filepath) as f:
                columns = ','.join(csv.reader(f).next())
                f.seek(0)
                copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                        % (basename(filepath).rsplit('.', 2)[0], columns))
                try:
                    cursor.copy_expert(copy, f)
                except Exception, e:
                    LOG.warn('Error importing file %s:\n%s',
                             basename(filepath), e.message)
                    connection.rollback()
                else:
                    LOG.info('Succesfully imported %s' % basename(filepath))
                    filepaths.remove(filepath)
        if len(filepaths) != remaining:
            remaining = len(filepaths)
        else:
            LOG.error('Could not import remaining files : %s :-('
                      % ', '.join([basename(f) for f in filepaths]))
            # don't permit update for non imported files
            for update_file in [filename.replace('.target2.csv', '.update2.csv')
                                for filename in filepaths]:
                rename(update_file, update_file + '.disabled')
            break
