from os.path import basename, exists
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
        for filepath in filepaths:
            if not exists(filepath):
                LOG.warn(u'Missing CSV for table %s', filepath.rsplit('.', 2)[0])
                continue
            with connection.cursor() as cursor, open(filepath) as f:
                columns = ','.join(csv.reader(f).next())
                f.seek(0)
                copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER"
                        % (basename(filepath).rsplit('.', 2)[0], columns))
                try:
                    cursor.copy_expert(copy, f)
                except Exception, e:
                    LOG.warn('Error importing file %s:\n%s',
                             basename(filepath), e.message)
                    connection.rollback()
                else:
                    LOG.info('Succesfully imported %s' % basename(filepath))
                    remaining.remove(filepath)
        if len(filepaths) == remaining:
            LOG.error('Could not import remaining files : %s :-('
                      % ', '.join([basename(f) for f in filepaths]))
            break
