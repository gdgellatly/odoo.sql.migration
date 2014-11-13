from os.path import basename, exists
from os import rename
import csv

from .sql_commands import make_savepoint

import logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))

FK_VIOLATION = 'violates foreign key constraint'
MISSING_COLUMN = 'of relation "%s" does not exist'


def csv_file_importer(filepath, connection, table=False):
    if not exists(filepath):
        LOG.warn(u'Missing CSV for table %s', filepath.rsplit('.', 2)[0])
        return -1, ""

    table = table or basename(filepath).rsplit('.', 2)[0]

    with open(filepath) as f:
        columns = ','.join(['"%s"' % c for c in csv.reader(f).next()])
        f.seek(0)
        copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                % (table, columns))
        try:
            cursor = connection.cursor()
            cursor.copy_expert(copy, f)
            make_savepoint(cursor)

            res = (0,  basename(filepath))
        except Exception, e:
            res = generate_csv_import_error_code(e.message, table, basename(filepath))
            cursor = connection.cursor()
            cursor.execute('ROLLBACK TO savepoint')
            cursor.close()
        return res


def import_from_csv(filepaths, connection, drop_fk=False):
    """ Import the csv file using postgresql COPY
    """
    remaining = list(filepaths)
    cursor = connection.cursor()
    make_savepoint(cursor)
    cursor.close()
    tbl_file_map = {k: basename(k).rsplit('.', 2)[0] for k in remaining}
    missing_columns = []
    while len(remaining) > 0:
        paths = remaining[:]
        if drop_fk:
            LOG.info(u'No Foreign Key constraints so straight import :)')
        else:
            # we try with brute force,
            # waiting for a pure sql implementation of get_dependencies
            LOG.info(u'BRUTE FORCE LOOP')
        for filepath in paths:
            tbl = tbl_file_map.get(filepath)
            ret_code = csv_file_importer(filepath, connection, table=tbl)
            if ret_code:
                if ret_code[0] == 0:
                    LOG.info(u'Succesfully imported %s' % ret_code[1])
                    # try:
                    #     del known_fk_deps[tbl]
                    # except KeyError:
                    #     LOG.error(u'The key %s was not found in the table map'.format(tbl))
                    remaining.remove(filepath)

                elif ret_code[0] == 1:
                    pass  # known_fk_deps[tbl] = error_code[1]
                elif ret_code[0] == 2:
                    missing_columns.append(ret_code[1])
                else:
                    LOG.critical(u'An unknown error occurred importing csv file')

        if remaining and len(paths) == len(remaining):
            LOG.error('\n\n***\n* Could not import remaining tables : %s :-( \n***\n'
                      % ', '.join([tbl_file_map.get(f, '') for f in remaining]))
            if missing_columns:
                missing_columns = set(missing_columns)
                LOG.error('\nThe following columns were missing:\n{0}\n{1}\n{0}\n'
                          ' Fix these first as they cause dependent tables to fail. '
                          'You should fully review the table schemas, work out the applicable\n'
                          'module and adjust your mapping or installation as appropriate '
                          'as only the first missing column is shown. To forget them they are\n'
                          'displayed in the YAML syntax needed'.format('-'*79, '\n'.join(missing_columns)))

            # don't permit update for non imported files
            for update_file in [filename.replace('.target2.csv', '.update2.csv')
                                for filename in remaining]:
                rename(update_file, update_file + '.disabled')
            break
    else:
        if len(filepaths) > 1:
            LOG.info('\n\n***\n* Successfully imported all csv files!! :-)\n***\n')
    return remaining


def generate_csv_import_error_code(msg, table_name, filepath):
    LOG.warn('Error importing file %s:\n%s',
             basename(filepath), msg)
    if FK_VIOLATION in msg:
        last = msg.rfind('"')
        first = msg.rfind('"', 0, last) + 1
        res = (1, msg[first:last])
    elif MISSING_COLUMN % table_name in msg:
        first = msg.find('"') + 1
        last = msg.find('"', first)
        col = msg[first:last]
        LOG.warn('Missing columns cause dependent tables to fail'
                 ' making you think you have cyclic dependencies when you don\'t')
        res = (2, '{0}.{1}: __forget__'.format(table_name, col))
    else:
        res = (99, "WTF")
    return res
