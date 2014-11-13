from os.path import basename, exists
from os import rename
import csv
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))

from .sql_commands import make_savepoint

FK_VIOLATION = 'violates foreign key constraint'
MISSING_COLUMN = 'of relation "%s" does not exist'


def reorder_filepath(scratch_dict, tbl_file_map):
    remaining = scratch_dict.keys()
    tbl_file_map = {v: k for k, v in tbl_file_map.items() if v in remaining}

    # depends on nothing  - go to front
    res = [r for r in remaining if not scratch_dict[r]]
    remaining = [r for r in remaining if r not in res]

    # nothing depends on it
    add_at_end = [r for r in remaining if r not in scratch_dict.values()]
    remaining = [r for r in remaining if r not in add_at_end]

    if not remaining:
        return [tbl_file_map[r] for r in res + add_at_end]

    while remaining:
        ins_list = remaining[:]
        for key in ins_list:
            if scratch_dict[key] in res:  # We have already seen its dependency
                # put it as far down list as possible
                index = min([r.index[k] for k, dep in scratch_dict.items() if dep == key] or -1)
                if index == -1:
                    res.append(remaining.pop(remaining.index[key]))
                else:
                    res.insert(index, remaining.pop(remaining.index[key]))
        if len(ins_list) == len(remaining):
            LOG.critical('Cyclic Dependency Detected\n Map: %s' % '\n'.join(
                ['%s->%s' % (k, v) for k, v in scratch_dict.items() if k in remaining]))
            res.extend(remaining)
            break
    return [tbl_file_map[r] for r in res + add_at_end]


def csv_file_importer(filepath, connection, table_name=False, pkey=False):
    if not exists(filepath):
        LOG.warn(u'Missing CSV for table %s', filepath.rsplit('.', 2)[0])
        return -1, ""

    table_name = table_name or basename(filepath).rsplit('.', 2)[0]

    with open(filepath) as f:
        columns = ','.join(['"%s"' % c for c in csv.reader(f).next()])
        f.seek(0)
        copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                % (table_name, columns))
        try:
            cursor = connection.cursor()
            cursor.copy_expert(copy, f)
            make_savepoint(cursor)

            res = (0,  basename(filepath))
        except Exception, e:
            msg = e.message
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
            cursor = connection.cursor()
            cursor.execute('ROLLBACK TO savepoint')
            cursor.close()
        return res


def import_from_csv(filepaths, connection, drop_fk=False):
    """ Import the csv file using postgresql COPY
    """
    # we try with brute force,
    # waiting for a pure sql implementation of get_dependencies
    remaining = list(filepaths)
    cursor = connection.cursor()
    make_savepoint(cursor)
    cursor.close()
    tbl_file_map = {k: basename(k).rsplit('.', 2)[0] for k in remaining}
    missing_columns = []
    # known_fk_deps = dict.fromkeys(tbl_file_map.values())
    while len(remaining) > 0:
        print remaining
        paths = remaining[:]
        if drop_fk:
            LOG.info(u'No Foreign Key constraints so straight import :)')
        else: #if len(remaining) == len(filepaths):
            LOG.info(u'BRUTE FORCE LOOP')
        # else:
        #     paths = [r for r in reorder_filepath(known_fk_deps, tbl_file_map) if r in remaining]
        #     LOG.info(u'MAYBE THIS IS THE BEST PATH: %s' % '->'.join(paths))
        for filepath in paths:
            tbl = tbl_file_map.get(filepath)
            error_code = csv_file_importer(filepath, connection, table_name=tbl)
            if error_code:
                if error_code[0] == 0:
                    LOG.info(u'Succesfully imported %s' % error_code[1])
                    # try:
                    #     del known_fk_deps[tbl]
                    # except KeyError:
                    #     LOG.error(u'The key %s was not found in the table map'.format(tbl))
                    remaining.remove(filepath)

                elif error_code[0] == 1:
                    pass  # known_fk_deps[tbl] = error_code[1]
                elif error_code[0] == 2:
                    missing_columns.append(error_code[1])
                else:
                    LOG.critical(u'An unknown error occurred importing csv file')

        if remaining and len(paths) == len(remaining):
            LOG.error('\n\n***\n* Could not import remaining tables : %s :-( \n***\n'
                      % ', '.join([basename(f).rsplit('.', 2)[0] for f in remaining]))
            if missing_columns:
                missing_columns = set(missing_columns)
                LOG.error('\nThe following columns were missing:\n{0}\n{1}\n{0}\n'
                          ' Fix these first as they cause dependent tables to fail. '
                          'You should fully review the table schemas, work out the applicable '
                          'module and adjust your mapping or installation as appropriate '
                          'as only the first missing column is shown. To forget them they are '
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
