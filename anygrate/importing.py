from os.path import basename, exists
from os import rename
import csv
import logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))

FK_VIOLATION = 'violates foreign key constraint'
MISSING_COLUMN = 'of relation "%s" does not exist'


def get_dependency_tree(scratch_dict):
    remaining = scratch_dict.keys()
    # depends on nothing  - go to front
    res = [r for r in remaining if not scratch_dict[r]]
    remaining = [r for r in remaining if r not in res]
    # nothing depends on it
    add_at_end = [r for r in remaining if r not in scratch_dict.values()]
    remaining = [r for r in remaining if r not in add_at_end]
    if not remaining:
        return res + add_at_end

    while remaining:
        ins_list = list(remaining)
        [res.insert(remaining.pop(remaining.index(r)), res.index(scratch_dict[key]) + 1)
         for key in ins_list if scratch_dict[key] in r]
        if len(ins_list) == len(remaining):
            LOG.critical('Cyclic Dependency Detected\n Map: %s' % '\n'.join(
                ['%s->%s' % (k, v) for k, v in scratch_dict.items() if k in remaining]))
            res.extend(remaining)
            break
    return res + add_at_end


def import_from_csv(filepaths, connection):
    """ Import the csv file using postgresql COPY
    """
    # we try with brute force,
    # waiting for a pure sql implementation of get_dependencies
    remaining = list(filepaths)
    cursor = connection.cursor()
    cursor.execute('SAVEPOINT savepoint')
    cursor.close()
    tbl_file_map = {k: basename(k).rsplit('.', 2)[0] for k in remaining}
    missing_columns = []
    if len(remaining) > 1:
        dependency_helper = dict.fromkeys(tbl_file_map.values())
    while len(remaining) > 0:
        LOG.info(u'NOT SUCH A BRUTE FORCE LOOP')
        paths = get_dependency_tree(dependency_helper)
        LOG.info(u'MAYBE THIS IS THE BEST PATH')
        for filepath in paths:
            if not exists(filepath):
                LOG.warn(u'Missing CSV for table %s', filepath.rsplit('.', 2)[0])
                continue
            with open(filepath) as f:
                columns = ','.join(['"%s"' % c for c in csv.reader(f).next()])
                f.seek(0)
                copy = ("COPY %s (%s) FROM STDOUT WITH CSV HEADER NULL ''"
                        % (basename(filepath).rsplit('.', 2)[0], columns))
                try:
                    cursor = connection.cursor()
                    cursor.copy_expert(copy, f)
                    try:
                        cursor.execute('RELEASE SAVEPOINT savepoint')
                    except Exception:
                        pass
                    finally:
                        cursor.execute('SAVEPOINT savepoint')
                    LOG.info('Succesfully imported %s' % basename(filepath))
                    del dependency_helper[tbl_file_map[basename(filepath)]]
                    remaining.remove(filepath)
                except Exception, e:
                    tbl = tbl_file_map[basename(filepath)]
                    msg = e.message
                    LOG.warn('Error importing file %s:\n%s',
                             basename(filepath), msg)
                    if FK_VIOLATION in msg:
                        last = msg.rfind('"')
                        first = msg.rfind('"', 0, last) + 1
                        dependency_helper[tbl] = msg[first:last]
                    elif MISSING_COLUMN % tbl in msg:
                        first = msg('"') + 1
                        last = msg('"', first)
                        col = msg[first:last]
                        LOG.warn('Missing columns cause dependent tables to fail'
                                 ' making you think you have cyclic dependencies when you don\'t')
                        missing_columns.append('    {0}.{1}:\n'
                                               '        {0}.{1}: __forget__'.format(tbl, col))
                    cursor = connection.cursor()
                    cursor.execute('ROLLBACK TO savepoint')
                    cursor.close()
        if len(paths) == len(remaining):
            LOG.error('\n\n***\n* Could not import remaining tables : %s :-( \n***\n'
                      % ', '.join([basename(f).rsplit('.', 2)[0] for f in remaining]))
            if missing_columns:
                LOG.error('The following columns were missing:\n %s \n'
                          ' Fix these first as they cause dependent tables to fail. '
                          'You should fully review the table schemas, work out the applicable '
                          'module and adjust your mapping or installation as appropriate '
                          'as only the first missing column is shown. To forget them they are '
                          'displayed in the YAML syntax needed'
                          % '\n'.join(missing_columns))
            # don't permit update for non imported files
            for update_file in [filename.replace('.target2.csv', '_temp.update2.csv')
                                for filename in remaining]:
                rename(update_file, update_file + '.disabled')
            break
    else:
            LOG.info('\n\n***\n* Successfully imported all csv files!! :-)\n***\n')
    return remaining
