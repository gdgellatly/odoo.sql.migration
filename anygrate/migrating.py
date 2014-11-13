import sys
import time
import psycopg2

import shutil
import argparse
from tempfile import mkdtemp
from .exporting import export_to_csv, extract_existing
from .importing import import_from_csv
from .mapping import Mapping
from .processing import CSVProcessor
from .depending import add_related_tables
from .depending import get_fk_to_update
from .sql_commands import drop_constraints, get_management_connection, create_new_db, kill_db_connections
import logging
from os.path import basename, join, abspath, dirname, exists, normpath
from os import listdir

HERE = dirname(__file__)
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def main():
    """ Main console script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--list',
                        action='store_true',
                        default=False,
                        help=u'List provided mappings')
    parser.add_argument('-s', '--source',
                        default='test',
                        help=u'Source db')
    parser.add_argument('-t', '--target',
                        help=u'Target db')
    parser.add_argument('-k', '--keepcsv',
                        action='store_true',
                        help=u'Keep csv files in the current directory')
    parser.add_argument('-r', '--relation',
                        nargs='+',
                        help=u'List of space-separated tables to migrate. '
                        'Example : res_partner res_users')
    parser.add_argument('-x', '--excluded',
                        nargs='+',
                        help=u'List of space-separated tables to exclude'
                        )
    parser.add_argument('-p', '--path',
                        default='openerp6.1-openerp7.0.yml',
                        help=u'List of mapping files. '
                        'If not found in the specified path, '
                        'each file is searched in the "mappings" dir of this tool. '
                        'Example: openerp6.1-openerp7.0.yml custom.yml',
                        nargs='+'
                        )
    parser.add_argument('-w', '--write',
                        action='store_true', default=False,
                        help=u'Really write to the target database if migration is successful.'
                        )
    parser.add_argument('-n', '--newdb',
                        help=u'Create a new database based on target. Existing db of same name will be dropped')
    parser.add_argument('-f', '--dropfk',
                        action='store_true', default=False,
                        help=u'Drops foreign key constraints on tables and adds back after import.'
                             u' Must be used with --newdb or -n')
    parser.add_argument('-q', '--quick',
                        action='store_true', default=False,
                        help=u'Turns it up to 11. '
                             u'Drops foreign key constraints on tables and adds back after import.'
                             u' Auto creates new database if -n not specified')

    args = parser.parse_args()
    source_db, target_db, relation = args.source, args.target, args.relation
    mapping_names = args.path if type(args.path) is list else [args.path]
    excluded = args.excluded or [] + [
        'ir_model'
    ]
    if args.list:
        print(u'\n'.join(listdir(join(HERE, 'mappings'))))
        sys.exit(0)

    if not all([source_db, target_db, relation]):
        print(u'Please provide at least -s, -t and -r options')
        sys.exit(1)

    if args.keepcsv:
        print(u"Writing CSV files in the current dir")

    identifier = str(int(time.time()))[-4:]

    if args.quick:
        args.dropfk = True

    if args.dropfk and not (args.newdb or args.quick):
        print(u'Due to the dangers of being unable to roll back if an error occurs\n'
              u'and the dangers of not correctly recording constraints this option\n'
              u'is only valid with the -n flag')
        sys.exit(1)

    tempdir = mkdtemp(prefix=source_db + '_' + identifier + '_',
                      dir=abspath('.'))
    identifier = basename(normpath(tempdir))
    if args.quick and not args.newdb:
        args.newdb = identifier.lower()
    print(u'The identifier for this migration is "{0}"'.format(identifier))
    migrate(source_db, target_db, relation, mapping_names,
            excluded, target_dir=tempdir, write=args.write,
            new_db=args.newdb, drop_fk=args.dropfk)
    print(u'The identifier for this migration is "{0}"'.format(identifier))
    if not args.keepcsv:
        shutil.rmtree(tempdir)


def migrate(source_db, target_db, source_tables, mapping_names,
            excluded=None, target_dir=None, write=False,
            new_db=False, drop_fk=False):
    """ The main migration function
    """
    start_time = time.time()
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    if new_db:
        target_db = create_new_db(source_db, target_db, new_db)
    target_connection = psycopg2.connect("dbname=%s" % target_db)

    # Get the list of modules installed in the target db
    with target_connection.cursor() as c:
        c.execute("select name from ir_module_module where state='installed'")
        target_modules = [m[0] for m in c.fetchall()]

    # we turn the list of wanted tables into the full list of required tables
    print(u'Computing the real list of tables to export...')
    #source_models, _ = get_dependencies('admin', 'admin',
    #                                    source_db, source_models, excluded_models)
    if drop_fk:
        print(u'Normally you would get a list of dependencies here but we don\'t care'
              u' as we are dropping the constraints')
    source_tables, m2m_tables = add_related_tables(source_connection, source_tables,
                                                   excluded, show_log=not drop_fk)
    source_tables.sort()
    col_width = max([len(tbl) for tbl in source_tables]) + 3
    max_cols = 80 / col_width
    split_tables = []
    offset = 0
    while offset < len(source_tables):
        split_tables.append(
            ''.join([tbl.ljust(col_width) for tbl in source_tables[offset:offset+max_cols]]))
        offset += max_cols

    print(u'The real list of tables to export is:\n%s' % '\n'.join(split_tables))

    # construct the mapping and the csv processor
    print('Exporting tables as CSV files...')
    filepaths = export_to_csv(source_tables, target_dir, source_connection)
    for i, mapping_name in enumerate(mapping_names):
        if not exists(mapping_name):
            mapping_names[i] = join(HERE, 'mappings', mapping_name)
            LOG.warn('%s not found. Trying %s', mapping_name, mapping_names[i])
    mapping = Mapping(target_modules, mapping_names, drop_fk=drop_fk)
    processor = CSVProcessor(mapping)
    target_tables = processor.get_target_columns(filepaths).keys()
    print(u'The real list of tables to import is: %s' % ', '.join(target_tables))
    processor.mapping.update_last_id(source_tables, source_connection,
                                     target_tables, target_connection)

    print('Computing the list of Foreign Keys to update in the target csv files...')
    processor.fk2update = get_fk_to_update(target_connection, target_tables)

    # update the list of fk to update with the fake __fk__ given in the mapping
    processor.fk2update.update(processor.mapping.fk2update)

    # extract the existing records from the target database
    existing_records = extract_existing(target_tables, m2m_tables,
                                        mapping.discriminators, target_connection)

    # create migrated csv files from exported csv files
    print(u'Migrating CSV files...')
    processor.set_existing_data(existing_records)
    processor.process(target_dir, filepaths, target_dir, target_connection)
    # drop foreign key constraints
    if drop_fk:
        print(u'Dropping Foreign Key Constraints in target tables')
        target_connection.close()
        mgmt_connection = get_management_connection(source_db)
        with mgmt_connection.cursor() as m:
            kill_db_connections(m, target_db)
        mgmt_connection.close()
        drop_fk = drop_constraints(db=target_db, tables=target_tables)
        target_connection = psycopg2.connect("dbname=%s" % target_db)

    # import data in the target
    print(u'Trying to import data in the target database...')
    target_files = [join(target_dir, '%s.target2.csv' % t) for t in target_tables]
    remaining = import_from_csv(target_files, target_connection)
    if remaining:
        print(u'Please improve the mapping by inspecting the errors above')
        sys.exit(1)

    # execute deferred updates for preexisting data
    print(u'Updating pre-existing data...')
    for table in target_tables:
        filepath = join(target_dir, table + '.update2.csv')
        if not exists(filepath):
            LOG.warn(u'Not updating %s as it was not imported', table)
            continue
        processor.update_one(filepath, target_connection)

    if write:
        target_connection.commit()
        print(u'Finished, and transaction committed !! \o/')
    else:
        target_connection.rollback()
        print(u'Finished \o/ Use --write to really write to the target database')
    target_connection.close()

    # Note we check here again just in case
    if all([(n.isalnum() or n == '_' for n in target_db)]):
        #First kill any connections
        s_mgmt_connection = get_management_connection(db=source_db)
        with s_mgmt_connection.cursor() as s:
            kill_db_connections(s, target_db)
        if new_db and not write:
            kill_db_connections(s, target_db)
            s.execute('DROP DATABASE IF EXISTS {0};'.format(target_db))
            print(u'Target Database dropped')
        elif drop_fk:
            print(u'Restoring Foreign Key Constraints')
            t_mgmt_connection = get_management_connection(db=target_db)
            with t_mgmt_connection.cursor() as t:
                t.execute(drop_fk)
            t_mgmt_connection.close()
        s_mgmt_connection.close()

    seconds = time.time() - start_time
    lines = processor.lines
    rate = lines / seconds
    print(u'Migrated %s lines in %s seconds (%s lines/s)'
          % (processor.lines, int(seconds), int(rate)))
