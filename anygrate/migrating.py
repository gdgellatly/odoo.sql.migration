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
import logging
from os.path import basename, join, abspath, dirname, exists
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
                        help=u'Really write to the target database if migration is successful'
                        )

    args = parser.parse_args()
    source_db, target_db, relation = args.source, args.target, args.relation
    mapping_names = args.path if type(args.path) is list else [args.path]
    excluded = args.excluded or [] + [
        'ir_model'
    ]
    if args.list:
        print '\n'.join(listdir(join(HERE, 'mappings')))
        sys.exit(0)

    if not all([source_db, target_db, relation]):
        print 'Please provide at least -s, -t and -r options'
        sys.exit(1)

    if args.keepcsv:
        print "Writing CSV files in the current dir"

    tempdir = mkdtemp(prefix=source_db + '-' + str(int(time.time()))[-4:] + '-',
                      dir=abspath('.'))
    migrate(source_db, target_db, relation, mapping_names,
            excluded, target_dir=tempdir, write=args.write)
    if not args.keepcsv:
        shutil.rmtree(tempdir)


def migrate(source_db, target_db, source_tables, mapping_names,
            excluded=None, target_dir=None, write=False):
    """ The main migration function
    """
    start_time = time.time()
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    target_connection = psycopg2.connect("dbname=%s" % target_db)

    # Get the list of modules installed in the target db
    with target_connection.cursor() as c:
        c.execute("select name from ir_module_module where state='installed'")
        target_modules = [m[0] for m in c.fetchall()]

    # we turn the list of wanted tables into the full list of required tables
    print(u'Computing the real list of tables to export...')
    #source_models, _ = get_dependencies('admin', 'admin',
    #                                    source_db, source_models, excluded_models)
    source_tables, m2m_tables = add_related_tables(source_connection, source_tables,
                                                   excluded)
    print(u'The real list of tables to export is: %s' % ', '.join(source_tables))

    # construct the mapping and the csv processor
    # (TODO? autodetect mapping file with source and target db)
    print('Exporting tables as CSV files...')
    filepaths = export_to_csv(source_tables, target_dir, source_connection)
    for i, mapping_name in enumerate(mapping_names):
        if not exists(mapping_name):
            mapping_names[i] = join(HERE, 'mappings', mapping_name)
            LOG.warn('%s not found. Trying %s', mapping_name, mapping_names[i])
    mapping = Mapping(target_modules, mapping_names)
    processor = CSVProcessor(mapping)
    target_tables = processor.get_target_columns(filepaths).keys()
    print(u'The real list of tables to import is: %s' % ', '.join(target_tables))
    processor.mapping.update_last_id(source_tables, source_connection,
                                     target_tables, target_connection)

    print('Computing the list of Foreign Keys to update in the exported csv files...')
    processor.fk2update = get_fk_to_update(target_connection, target_tables)

    # update the list of fk to update with the fake fk given in the mapping
    processor.fk2update.update(processor.mapping.fk2update)

    # extract the existing records from the target database
    existing_records = extract_existing(target_tables, m2m_tables,
                                        mapping.discriminators, target_connection)

    # create migrated csv files from exported csv
    print(u'Migrating CSV files...')
    # FIXME refactor the process() arguments, there are too many of them
    processor.process(target_dir, filepaths, target_dir,
                      target_connection, existing_records)

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

    seconds = time.time() - start_time
    lines = processor.lines
    rate = lines / seconds
    print(u'Migrated %s lines in %s seconds (%s lines/s)'
          % (processor.lines, int(seconds), int(rate)))
