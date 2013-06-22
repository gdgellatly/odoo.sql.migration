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

HERE = dirname(__file__)
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def main():
    """ Main console script
    """
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument('-s', '--source',
                        default='test',
                        required=True,
                        help=u'Source db')
    parser.add_argument('-t', '--target',
                        required=True,
                        help=u'Target db')
    parser.add_argument('-k', '--keepcsv',
                        action='store_true',
                        help=u'Keep csv files in the current directory')
    parser.add_argument('-m', '--models', nargs='+',
                        required=True,
                        help=u'List of space-separated models to migrate'
                        'Example : (res.partner res.users)')
    parser.add_argument('-x', '--excluded_models', nargs='+',
                        required=False,
                        help=u'List of space-separated models to exclude'
                        )
    parser.add_argument('-p', '--path',
                        required=False, default='openerp6.1-openerp7.0.yml',
                        help=u'filemane.yml'
                        'the file must be stored in the mappings dir'  # FIXME allow a real path
                        'Exemple: openerp6.1-openerp7.0.yml'
                        )
    parser.add_argument('-w', '--write',
                        action='store_true', default=False,
                        help=u'Really write to the target database if migration is successfull'
                        )

    args = parser.parse_args()
    source_db, target_db, models = args.source, args.target, args.models
    excluded_models = args.excluded_models or [] + [
        'ir.model'
    ]
    mapping_name = args.path

    print "Importing into target db is not yet supported. Use --keepcsv for now"
    if args.keepcsv:
        print "Writing CSV files in the current dir"

    tempdir = mkdtemp(prefix=source_db + '-' + str(int(time.time()))[-4:] + '-',
                      dir=abspath('.'))
    migrate(source_db, target_db, models, mapping_name,
            excluded_models, target_dir=tempdir, write=args.write)
    if not args.keepcsv:
        shutil.rmtree(tempdir)


def migrate(source_db, target_db, source_tables, mapping_name,
            excluded_models=None, target_dir=None, write=False):
    """ The main migration function
    """
    start_time = time.time()
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    target_connection = psycopg2.connect("dbname=%s" % target_db)

    # Get the list of modules installed in the target db
    with target_connection.cursor() as c:
        c.execute("select name from ir_module_module where state='installed'")
        target_modules = [m[0] for m in c.fetchall()]

    # we turn the list of wanted models into the full list of required models
    print(u'Computing the real list of tables to export...')
    #source_models, _ = get_dependencies('admin', 'admin',
    #                                    source_db, source_models, excluded_models)
    source_tables, m2m_tables = add_related_tables(source_connection, source_tables,
                                                   excluded_models)
    print(u'The real list of tables to export is: %s' % ', '.join(source_tables))

    # construct the mapping and the csv processor
    # (TODO? autodetect mapping file with source and target db)
    print('Exporting tables as CSV files...')
    filepaths = export_to_csv(source_tables, target_dir, source_connection)
    mappingfile = join(HERE, 'mappings', mapping_name)
    mapping = Mapping(target_modules, mappingfile)
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
    target_files = [join(target_dir, '%s.target2.csv' % c) for c in target_tables]
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
