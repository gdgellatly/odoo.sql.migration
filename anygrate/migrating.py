import time
import psycopg2
import shutil
import argparse
import os
from tempfile import mkdtemp
from .exporting import export_to_csv, extract_existing
from .importing import import_from_csv
from .mapping import Mapping
from .processing import CSVProcessor
from .depending import get_dependencies
from .depending import get_fk_to_update
import logging
from os.path import basename

HERE = os.path.dirname(__file__)
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

    args = parser.parse_args()
    source_db, target_db, models = args.source, args.target, args.models
    excluded_models = args.excluded_models
    mapping_name = args.path

    print "Importing into target db is not yet supported. Use --keepcsv for now"
    if args.keepcsv:
        print "Writing CSV files in the current dir"

    tempdir = mkdtemp(prefix=source_db + '-' + str(int(time.time()))[-4:] + '-',
                      dir=os.path.abspath('.'))
    migrate(source_db, target_db, models, mapping_name,
            excluded_models, target_dir=tempdir)
    if not args.keepcsv:
        shutil.rmtree(tempdir)


def migrate(source_db, target_db, source_models, mapping_name, excluded_models=None,
            target_dir=None):
    """ The main migration function
    """
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    target_connection = psycopg2.connect("dbname=%s" % target_db)

    # Get the list of modules installed in the target db
    with target_connection.cursor() as c:
        c.execute("select name from ir_module_module where state='installed'")
        target_modules = [m[0] for m in c.fetchall()]

    # we turn the list of wanted models into the full list of required models
    print('Computing the real list of models to export...')
    source_models = get_dependencies('admin', 'admin',
                                     source_db, source_models, excluded_models)

    # compute the foreign keys to modify in the csv
    print('Computing the list of Foreign Keys to update in the exported csv files...')
    fields2update = get_fk_to_update(target_connection, source_models)

    # construct the mapping and the csv processor
    # (TODO? autodetect mapping file with source and target db)
    print('Exporting tables as CSV files...')
    source_tables = [model.replace('.', '_') for model in source_models]
    filepaths = export_to_csv(source_tables, target_dir, source_connection)
    mappingfile = os.path.join(HERE, 'mappings', mapping_name)
    mapping = Mapping(target_modules, mappingfile)
    processor = CSVProcessor(mapping, fields2update)
    target_tables = processor.get_target_columns(filepaths).keys()
    processor.mapping.update_last_id(source_tables, source_connection,
                                     target_tables, target_connection)

    # extract the existing records from the target database
    existing_records = extract_existing(source_tables, mapping.discriminators, target_connection)

    # create migrated csv files from exported csv
    print(u'Migrating CSV files...')
    # FIXME refactor the process() arguments, there are too many of them
    processor.process(target_dir, filepaths, target_dir,
                      target_connection, existing_records, fields2update)

    # import data in the target
    print(u'Trying to import data in the target database...')
    # FIXME below not reliable. We should only manage tables, not models.
    target_models = [c.replace('_', '.') for c in target_tables]
    target_models = get_dependencies('admin', 'admin',
                                     source_db, target_models, excluded_models)
    target_files = ['%s.out.csv' % c for c in target_tables]
    import_from_csv(target_files, target_connection)

    # \o/
    print(u'Finished ! \o/')
