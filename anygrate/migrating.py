import time
import psycopg2
import shutil
import argparse
import os
from tempfile import mkdtemp
from .exporting import export_to_csv
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
    mapping_file = args.path

    print "Importing into target db is not yet supported. Use --keepcsv for now"
    if args.keepcsv:
        print "Writing CSV files in the current dir"

    tempdir = mkdtemp(prefix=source_db + '-' + str(int(time.time()))[-4:] + '-',
                      dir=os.path.abspath('.'))
    migrate(source_db, target_db, models, mapping_file,
            excluded_models, target_dir=tempdir)
    if not args.keepcsv:
        shutil.rmtree(tempdir)


def migrate(source_db, target_db, source_models, mapping_file, excluded_models=None,
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
    print('Computing the Foreign Keys to update in the exported csv files...')
    fields2update = get_fk_to_update(target_connection, source_models)

    # construct the mapping and the processor
    # (TODO? autodetect mapping file with source and target db)
    print('Exporting tables as CSV files...')
    source_tables = [model.replace('.', '_') for model in source_models]
    filepaths = export_to_csv(source_tables, target_dir, source_connection)
    mappingfile = os.path.join(HERE, 'mappings', mapping_file)
    mapping = Mapping(target_modules, mappingfile)
    processor = CSVProcessor(mapping, mapping_file, fields2update)
    target_tables = processor.get_target_columns(filepaths).keys()

    # Get the max id of source and target dbs
    for source_table in source_tables:
        with source_connection.cursor() as c:
            # FIXME the key (id) shouldn't be hardcoded below
            try:
                c.execute('select max(id) from %s' % source_table)
                mapping.last_id[source_table] = c.fetchone()[0]
            except psycopg2.ProgrammingError:
                LOG.debug(u'"id" column does not exist in table "%s"', source_table)
                source_connection.rollback()
    for target_table in target_tables:
        with target_connection.cursor() as c:
            # FIXME the key (id) shouldn't be hardcoded below
            try:
                c.execute('select max(id) from %s' % target_table)
                mapping.last_id[source_table] = max(
                    c.fetchone()[0],
                    mapping.last_id[source_table])
            except psycopg2.ProgrammingError:
                LOG.debug(u'"id" column does not exist in table "%s"', source_table)
                target_connection.rollback()
            except KeyError:
                LOG.debug(u'Cannot create a record for this table')

    # create migrated csv files from exported csv
    processor.process(target_dir, filepaths, target_dir, target_connection,
                      fields2update, mappingfile)
