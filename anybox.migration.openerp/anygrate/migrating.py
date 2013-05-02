import psycopg2
import argparse
import os
from .exporting import export_tables
from .mapping import Mapping
from .processing import CSVProcessor
from .depending import get_ordre_importation

HERE = os.path.dirname(__file__)


def main():
    """ Main console script
    """
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument('-s', '--source',
                        default='test',
                        required=True,
                        help='Source db')
    parser.add_argument('-t', '--target',
                        required=True,
                        help='Target db')
    parser.add_argument('-k', '--keepcsv',
                        action='store_true',
                        help='Keep csv files in the current directory')
    args = parser.parse_args()
    source_db, target_db = args.source, args.target

    print "Importing in target db is not yet supported. Use --keepcsv for now"
    if args.keepcsv:
        print "Writing CSV files in the current dir"
    migrate(source_db, target_db,
            target_dir='.' if args.keepcsv else None)


def migrate(source_db, target_db, target_dir=None):
    """ Migrate using importing/mapping/processing modules
    """
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    target_connection = psycopg2.connect("dbname=%s" % target_db)
    source_models = []
    ordered_source_tables = []
    # FIXME automatically determine dependent tables
    source_tables = [
        'res_partner_address',
        'res_partner',
        'res_users',
        'res_partner_title'
    ]
    # From PSQL tables to OERP models ...
    for table in source_tables:
        source_models.append(table.replace('_', '.'))
    ordered_dependencies = get_ordre_importation('admin', 'admin',
                                                 source_db, source_models, None)
    # ... and from models to tables
    for model in ordered_dependencies:
        ordered_source_tables.append(model.replace('.', '_'))
    target_modules = ['base']
    filepaths = export_tables(ordered_source_tables, target_dir,
                              source_connection)
    # TODO autodetect mapping file with source and target db
    mappingfile = os.path.join(HERE, 'mappings', 'openerp6.1-openerp7.0.yml')
    mapping = Mapping(target_modules, mappingfile)
    processor = CSVProcessor(mapping)
    target_tables = processor.get_target_columns(filepaths).keys()
    with source_connection.cursor() as c:
        for source_table in source_tables:
            # FIXME the key (id) shouldn't be hardcoded below
            c.execute('select max(id) from %s' % source_table)
            mapping.last_id[source_table] = c.fetchone()[0]
    with target_connection.cursor() as c:
        for target_table in target_tables:
            # FIXME the key (id) shouldn't be hardcoded below
            c.execute('select max(id) from %s' % target_table)
            mapping.last_id[source_table] = max(
                c.fetchone()[0],
                mapping.last_id[source_table])

    processor.process(target_dir, filepaths, target_dir)
