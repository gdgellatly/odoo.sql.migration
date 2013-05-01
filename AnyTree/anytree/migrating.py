import psycopg2
import argparse
import os
from .exporting import export_tables
from .mapping import Mapping
from .processing import CSVProcessor

HERE = os.path.dirname(__file__)


def main():
    """ Main console script
    """
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument('-i', '--input',
                        default='test',
                        required=True,
                        help='Migration input.\npg:dbname only supported for now')
    parser.add_argument('-o', '--output',
                        required=True,
                        help=('Migration output. Ex:\n'
                              '  csv:/tmp/ for a csv output\n'
                              '  or pg:dbname for a postgres db insertion'))
    args = parser.parse_args()

    output_type, output_name = args.output.split(':')
    input_type, input_name = args.input.split(':')

    if input_type != 'pg':
        print u"Only 'pg' is currently supported for the migration source"
        return

    if output_type == 'csv':
        migrate(source_db=input_name,
                target_dir=output_name)
    if output_type == 'pg':
        migrate(source_db=input_name,
                target_dir='/tmp/',  # TODO remove
                target_db=output_name)


def migrate(source_db, target_dir=None, target_db=None):
    """ Migrate using importing/mapping/processing modules
    """
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    if target_db is not None:
        target_connection = psycopg2.connect("dbname=%s" % target_db)
        print 'target db importing is not yet implemented. Writing csv in /tmp'

    # FIXME automatically determine dependent tables
    source_tables = [
        'res_partner_address',
        'res_partner',
        'res_users',
        'res_partner_title'
    ]
    target_modules = ['base']
    filepaths = export_tables(source_tables, target_dir, source_connection)
    # TODO autodetect mapping file with input and output db
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
