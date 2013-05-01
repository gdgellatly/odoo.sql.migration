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
        print u"'pg' output is not yet implemented"
        return


def migrate(source_db, target_dir=None, target_db=None):
    """ Migrate using importing/mapping/processing modules
    """
    if target_db is not None or target_dir is None:
        raise NotImplementedError
    # FIXME automatically determine dependent tables
    tables = [
        'res_partner_address',
        'res_partner',
        'res_users',
        'res_partner_title'
    ]
    modules = ['base']
    filenames = export_tables(tables, target_dir, db=source_db)
    # TODO autodetect mapping with input and output db
    mappingfile = os.path.join(HERE, 'mappings', 'openerp6.1-openerp7.0.yml')
    mapping = Mapping(modules, mappingfile)
    processing = CSVProcessor(mapping)
    processing.process(target_dir, filenames, target_dir)
