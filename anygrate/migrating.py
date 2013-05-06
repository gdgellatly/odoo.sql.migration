import time
import psycopg2
import shutil
import argparse
import os
from tempfile import mkdtemp
from .exporting import export_tables
from .mapping import Mapping
from .processing import CSVProcessor
from .depending import get_ordre_importation
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
                        help=u'List of space-separated models to export'
                        'Example : (res.partner res.users)')
    parser.add_argument('-x', '--excluded_models', nargs='+',
                        required=True,
                        help=u'List of space-separated models to exclude'
                        )

    args = parser.parse_args()
    source_db, target_db, models = args.source, args.target, args.models
    excluded_models = args.excluded_models

    print "Importing into target db is not yet supported. Use --keepcsv for now"
    if args.keepcsv:
        print "Writing CSV files in the current dir"

    tempdir = mkdtemp(prefix=source_db + '-' + str(int(time.time()))[-4:] + '-',
                      dir=os.path.abspath('.'))
    migrate(source_db, target_db, models, excluded_models, target_dir=tempdir)
    if not args.keepcsv:
        shutil.rmtree(tempdir)


def migrate(source_db, target_db, models, excluded_models=None,
            target_dir=None):
    """ Migrate using importing/mapping/processing modules
    """
    source_connection = psycopg2.connect("dbname=%s" % source_db)
    target_connection = psycopg2.connect("dbname=%s" % target_db)
    source_tables = []
    ordered_models = get_ordre_importation('admin', 'admin',
                                           source_db, models, None)
    for model in ordered_models:
        source_tables.append(model.replace('.', '_'))
    target_modules = ['base']
    filepaths = export_tables(source_tables, target_dir,
                              source_connection)
    # TODO autodetect mapping file with source and target db
    mappingfile = os.path.join(HERE, 'mappings', 'openerp6.1-openerp7.0.yml')
    mapping = Mapping(target_modules, mappingfile)
    processor = CSVProcessor(mapping)
    target_tables = processor.get_target_columns(filepaths).keys()
    for source_table in source_tables:
        with source_connection.cursor() as c:
            # FIXME the key (id) shouldn't be hardcoded below
            try:
                c.execute('select max(id) from %s' % source_table)
                mapping.last_id[source_table] = c.fetchone()[0]
            except psycopg2.ProgrammingError:
                LOG.debug(u'La colonne id n\'existe pas'
                          'pour la table %s ' % source_table)
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
                LOG.debug(u'La colonne id n\'existe pas'
                          'pour la table %s ' % source_table)
                target_connection.rollback()
            except KeyError:
                LOG.debug(u'Impossible de creer un enregistrement'
                          ' pour cette table')
    processor.process(target_dir, filepaths, target_dir)
