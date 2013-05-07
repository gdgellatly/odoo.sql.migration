import csv
import psycopg2.extras
import logging
import yaml
import os
from os.path import basename, join

HERE = os.path.dirname(__file__)
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


class CSVProcessor(object):
    """ Take a csv file, process it with the mapping
    and output a new csv file
    """
    def __init__(self, mapping, mapping_file, fields2update):

        self.fields2update = fields2update
        self.mapping = mapping
        self.target_columns = {}
        self.writers = {}
        mapping_file  = 'disc-'+mapping_file
        self.discmappingfile = os.path.join(HERE, 'mappings',
                                            mapping_file)
        self.discmapping = {}

    def get_target_columns(self, filepaths):
        """ Compute target columns with source columns + mapping
        """
        if self.target_columns:
            return self.target_columns
        for filepath in filepaths:
            source_table = basename(filepath).rsplit('.', 1)[0]
            with open(filepath) as f:
                source_columns = csv.reader(f).next()
            for source_column in source_columns:
                mapping = self.mapping.get_targets('%s.%s' % (source_table, source_column))
                # no mapping found, we warn the user
                if mapping is None:
                    origin = source_table + '.' + source_column
                    LOG.warn('No mapping definition found for column %s', origin)
                    continue
                elif mapping is False:
                    continue
                else:
                    for target in mapping:
                        t, c = target.split('.')
                        self.target_columns.setdefault(t, set()).add(c)

        self.target_columns = {k: sorted(list(v)) for k, v in self.target_columns.items()}
        return self.target_columns

    def process(self, source_dir, source_filenames, target_dir,
                target_connection, mapping_file, fields2update):
        """ The main processing method
        """
        with open(self.discmappingfile) as stream:
            self.discmapping = yaml.load(stream)
        # compute the target columns
        filepaths = [join(source_dir, source_filename) for source_filename in source_filenames]
        self.target_columns = self.get_target_columns(filepaths)
        # open target files for writing
        self.target_files = {
            table: open(join(target_dir, table + '.out.csv'), 'ab')
            for table in self.target_columns
        }
        # create csv writers
        self.writers = {t: csv.DictWriter(f, self.target_columns[t], delimiter=',')
                        for t, f in self.target_files.items()}
        # write csv headers once
        for writer in self.writers.values():
            writer.writeheader()
        # process csv files
        for source_filename in source_filenames:
            source_filepath = join(source_dir, source_filename)
            self.process_one(source_filepath, target_connection)
        for target_file in self.target_files.values():
            target_file.close()

    def process_one(self, source_filepath, target_connection):
        """ Process one csv file
        """
        source_table = basename(source_filepath).rsplit('.', 1)[0]
        with open(source_filepath, 'rb') as source_csv:
            reader = csv.DictReader(source_csv, delimiter=',')
            # process each csv line
            for source_row in reader:
                target_rows = {}
                # process each column
                for source_column in source_row:
                    mapping = self.mapping.get_targets(source_table + '.' + source_column)
                    if mapping is None:
                        continue
                    # we found a mapping, use it
                    for target_column, function in mapping.items():
                        target_table, target_column = target_column.split('.')
                        target_rows.setdefault(target_table, {})
                        if function is None:
                            # mapping is None: use identity
                            target_rows[target_table][target_column] = source_row[source_column]
                        elif function is False:
                            # mapping is False: remove the target column
                            del target_rows[target_table][target_column]
                        else:
                            # mapping is a function
                            result = function(source_row, target_rows)
                            target_rows[target_table][target_column] = result
                for table, target_row in target_rows.items():
                    if any(target_row.values()):
                        self.check_record(target_connection, table, target_row)
                        self.writers[table].writerow(target_row)

    def check_record(self, target_connection, table, target_row):
        """ Method to check if one record has an equivalent in the
        targeted db"""

        c = target_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if table in self.discmapping:
            discriminant = self.discmapping[table]
        try:
            query = "SELECT * FROM %s WHERE %s = '%s';" % (table, discriminant, target_row[discriminant])
            c.execute(query)
            record_cible = c.fetchone()
        except:
            record_cible = None
        if record_cible:
            if record_cible['id'] != target_row['id']:
                print('WE NEED TO CHANGE THE SRC ID')
                target_row['id'] = record_cible['id']
            elif record_cible['id'] == target_row['id']:
                print('JACKPOT ! NO NEED TO CHANGE')
        else:
            print('NO EQUIVALENT ! WE CAN ADD IT BUT WE NEED TO GET THE MAX ID FIRST')
            # THIS IS MAX ID FROM THE SOURCE BASE ...
            if table in self.mapping.last_id:
                target_row['id'] = self.mapping.last_id[table]+1
            else:
                print
                # Il s'agit d'une table de jointure ! Comment gerer ca ?

        """ Maintenant il faut appliquer ces modifications aux cles etrangeres
        qui referencent l\'id """
        if table in self.fields2update:
            print(self.fields2update[table])
