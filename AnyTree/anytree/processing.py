import csv
import logging
from os.path import dirname, basename, join

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


class CSVProcessor(object):
    """ Take a csv file, process it with the mapping
    and output a new csv file
    """
    def __init__(self, mapping):
        self.mapping = mapping
        self.missing = []

    def process(self, src_filepath):
        """ The main processing method
        """
        directory = dirname(src_filepath)
        src_table = basename(src_filepath).rsplit('.', 1)[0]
        dst_columns = self.mapping.dst_columns
        with open(src_filepath, 'rb') as src_csv:
            reader = csv.DictReader(src_csv, delimiter=',')
            dst_files = {t: open(join(directory, t + '.out.csv'), 'wb') for t in dst_columns}
            writers = {t: csv.DictWriter(f, dst_columns[t], delimiter=',')
                       for t, f in dst_files.items()}
            for writer in writers.values():
                writer.writeheader()
            # process each csv line
            for src_row in reader:
                dst_rows = {t: {} for t in dst_columns}
                # process each column
                for src_column in src_row:
                    mapping = self.mapping.mapping.get(src_table + '.' + src_column)
                    if mapping is None:
                        origin = src_table + '.' + src_column
                        if origin not in self.missing:
                            LOG.warn('No mapping found for column %s', origin)
                            self.missing.append(origin)
                        continue
                    # we found a mapping, use it
                    for dst_column, function in mapping.items():
                        dst_table, dst_column = dst_column.split('.')
                        if function is None:
                            # mapping is empty: use identity
                            dst_rows[dst_table][dst_column] = src_row[src_column]
                        else:
                            # mapping is a function
                            dst_rows[dst_table][dst_column] = function(src_row)
                for table, dst_row in dst_rows.items():
                    writers[table].writerow(dst_row)
            for dst_file in dst_files.values():
                dst_file.close()
