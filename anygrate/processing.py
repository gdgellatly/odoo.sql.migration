import csv
import logging
import os
from os.path import basename, join

HERE = os.path.dirname(__file__)
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


class CSVProcessor(object):
    """ Take a csv file, process it with the mapping
    and output a new csv file
    """
    def __init__(self, mapping, fields2update=None):

        self.fields2update = fields2update
        self.mapping = mapping
        self.target_columns = {}
        self.writers = {}
        self.updated_values = {}
        self.fk_mapping = {}

    def get_target_columns(self, filepaths):
        """ Compute target columns with source columns + mapping
        """
        if self.target_columns:
            return self.target_columns
        for filepath in filepaths:
            source_table = basename(filepath).rsplit('.', 1)[0]
            with open(filepath) as f:
                source_columns = csv.reader(f).next()
            for source_column in source_columns + ['_']:
                mapping = self.mapping.get_targets('%s.%s' % (source_table, source_column))
                # no mapping found, we warn the user
                if mapping in (None, '__copy__'):
                    origin = source_table + '.' + source_column
                    if source_column != '_':
                        LOG.warn('No mapping definition found for column %s', origin)
                    continue
                elif mapping in (False, '__forget__'):
                    continue
                else:
                    for target in mapping:
                        t, c = target.split('.')
                        self.target_columns.setdefault(t, set()).add(c)

        self.target_columns = {k: sorted([c for c in v if c != '_'])
                               for k, v in self.target_columns.items()}
        return self.target_columns

    def process(self, source_dir, source_filenames, target_dir,
                target_connection=None, existing_records=None, fields2update=None):
        """ The main processing method
        """
        # compute the target columns
        filepaths = [join(source_dir, source_filename) for source_filename in source_filenames]
        self.target_columns = self.get_target_columns(filepaths)
        # load discriminator values for target tables
        # TODO

        # filenames and files
        target_filenames = {
            table: join(target_dir, table + '.target.csv')
            for table in self.target_columns
        }
        target_files = {
            table: open(filename, 'ab')
            for table, filename in target_filenames.items()
        }
        self.writers = {t: csv.DictWriter(f, self.target_columns[t], delimiter=',')
                        for t, f in target_files.items()}
        for writer in self.writers.values():
            writer.writeheader()

        # update filenames and files
        update_filenames = {
            table: join(target_dir, table + '.update.csv')
            for table in self.target_columns
        }
        update_files = {
            table: open(filename, 'ab')
            for table, filename in update_filenames.items()
        }
        self.updatewriters = {t: csv.DictWriter(f, self.target_columns[t], delimiter=',')
                              for t, f in update_files.items()}
        for writer in self.updatewriters.values():
            writer.writeheader()
        for source_filename in source_filenames:
            source_filepath = join(source_dir, source_filename)
            self.process_one(source_filepath, target_connection, existing_records, fields2update)
        # close files
        for target_file in target_files.values():
            target_file.close()
        for update_file in update_files.values():
            update_file.close()

        # POSTPROCESS target filenames and files
        target2_filenames = {
            table: join(target_dir, table + '.target2.csv')
            for table in self.target_columns
        }
        target2_files = {
            table: open(filename, 'ab')
            for table, filename in target2_filenames.items()
        }
        self.writers = {t: csv.DictWriter(f, self.target_columns[t], delimiter=',')
                        for t, f in target2_files.items()}
        for writer in self.writers.values():
            writer.writeheader()
        for target_filename in target_filenames.values():
            filepath = join(target_dir, target_filename)
            self.postprocess_one(filepath, existing_records, fields2update)
        for target2_file in target2_files.values():
            target2_file.close()

        # POSTPROCESS update filenames and files
        update2_filenames = {
            table: join(target_dir, table + '.update2.csv')
            for table in self.target_columns
        }
        update2_files = {
            table: open(filename, 'ab')
            for table, filename in update2_filenames.items()
        }
        self.writers = {t: csv.DictWriter(f, self.target_columns[t], delimiter=',')
                        for t, f in update2_files.items()}
        for writer in self.writers.values():
            writer.writeheader()
        for update_filename in update_filenames.values():
            update_filepath = join(target_dir, update_filename)
            self.postprocess_one(update_filepath, existing_records, fields2update)
        # close files
        for update2_file in update2_files.values():
            update2_file.close()

    def process_one(self, source_filepath,
                    target_connection=None, existing_records=None, fields2update=None):
        """ Process one csv file
        """
        source_table = basename(source_filepath).rsplit('.', 1)[0]
        with open(source_filepath, 'rb') as source_csv:
            reader = csv.DictReader(source_csv, delimiter=',')
            # process each csv line
            for source_row in reader:
                target_rows = {}
                # process each column (also handle '_' as a possible new column)
                source_row.update({'_': None})
                for source_column in source_row:
                    mapping = self.mapping.get_targets(source_table + '.' + source_column)
                    if mapping is None:
                        continue
                    # we found a mapping, use it
                    for target_column, function in mapping.items():
                        target_table, target_column = target_column.split('.')
                        target_rows.setdefault(target_table, {})
                        if target_column == '_':
                            continue
                        if function in (None, '__copy__'):
                            # mapping is None: use identity
                            target_rows[target_table][target_column] = source_row[source_column]
                        elif function in (False, '__forget__'):
                            # mapping is False: remove the target column
                            del target_rows[target_table][target_column]
                        else:
                            # mapping is supposed to be a function
                            result = function(source_row, target_rows)
                            target_rows[target_table][target_column] = result

                # offset everything but existing data and choose to write now or update later
                for table, target_row in target_rows.items():
                    if not any(target_row.values()):
                        continue
                    discriminators = self.mapping.discriminators.get(table)
                    # if the line exists in the target db, we don't offset and write to update file
                    # (we recognize by matching the set of discriminator values against existing)
                    existing = existing_records[table]
                    existing_without_id = [{v for k, v in nt.iteritems() if k != 'id'}
                                           for nt in existing]
                    discriminator_values = {target_row[d] for d in (discriminators or [])}
                    if discriminators and discriminator_values in existing_without_id:
                        # find the id of the existing record in the target
                        for i, nt in enumerate(existing):
                            if discriminator_values == {v for k, v in nt.items() if k != 'id'}:
                                real_target_id = existing[i]['id']
                                break
                        self.fk_mapping.setdefault(table, {})
                        # we save the match between source and target for existing id
                        # to be able to update the fks in the 2nd pass
                        self.fk_mapping[table][int(target_row['id'])] = real_target_id
                        self.updatewriters[table].writerow(target_row)
                    else:
                        # offset the id of the line, except for m2m
                        if (fields2update and 'id' in target_row
                                and table in fields2update.itervalues()):
                            last_id = self.mapping.last_id.get(table, 0)
                            target_row['id'] = int(target_row['id']) + last_id
                            # otherwise write the target csv line
                        self.writers[table].writerow(target_row)

    def postprocess_one(self, target_filepath, existing_records=None, fields2update=None):
        """ Postprocess one target csv file
        """
        table = basename(target_filepath).rsplit('.', 2)[0]
        with open(target_filepath, 'rb') as target_csv:
            reader = csv.DictReader(target_csv, delimiter=',')
            for target_row in reader:
                postprocessed_row = {}
                # fix the foreign keys of the line
                for key, value in target_row.items():
                    postprocessed_row[key] = value
                    fk_table = fields2update.get(table + '.' + key)
                    # if this is a fk, fix it
                    if value and fk_table:
                        last_id = self.mapping.last_id.get(fk_table, 0)
                        # if the target record is an existing record it should be in the fk_mapping
                        # so we restore the real target id, or offset it if not found
                        value = int(value)
                        if fk_table in self.fk_mapping and value in self.fk_mapping[fk_table]:
                            postprocessed_row[key] = self.fk_mapping[fk_table][value]
                        else:
                            postprocessed_row[key] = value + last_id
                self.writers[table].writerow(postprocessed_row)

    def update_one(self, filepath, target_connection):
        """ Apply updates in the target db with update file
        """
        table = basename(filepath).rsplit('.', 2)[0]
        discriminators = self.mapping.discriminators.get(table)
        # create a deferred update for existing data in the target
        if not discriminators:
            LOG.warn(u'Cannot update table %s, no discriminators found', table)
            return
        with open(filepath, 'rb') as update_csv, target_connection.cursor() as cursor:
            reader = csv.DictReader(update_csv, delimiter=',')
            for update_row in reader:
                columns = ', '.join(update_row.keys())
                values = ', '.join(['%s' for i in update_row])
                args = [i if i != '' else None for i in update_row.values()
                        ] + [update_row['id']]
                cursor.execute('UPDATE %s SET (%s)=(%s) WHERE id=%s'
                               % (table, columns, values, '%s'), tuple(args))
