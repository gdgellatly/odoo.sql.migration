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
    def __init__(self, mapping, fk2update=None):

        self.fk2update = fk2update or {}
        self.mapping = mapping
        self.target_columns = {}
        self.writers = {}
        self.updated_values = {}
        self.fk_mapping = {}
        self.lines = 0
        self.moved_records = {}

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
                target_connection=None, existing_records=None):
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
        LOG.info(u"Processing CSV files...")
        for source_filename in source_filenames:
            source_filepath = join(source_dir, source_filename)
            self.process_one(source_filepath, target_connection, existing_records)
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
        LOG.info(u"Postprocessing CSV files...")
        for filename in target_filenames.values():
            filepath = join(target_dir, filename)
            self.postprocess_one(filepath, existing_records)
        for f in target2_files.values():
            f.close()

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
        for filename in update_filenames.values():
            filepath = join(target_dir, filename)
            self.postprocess_one(filepath, existing_records)
        # close files
        for f in update2_files.values():
            f.close()

    def process_one(self, source_filepath,
                    target_connection=None, existing_records=None):
        """ Process one csv file
        """
        existing_records = existing_records or {}
        source_table = basename(source_filepath).rsplit('.', 1)[0]
        with open(source_filepath, 'rb') as source_csv:
            reader = csv.DictReader(source_csv, delimiter=',')
            self.is_moved = set()
            # process each csv line
            for source_row in reader:
                self.lines += 1
                target_rows = {}
                # process each column (also handle '_' as a possible new column)
                source_row.update({'_': None})
                for source_column in source_row:
                    mapping = self.mapping.get_targets(source_table + '.' + source_column)
                    if mapping is None:
                        continue
                    # we found a mapping, use it
                    for target_record, function in mapping.items():
                        target_table, target_column = target_record.split('.')
                        target_rows.setdefault(target_table, {})
                        if target_column == '_':
                            continue
                        if function in (None, '__copy__'):
                            # mapping is None: use identity
                            target_rows[target_table][target_column] = source_row[source_column]
                        elif function in (False, '__forget__'):
                            # mapping is False: remove the target column
                            del target_rows[target_table][target_column]
                        # in case the id has moved to a new record,
                        # we should save the mapping to correctly fix fks
                        # This can happen in case of semantic change like res.partner.address
                        elif function == '__moved__':
                            self.is_moved.add(target_table)
                            newid = self.mapping.newid()
                            target_rows[target_table][target_column] = newid
                            self.fk_mapping.setdefault(source_table, {})
                            self.fk_mapping[source_table][int(source_row[source_column])] \
                                = newid + self.mapping.last_id
                        else:
                            # mapping is supposed to be a function
                            result = function(self, source_row, target_rows)
                            target_rows[target_table][target_column] = result

                # offset all ids except existing data and choose to write now or update later
                for table, target_row in target_rows.items():
                    if not any(target_row.values()):
                        continue
                    discriminators = self.mapping.discriminators.get(table)
                    # if the line exists in the target db, we don't offset and write to update file
                    # (we recognize by matching the dict of discriminator values against existing)
                    existing = existing_records.get(table, [])
                    existing_without_id = [{k: str(v) for k, v in nt.iteritems() if k != 'id'}
                                           for nt in existing]
                    discriminator_values = {d: target_row[d] for d in (discriminators or [])}
                    # before matching existing, we should fix the discriminator_values which are fk
                    # FIXME refactor and merge with the code in postprocess
                    for key, value in discriminator_values.items():
                        fk_table = self.fk2update.get(table + '.' + key)
                        if not fk_table:
                            continue
                        value = int(value)
                        if value in self.fk_mapping.get(fk_table, []):
                            discriminator_values[key] = str(
                                self.fk_mapping[fk_table].get(value, value))
                    if (discriminators
                            and 'id' in target_row
                            and all(discriminator_values.values())
                            and discriminator_values in existing_without_id):
                        # find the id of the existing record in the target
                        for i, nt in enumerate(existing):
                            if discriminator_values == {k: str(v)
                                                        for k, v in nt.iteritems() if k != 'id'}:
                                existing_id = existing[i]['id']
                                break
                        self.fk_mapping.setdefault(table, {})
                        # we save the match between source and existing id
                        # to be able to update the fks in the 2nd pass
                        if table in self.is_moved:
                            target_row['id'] = existing_id
                            source_id = int(source_row['id'])
                            self.fk_mapping[source_table][source_id] = existing_id
                        else:  # normal
                            self.fk_mapping[table][int(target_row['id'])] = existing_id
                        self.updatewriters[table].writerow(target_row)
                    else:
                        # offset the id of the line, except for m2m (no id)
                        if 'id' in target_row:
                            target_row['id'] = int(target_row['id']) + self.mapping.last_id
                            # handle deferred records
                            if table in self.mapping.deferred:
                                upd_row = {k: v for k, v in target_row.iteritems()
                                           if k == 'id'
                                           or (k in self.mapping.deferred[table] and v != '')}
                                if len(upd_row) > 1:
                                    self.updatewriters[table].writerow(upd_row)
                                for k in self.mapping.deferred[table]:
                                    if k in target_row:
                                        del target_row[k]
                        # otherwise write the target csv line
                        self.writers[table].writerow(target_row)

    def postprocess_one(self, target_filepath, existing_records=None):
        """ Postprocess one target csv file
        """
        existing_records = existing_records or {}
        table = basename(target_filepath).rsplit('.', 2)[0]
        with open(target_filepath, 'rb') as target_csv:
            reader = csv.DictReader(target_csv, delimiter=',')
            for target_row in reader:
                postprocessed_row = {}
                # fix the foreign keys of the line
                for key, value in target_row.items():
                    target_record = table + '.' + key
                    postprocessed_row[key] = value
                    fk_table = self.fk2update.get(target_record)
                    # if this is a fk, fix it
                    if value and fk_table:
                        # if the target record is an existing record it should be in the fk_mapping
                        # so we restore the real target id, or offset it if not found
                        value = int(value)
                        if value in self.fk_mapping.get(fk_table, []):
                            postprocessed_row[key] = self.fk_mapping[fk_table][value]
                        else:
                            postprocessed_row[key] = value + self.mapping.last_id
                    # if we're postprocessing an update we should restore the id as well
                    if key == 'id' and table in self.fk_mapping:
                        value = int(value)
                        postprocessed_row[key] = self.fk_mapping[table].get(value, value)
                    # if the record comes from another table, fix it
                    if value in self.moved_records.get(target_record, []):
                        postprocessed_row[key] = self.moved_records[table][value]
                # don't write m2m lines if they exist in the target
                # FIXME: refactor these 4 lines with those from process_one()?
                discriminators = self.mapping.discriminators.get(table)
                existing = existing_records.get(table, [])
                existing_without_id = [{k: v for k, v in nt.iteritems() if k != 'id'}
                                       for nt in existing]
                discriminator_values = {d: postprocessed_row[d] for d in (discriminators or [])}
                if ('id' in postprocessed_row
                        or {k: int(v) for k, v in discriminator_values.iteritems()}
                        not in existing_without_id):
                    self.writers[table].writerow(postprocessed_row)

    def update_one(self, filepath, connection):
        """ Apply updates in the target db with update file
        """
        table = basename(filepath).rsplit('.', 2)[0]
        has_data = False
        with open(filepath, 'rb') as update_csv:
            cursor = connection.cursor()
            reader = csv.DictReader(update_csv, delimiter=',')
            for update_row in reader:
                has_data = True
                items = [(k, v) for k, v in update_row.iteritems() if v != '']
                columns = ', '.join([i[0] for i in items])
                values = ', '.join(['%s' for i in items])
                args = [i[1] for i in items] + [update_row['id']]
                try:
                    cursor.execute('UPDATE %s SET (%s)=(%s) WHERE id=%s'
                                   % (table, columns, values, '%s'), tuple(args))
                    cursor.execute('SAVEPOINT savepoint')
                except Exception, e:
                    LOG.warn('Error updating table %s:\n%s', table, e.message)
                    cursor = connection.cursor()
                    cursor.execute('ROLLBACK TO savepoint')
                    cursor.close()
                    break
            if has_data:
                LOG.info(u'Successfully updated table %s', table)
            else:
                LOG.info(u'Nothing to update in table %s', table)
