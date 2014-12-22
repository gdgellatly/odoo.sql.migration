# coding: utf-8
import psycopg2
import yaml
import logging
from os.path import basename
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(basename(__file__))


class Mapping(object):
    """ Stores the mapping and offers a simple API
    """

    max_target_id = {}
    max_source_id = {}
    new_id = {}
    target_connection = None
    source_connection = None
    fk2update = None

    def __init__(self, modules, filenames, drop_fk=False):
        """ Open the file and compute the mapping
        """
        self.target_tables = []
        self.fk2update = {}
        full_mapping = {} # ends up as {'module': {'table': {'column': v}}}
        # load the full mapping file
        if isinstance(filenames, str):
            filenames = [filenames]
        for filename in filenames:
            with open(filename) as stream:
                full_mapping.update(yaml.load(stream))
        # filter to keep only wanted modules
        self.mapping = {}
        self.deferred = {}
        for module in modules:
            if module not in full_mapping: # skip modules not in YAML files
                LOG.warn('Mapping is not complete: module "%s" is missing!', module)
                continue
            elif full_mapping[module] == '__nothing_to_do__':
                del full_mapping[module]
                continue
            for source_column, target_columns in full_mapping[module].items():
            #here we are going over entire dictionary for module, by module, can't we just merge to table dict now
                if '__' in source_column:
                    # skip special markers
                    continue
                if (target_columns in ('__forget__', False) #if it needs forgetting
                        or self.mapping.get(source_column) == '__forget__'):
                    self.mapping[source_column] = '__forget__'
                    continue
                if target_columns is None:
                    target_columns = {}
                try:
                    self.mapping.setdefault(source_column, target_columns)
                    self.mapping[source_column].update(target_columns)
                except:
                    raise ValueError('Error in the mapping file: "%s" is invalid here'
                                     % repr(target_columns))
        # replace function bodies with real functions
        for incolumn in self.mapping:
            targets = self.mapping[incolumn]
            if targets in (False, '__forget__'):
                self.mapping[incolumn] = {}
                continue
            for outcolumn, function in targets.items():
                if function in ('__copy__', '__moved__', None):
                    continue
                if function == '__defer__':
                    self.mapping[incolumn][outcolumn] = '__copy__'
                    if not drop_fk:
                        table, column = outcolumn.split('.')
                        self.deferred.setdefault(table, set())
                        self.deferred[table].add(column)
                    continue
                if function.startswith('__fk__ '):
                    if len(function.split()) != 2:
                        raise ValueError('Error in the mapping file: "%s" is invalid in %s'
                                         % (repr(function), outcolumn))
                    self.fk2update[outcolumn] = function.split()[1]
                    self.mapping[incolumn][outcolumn] = '__copy__'
                    continue
                if function.startswith('__ref__'):
                    if len(function.split()) != 2:
                        raise ValueError('Error in the mapping file: "%s" is invalid in %s'
                                         % (repr(function), outcolumn))
                    # we handle that in the postprocess
                    self.mapping[incolumn][outcolumn] = function
                    continue
                function_body = "def mapping_function(self, source_row, target_rows):\n"
                if type(function) is not str:
                    raise ValueError('Error in the mapping file: "%s" is invalid in %s'
                                     % (repr(function), outcolumn))
                function_body += '\n'.join([4*' ' + line for line in function.split('\n')])
                mapping_function = None
                exec(compile(function_body, '<' + incolumn + ' → ' + outcolumn + '>', 'exec'),
                     globals().update({
                         'newid': self.newid,
                         'sql': self.sql}))
                self.mapping[incolumn][outcolumn] = mapping_function
                del mapping_function

        # build the discriminator mapping
        self.discriminators = {}
        for mapping in full_mapping.values():
            self.discriminators.update({
                key.split('.')[0]: value
                for key, value in mapping.items()
                if '__discriminator__' in key})

    def newid(self, target_table):
        """ increment the global stored new_id for table
        This method is available as a function in the mapping
        """
        self.new_id[target_table] += 1
        return self.new_id[target_table]

    def sql(self, db, sql, args=()):
        """ execute an sql statement in the target db and return the value
        This method is available as a function in the mapping
        """
        assert db in ('source', 'target'), u"First arg of sql() should be 'source' or 'target'"
        connection = self.target_connection if db == 'target' else self.source_connection
        with connection.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchall() if 'select ' in sql.lower() else ()

    def get_target_column(self, source, column):
        """ Return the target mapping for a column or table
        """
        # Refactor as never called without column
        tbl_col = source + '.' + column
        mapping = self.mapping.get(tbl_col, None)
        # not found? We look for wildcards
        if mapping is None:
            # wildcard, we match the source

            # partial wildcard, we match only for the table
            partial_pattern = '%s.*' % source
            if partial_pattern in self.mapping:
                if self.mapping[partial_pattern]:
                    return {k.replace('*', column): v
                            for k, v in self.mapping[partial_pattern].items()}
                return {tbl_col: None}
            elif '.*' in self.mapping:
                return {tbl_col: None}
        return mapping

    def get_target_table(self, source):
        """
        Not currently used anywhere but left in case
        Seems a bad use of class
        :param source:
        :return:
        """
        target_tables = set()
        target_fields = [t[1] for t in self.mapping.items() if t[0].split('.')[0] == source]
        for f in target_fields:
            target_tables.update([c.split('.')[0] for c in f.keys()])
        self.target_tables = list(target_tables)
        return self.target_tables

    def get_sources(self, target):
        """ Return the source tables given a target table
        """
        return sorted(list({t[0].split('.')[0]
                            for t in self.mapping.items()
                            if target in [c.split('.')[0]
                                          for c in type(t[1]) is dict and t[1].keys() or ()]}))

    def set_database_ids(self, source_tables, source_connection,
                       target_tables, target_connection):
        """ create mapping of the max id with
        max of source and target dbs
        """
        self.target_connection = target_connection
        self.source_connection = source_connection
        for source_table in source_tables:
            with source_connection.cursor() as c:
                # FIXME the key (id) shouldn't be hardcoded below
                try:
                    c.execute('select max(id) from %s' % source_table)
                    maxid = c.fetchone()
                    maxid = maxid and maxid[0]
                    self.max_source_id[source_table] = maxid or 0
                except psycopg2.ProgrammingError:
                    # id column does not exist
                    source_connection.rollback()
        for target_table in target_tables:
            with target_connection.cursor() as c:
                # FIXME the key (id) shouldn't be hardcoded below
                try:
                    c.execute('select max(id) from %s' % target_table)
                    maxid = c.fetchone()
                    maxid = maxid and maxid[0] or 0
                    self.max_target_id[target_table] = maxid
                    self.new_id[target_table] = maxid + self.max_source_id.get(target_table, 0)
                except psycopg2.ProgrammingError:
                    # id column does not exist
                    target_connection.rollback()

    def update_database_sequences(self, target_conn):
        with target_conn.cursor as t:
            for table in self.max_target_id.iterkeys():
                try:
                    t.execute("SELECT 1 from pg_class "
                              "WHERE relname=%s || '_id_seq'", tuple(table,))
                    res = t.fetchone()
                    if res and res[0]:
                        t.execute("UPDATE %s_id_seq SET last_value = "
                                  "(SELECT max(id) from %s);" % (table, table))
                except psycopg2.ProgrammingError:
                    target_conn.rollback()

