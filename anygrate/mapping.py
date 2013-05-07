# coding: utf-8
import yaml


class Mapping(object):
    """ Stores the mapping and offers a simple API
    """

    last_id = {}

    def __init__(self, modules, filename):
        """ Open the file and store the mapping
        """
        # load the full mapping file
        with open(filename) as stream:
            full_mapping = yaml.load(stream)
        # filter to keep only wanted modules
        self.mapping = {}
        for module in modules:
            if module not in full_mapping:
                raise ValueError('The %s module is not in the mapping' % module)
            for source_column, target_columns in full_mapping[module].items():
                target_columns = target_columns or {}
                self.mapping.setdefault(source_column, target_columns)
                self.mapping[source_column].update(target_columns)
        # replace function bodies with real functions
        for incolumn in self.mapping:
            for outcolumn in self.mapping[incolumn]:
                mapping = self.mapping[incolumn][outcolumn]
                if mapping is not None:
                    function_body = "def mapping_function(source_row, target_rows):\n"
                    function_body += '\n'.join([4*' ' + line for line in mapping.split('\n')])
                    mapping_function = None
                    exec(compile(function_body, '<' + incolumn + ' → ' + outcolumn + '>', 'exec'),
                         globals().update({'newid': self.newid}))
                    self.mapping[incolumn][outcolumn] = mapping_function
                    del mapping_function
    def newid(self, table):
        """ increment the global stored last_id
        """
        self.last_id.setdefault(table, 0)
        self.last_id[table] += 1
        return self.last_id[table]

    def get_targets(self, source):
        """ Return the target mapping for a column or table
        """
        if '.' in source:  # asked for a column
            table, column = source.split('.')
            mapping = self.mapping.get(source, None)
            # not found? We look for wildcards
            if mapping is None:
                # wildcard, we match the source
                if '.*' in self.mapping:
                    return {source: None}
                # partial wildcard, we match only for the table
                partial_pattern = '%s.*' % table
                if partial_pattern in self.mapping:
                    if self.mapping[partial_pattern]:
                        return {k.replace('*', column): v
                                for k, v in self.mapping[partial_pattern].items()}
                    return {source: None}
            return mapping

        else:  # asked for a table
            self.target_tables = set()
            target_fields = [t[1] for t in self.mapping.items() if t[0].split('.')[0] == source]
            for f in target_fields:
                self.target_tables.update([c.split('.')[0] for c in f.keys()])
            self.target_tables = list(self.target_tables)
            return self.target_tables

    def get_sources(self, target):
        """ Return the source tables given a target table
        """
        return sorted(list({t[0].split('.')[0] for t in self.mapping.items()
                            if target in [c.split('.')[0] for c in t[1].keys()]}))
