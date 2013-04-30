# coding: utf-8
import yaml


class Mapping(object):
    """ Stores the mapping and offers a simple API
    """
    def __init__(self, modules, filename):
        """ Open the file and store the mapping
        """
        # load the full mapping file
        with open(filename) as stream:
            full_mapping = yaml.load(stream, Loader=yaml.Loader)

        # filter to keep only wanted modules
        self.mapping = {}
        for module in modules:
            if module not in full_mapping:
                raise ValueError('The %s module is not in the mapping' % module)
            for src_column, dest_columns in full_mapping[module].items():
                self.mapping.setdefault(src_column, dest_columns)
                self.mapping[src_column].update(dest_columns)

        # compute the output columns
        dst_columns = []
        for values in self.mapping.values():
            dst_columns.extend(values.keys())
        self.dst_columns = {}
        for column in dst_columns:
            table = column.split('.')[0]
            column = column.split('.')[1]
            if table not in self.dst_columns:
                self.dst_columns[table] = set()
            self.dst_columns[table].add(column)

        # replace function bodies with real functions
        for incolumn in self.mapping:
            for outcolumn in self.mapping[incolumn]:
                mapping = self.mapping[incolumn][outcolumn]
                if mapping is not None:
                    function_body = "def mapping_function(line):\n"
                    function_body += '\n'.join([4*' ' + line for line in mapping.split('\n')])
                    mapping_function = None
                    exec(compile(function_body, '<' + incolumn + ' → ' + outcolumn + '>', 'exec'))
                    self.mapping[incolumn][outcolumn] = mapping_function
                    del mapping_function

    def get_targets(self, source):
        """ Return the target mapping for a column or table
        """
        if '.' in source:  # asked for a column
            return self.mapping.get(source, None)
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
