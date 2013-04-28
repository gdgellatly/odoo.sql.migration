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
            for src_column, dest_columns in full_mapping[module].items():
                self.mapping.setdefault(src_column, dest_columns)
                self.mapping[src_column].update(dest_columns)

        # compute the output columns
        out_columns = []
        for values in self.mapping.values():
            out_columns.extend(values.keys())
        self.out_columns = {}
        for column in out_columns:
            table = column.split('.')[0]
            column = column.split('.')[1]
            if table not in self.out_columns:
                self.out_columns[table] = set()
            self.out_columns[table].add(column)

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
