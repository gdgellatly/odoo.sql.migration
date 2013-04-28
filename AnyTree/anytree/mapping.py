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
            self.mapping.update(full_mapping[module])

    def get_targets(self, source):
        """ Return the target mapping for a column or table
        """
        if '.' in source:  # asked for a column
            pass
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
