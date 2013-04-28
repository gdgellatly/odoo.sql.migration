import csv


class CSVProcessor(object):
    """ Take a csv file, process it with the mapping
    and output a new csv file
    """
    def __init__(self, mapping):
        self.mapping = mapping

    def process(self, in_filename):
        table = in_filename.rsplit('.', 1)[0]
        out_columns = self.mapping.out_columns
        out_filename = table + '.out.csv'
        with open(in_filename, 'rb') as in_csv, open(out_filename, 'wb') as out_csv:
            reader = csv.DictReader(in_csv, delimiter=',')
            writer = csv.DictWriter(out_csv, delimiter=',')
            for inrow in reader:
                for column in inrow:
                    outrow = {}
                    outrow
                writer.writerow(outrow)
