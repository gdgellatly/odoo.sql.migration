from os.path import basename


def import_from_csv(filepaths, connection):
    """ Import the csv file using postgresql COPY
    """
    for filepath in filepaths:
        with connection.cursor() as cursor, open(filepath) as f:
            copy = "COPY %s FROM STDOUT WITH CSV HEADER" % basename(filepath).rsplit('.', 1)[0]
            cursor.copy_expert(copy, f)
