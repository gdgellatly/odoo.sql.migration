from os.path import basename


def import_csv(filepath, connection):
    """ Import the csv file using postgresql COPY
    """
    with connection.cursor() as cursor, open(filepath) as f:
        copy = "COPY %s FROM STDOUT WITH CSV HEADER" % basename(filepath).rsplit('.', 1)[0]
        cursor.copy_expert(copy, f)
