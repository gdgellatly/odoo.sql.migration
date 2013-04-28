import psycopg2
from os.path import basename


def import_csv(filepath, db="test"):
    """ Import the csv file using postgresql COPY
    """
    connection = psycopg2.connect("dbname=%s" % db)
    with connection.cursor() as cursor, open(filepath) as f:
        copy = "COPY %s FROM STDOUT WITH CSV HEADER" % basename(filepath).rsplit('.', 1)[0]
        cursor.copy_expert(copy, f)
