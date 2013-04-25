import xmlrpclib
import argparse
import psycopg2

""" Method to find out the dependencies order to import of an OpenERP model
    Set excluded_models to None if there is no model to exclude.
    If you want to exclude some models, use the following syntax :
    excluded_models = ['res.currency', 'res.country']
"""
parser = argparse.ArgumentParser(description='Return the dependencies order'
                                             ' of models give as parameters')

parser.add_argument('-m', '--models', nargs='+', help="One or many models",
                    required=True)
parser.add_argument('-df', '--db_name_from',
                    help="Name of the database to migrate data from",
                    required=True)
parser.add_argument('-dt', '--db_name_to',
		    help="Name of the database to migrate data to",
		    required=False) # Temporary, will be required
parser.add_argument('-ut', '--user_to',
		    help="Name of the user of the database aimed",
		    required=False) # Temporay, will be required
parser.add_argument('-uf', '--user_from',
                    help="Name of the user of the database source",
                    required=True)
parser.add_argument('-pf', '--pwd_from',
                    help="Password of the user of the database source",
                    required=True)
parser.add_argument('-pt', '--pwd_to',
		    help="Password of the user of the database aimed",
		    required=False) # Temporary, will be required
parser.add_argument('-x', '--excluded', nargs='+', help="One or many models"
                    " to exclude", required=False, default=None)

args = parser.parse_args()

username = args.user_from
pwd = args.pwd_from
dbname = args.db_name_from
models = args.models
excluded_models = args.excluded


def get_ordre_importation(username, pwd, dbname, models, excluded_models,
                          path=None, seen=None):
    # XML-RPC
    sock_common = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/common')
    uid = sock_common.login(dbname, username, pwd)
    sock = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/object')
    res = []
    if seen is None:
        seen = set()
    if path is None:
        path = ()
    if excluded_models is not None:
        for excl_model in excluded_models:
            seen.add(excl_model)
        excluded_models = None
    m2o = set()
    m2m = set()
    related_tables = set()
    for model in models:
        seen.add(model)
        fields = sock.execute(dbname, uid, pwd, model, 'fields_get')
        for field in fields:
            if fields[field]['type'] == 'many2one':
                m = fields[field]['relation']
                # Cas des structures arborescentes (reflexives)
                if m in path:
                    print(' BOUCLE %s a un m2o %r vers %s, qui est un de ses '
                          'ancetres (path=%r)' % (model, field, m, path))
                if m not in seen:
                    m2o.add(m)
                    seen.add(m)
            if fields[field]['type'] == 'many2many' and 'related_columns' in fields[field]:
                m = fields[field]['relation']
                third_table = fields[field]['third_table']
                print(" M2M vers %r THIRD_TABLE : %r" % (m,
                      third_table))
                # Cas des structures arborescentes (reflexives)
                if m in path:
                    print(' BOUCLE %s a un m2m %r vers %s, qui est un de ses '
                          'ancetres (path=%r)' % (model, field, m, path))
                if m not in seen:
                    m2m.add(m)
                    seen.add(m)
                if third_table not in seen:
                    related_tables.add(third_table)
                    seen.add(third_table)

        for m in m2m:
            res += get_ordre_importation(username, pwd, dbname, (m,),
                                         path=path+(model,),
                                         excluded_models=excluded_models,
                                         seen=seen)
        for m in m2o:
            res += get_ordre_importation(username, pwd, dbname, (m,),
                                         path=path+(model,),
                                         excluded_models=excluded_models,
                                         seen=seen)

    res.append(model)
    if related_tables:
        for table in related_tables:
            res.append(table)
    return res


def get_mapping_migration(username, pwd, dbname, model):

    #conn = psycopg2.connect(database=dbname, user='fjouatte', password='')
    #cur = conn.cursor()
    sock_common = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/common')
    uid = sock_common.login(dbname, username, pwd)
    sock = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/object')
    for m in model:
	import pdb
	pdb.set_trace()
        #m = m.replace('.', '_')
        records = sock.execute(dbname, uid, pwd, 'ir.model.data', 'search',
                               [('model', '=', m)])
        if records :
		xml_ids = sock.execute(dbname, uid, pwd, 'ir.model.data',
			  	       'read', )
	print(records)
        #select_query = "SELECT * FROM %s;" %m
        #cur.execute(select_query)
        #cur.fetchone()
    #conn.commit()
    #cur.close()
    #conn.close()

get_mapping_migration(username, pwd, dbname, models)
#get_ordre_importation(username, pwd, dbname, models, excluded_models)
