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
                    required=False)  # Temporary, will be required
parser.add_argument('-ut', '--user_to',
                    help="Name of the user of the database aimed",
                    required=False)  # Temporay, will be required
parser.add_argument('-uf', '--user_from',
                    help="Name of the user of the database source",
                    required=True)
parser.add_argument('-pf', '--pwd_from',
                    help="Password of the user of the database source",
                    required=True)
parser.add_argument('-pt', '--pwd_to',
                    help="Password of the user of the database aimed",
                    required=False)  # Temporary, will be required
parser.add_argument('-x', '--excluded', nargs='+', help="One or many models"
                    " to exclude", required=False, default=None)

args = parser.parse_args()

username_from = args.user_from
username_to = args.user_to
pwd_from = args.pwd_from
pwd_to = args.pwd_to
dbname_from = args.db_name_from
dbname_to = args.db_name_to
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


def get_mapping_migration(username_from, username_to, pwd_from, pwd_to,
                          dbname_from, dbname_to, model):
    sock_common_from = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/common')
    uid_from = sock_common_from.login(dbname_from, username_from, pwd_from)
    sock_from = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/object')
    sock_common_to = xmlrpclib.ServerProxy('http://localhost:8169/xmlrpc/common')
    uid_to = sock_common_to.login(dbname_to, username_to, pwd_to)
    sock_to = xmlrpclib.ServerProxy('http://localhost:8169/xmlrpc/object')
    for m in model:
        records_source = sock_from.execute(dbname_from, uid_from, pwd_from,
                                           'ir.model.data', 'search',
                                           [('model', '=', m)])
        if records_source:
            xml_ids_source = sock_from.execute(dbname_from, uid_from, pwd_from,
                                               'ir.model.data',
                                               'read', records_source, ['name', 'res_id'])
            if xml_ids_source:
                records_cible = []
                for xml_id in xml_ids_source:
                    records_cible += sock_to.execute(dbname_to, uid_to, pwd_to,
                                                     'ir.model.data',
                                                     'search',
                                                     [('name', '=',
                                                     xml_id['name'])])

        if records_cible:
            xml_ids_cible = sock_to.execute(dbname_to, uid_to, pwd_to,
                                            'ir.model.data',
                                            'read', records_cible,
                                            ['name', 'res_id'])

            for xml_id_cible in xml_ids_cible:
                for xml_id_source in xml_ids_source:
                    if xml_id_cible['res_id'] ==  xml_id_source['res_id']:
                        print('XML ID CORRESPONDANT')
                    else:
                        print('RES_ID DIFFERENT')

    print(xml_ids_source)
    print
    print(xml_ids_cible)

get_mapping_migration(username_from, username_to, pwd_from, pwd_to, dbname_from, dbname_to, models)
#get_ordre_importation(username, pwd, dbname, models, excluded_models)
