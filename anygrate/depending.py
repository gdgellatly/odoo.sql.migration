import xmlrpclib
import argparse
import logging
from os.path import basename
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(basename(__file__))


def main():
    """ Main console script
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
    #args = parser.parse_args()
    #username_from = args.user_from
    #username_to = args.user_to
    #pwd_from = args.pwd_from
    #pwd_to = args.pwd_to
    #dbname_from = args.db_name_from
    #dbname_to = args.db_name_to
    #models = args.models
    #excluded_models = args.excluded
    #ordered_models = get_dependencies(username_from,
    #                                       pwd_from,
    #                                       dbname_from,
    #                                       models,
    #                                       excluded_models)


if __name__ == '__main__':
    main()


def add_related_tables_to_tables_dependencies(target_connection, tables,
                                              excluded_tables, path=None,
                                              seen=None, related_tables=None):
    res, related_tables = get_sql_dependencies(target_connection,
                                               tables, excluded_tables)
    for tbl in related_tables:
        res.append(tbl)
    return res


def add_related_tables_to_models_dependencies(username, pwd, dbname, models,
                                              excluded_models, path=None,
                                              seen=None, related_tables=None):

    res, related_tables = get_dependencies(username, pwd, dbname, models,
                                           excluded_models)
    for tbl in related_tables:
        res.append(tbl)
    return res


def get_sql_dependencies(target_connection, tables, excluded_tables,
                         path=None, seen=None, related_tables=None):
    """ Given a list of PSQL tables, return the full list of dependant tables,
    ordered by dependencies. Warning are displayed if there are dependency loops
    Set excluded_models to None if there is no table to exclude.
    If you want to exclude some tables, use the following syntax :
    excluded_tables = ['res.currency', 'res.country']
    """
    res = []
    if seen is None:
        seen = set()
    if path is None:
        path = ()
    if excluded_tables is not None:
        for excl_table in excluded_tables:
            seen.add(excl_table)
        excluded_tables = None
    third_tables = set()
    potentials_m2m = set()
    related_tables = set()
    for table in tables:
        with target_connection.cursor() as c:
            m2o = set()
            m2m = set()
            seen.add(table)
            # Requete permettant de recuperer les tables referencees
            # par les cle etrangeres de la table passee en entree
            query_fk = """
  SELECT pg_cl_2.relname as related_table
  FROM pg_class pg_cl_1, pg_class pg_cl_2, pg_constraint,
  pg_attribute pg_attr_1,
  pg_attribute pg_attr_2 WHERE pg_cl_1.relname = '%s'
  and pg_constraint.conrelid = pg_cl_1.oid
  AND pg_cl_2.relkind = 'r' AND pg_cl_2.oid = pg_constraint.confrelid
  AND pg_attr_1.attnum = pg_constraint.confkey[1]
  AND pg_attr_1.attrelid = pg_cl_2.oid
  AND pg_attr_2.attnum = pg_constraint.conkey[1]
  AND pg_attr_2.attrelid = pg_cl_1.oid;
  """ % table

            c.execute(query_fk)
            results_fk = c.fetchall()
            # Requete permettant de recuperer toutes les tables referencant
            # la table passee en entree
            query_table_ref = """
  SELECT tc.table_name
  FROM information_schema.table_constraints AS tc JOIN
  information_schema.key_column_usage AS kcu ON
  tc.constraint_name = kcu.constraint_name JOIN
  information_schema.constraint_column_usage AS
  ccu ON ccu.constraint_name = tc.constraint_name
  WHERE constraint_type = 'FOREIGN KEY' AND
  ccu.table_name='%s';""" % table
            c.execute(query_table_ref)
            results_ref = c.fetchall()
            if results_fk:
                for fk in results_fk:
                    # Cas des structures arborescentes (reflexives)
                    tbl = fk[0]
                    if tbl in path:
                        LOG.warn('Dependency LOOP: '
                                 '%s has a m2o to %s which is one of its ancestors (path=%r)',
                                 table, tbl, path)
                    if tbl not in seen:
                        m2o.add(tbl)
                        seen.add(tbl)
            # Premier ecremage, on vire les doublons
            # On considere qu'il ya des tables de jointures ici
            # NOT REALLY EFFICIENT
            diff_tables = set(results_ref).difference(set(results_fk))
            if diff_tables:
                for t in diff_tables:
                    # Requete permettant de trouver les tables referencees
                    # dans une table de jointure donnee
                    query_third_tables = """
SELECT information_schema.constraint_table_usage.table_name
FROM information_schema.table_constraints, information_schema.constraint_table_usage
WHERE information_schema.table_constraints.table_name = '%s'
AND information_schema.constraint_table_usage.constraint_name \
= information_schema.table_constraints.constraint_name;""" % t
                    c.execute(query_third_tables)
                    third_tables = c.fetchall()
                    if third_tables:
                        for third_table in third_tables:
                            tbl = third_table[0]
                            if tbl not in seen:
                                seen.add(tbl)
                            if tbl not in related_tables:
                                related_tables.add(tbl)
            if potentials_m2m:
                for tbl in potentials_m2m:
                    if tbl in path:
                        LOG.warn('Dependency LOOP: '
                                 '%s has a m2m to %s which is one of its ancestors (path=%r)',
                                 table, tbl, path)
                    if tbl not in seen:
                        m2m.add(tbl)
        for t in m2m:
            results, related_tables = get_sql_dependencies(
                target_connection, (t,),
                path=path+(table,),
                excluded_tables=excluded_tables,
                seen=seen, related_tables=related_tables)
            res += results
        for t in m2o:
            results, related_tables = get_sql_dependencies(
                target_connection, (t,),
                path=path+(table,),
                excluded_tables=excluded_tables,
                seen=seen, related_tables=related_tables)
            res += results
        #if model == 'ir.actions.actions':
        #    model = 'ir.actions'
        if table not in res:
            res.append(table)
    return res, related_tables


def get_dependencies(username, pwd, dbname, models, excluded_models,
                     path=None, seen=None, related_tables=None):
    """ Given a list of OpenERP models, return the full list of dependant models,
    ordered by dependencies. Warning are displayed if there are dependency loops
    Set excluded_models to None if there is no model to exclude.
    If you want to exclude some models, use the following syntax :
    excluded_models = ['res.currency', 'res.country']
    """
    # XML-RPC
    sock, uid = get_socket(username, pwd, dbname, 8069)
    res = []
    if seen is None:
        seen = set()
    if path is None:
        path = ()
    if excluded_models is not None:
        for excl_model in excluded_models:
            seen.add(excl_model)
        excluded_models = None
    if related_tables is None:
        related_tables = set()
    for model in models:
        m2o = set()
        m2m = set()
        seen.add(model)
        fields = sock.execute(dbname, uid, pwd, model, 'fields_get')
        for field in fields:
            if fields[field]['type'] == 'many2one':
                m = fields[field]['relation']
                # Cas des structures arborescentes (reflexives)
                if m in path:
                    LOG.warn('Dependency LOOP: '
                             '%s has a m2o %r to %s which is one of its ancestors (path=%r)',
                             model, field, m, path)
                if m not in seen:
                    m2o.add(m)
                    seen.add(m)
            if fields[field]['type'] == 'many2many' and 'related_columns' in fields[field]:
                m = fields[field]['relation']
                third_table = fields[field]['third_table']
                # Cas des structures arborescentes (reflexives)
                if m in path:
                    LOG.warn('Dependency LOOP: '
                             '%s has a m2m %r to %s which is one of its ancestors (path=%r)',
                             model, field, m, path)
                if m not in seen:
                    m2m.add(m)
                    seen.add(m)
                if third_table not in seen:
                    seen.add(third_table)
                if third_table not in related_tables:
                    related_tables.add(third_table)
        for m in m2m:
            result, related_tables = get_dependencies(
                username, pwd, dbname, (m,),
                path=path+(model,),
                excluded_models=excluded_models,
                seen=seen, related_tables=related_tables)
            res += result
        for m in m2o:
            result, related_tables = get_dependencies(
                username, pwd, dbname, (m,),
                path=path+(model,),
                excluded_models=excluded_models,
                seen=seen, related_tables=related_tables)
            res += result
        if model == 'ir.actions.actions':
            model = 'ir.actions'
        if model not in res:
            res.append(model)
    return res, related_tables


def get_fk_to_update(connection, models):
    """ Method to get back all columns referencing another table
    """
    fields2update = {}
    for model in models:
        with connection.cursor() as c:
            if model not in fields2update:
                if model == 'ir.actions':
                    model = 'ir.actions.actions'
                model = model.replace('.', '_')
                query = """
SELECT tc.table_name, kcu.column_name
FROM information_schema.table_constraints AS tc JOIN
information_schema.key_column_usage AS kcu ON
tc.constraint_name = kcu.constraint_name JOIN
information_schema.constraint_column_usage AS
ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY' AND
ccu.table_name='%s';""" % model
                c.execute(query)
                results = c.fetchall()
                fields2update[model] = results
    # transpose the result to obtain:
    # {'table.fkname': 'pointed_table', ...}
    # so that processing each input line is easier
    result = {}
    for pointed_table, fknames in fields2update.iteritems():
        for fkname in fknames:
            result['.'.join(fkname)] = pointed_table
    return result


def get_mapping_migration(username_from, username_to, pwd_from, pwd_to,
                          dbname_from, dbname_to, model):
    """ Method to define which record needs to be update or not before importing it
    """
    sock_from, uid_from = get_socket(username_from, pwd_from, dbname_from, 8069)
    sock_to, uid_to = get_socket(username_to, pwd_to, dbname_to, 8169)
    mapping_xml_id = {}
    mapping_list = []
    for m in model:

        records_source = sock_from.execute(dbname_from, uid_from, pwd_from,
                                           'ir.model.data', 'search',
                                           [('model', '=', m)])
        if records_source:
            for r in records_source:
                xml_id_source = get_xml_id_source(r, username_from, pwd_from,
                                                  dbname_from, m)

                xml_id_destination = get_xml_id_destination(xml_id_source,
                                                            username_to, pwd_to,
                                                            dbname_to, m)
                if xml_id_destination:
                    if (xml_id_source['name'] == xml_id_destination['name']
                            and xml_id_source['id'] != xml_id_destination['id']):
                        data = {
                            'xml_id': xml_id_source['name'],
                            'res_id_source': xml_id_source['id'],
                            'res_id_destination': xml_id_destination['id'],
                        }
                        mapping_list.append(data)
                else:
                    print('XML_ID NOT FOUND')
        mapping_xml_id[m] = mapping_list


def get_destination_id(source_id, username_from, username_to, pwd_from, pwd_to,
                       dbname_from, dbname_to, model):

    sock_from, uid_from = get_socket(username_from, pwd_from, dbname_from, 8069)
    sock_to, uid_to = get_socket(username_to, pwd_to, dbname_to, 8169)

    id_model_data = get_xml_id_source(source_id, username_from, username_to,
                                      pwd_from, pwd_to, dbname_from,
                                      dbname_to, model)
    if id_model_data:

        destination_id = sock_to.execute(dbname_to, uid_to, pwd_to,
                                         'ir.model.data', 'read',
                                         id_model_data, ['res_id'])
        return destination_id
    return None


def get_xml_id_source(source_id, username_source, pwd_source,
                      dbname_from, model):

    sock_from, uid_from = get_socket(username_source, pwd_source, dbname_from, 8069)
    xml_id_source = sock_from.execute(dbname_from, uid_from, pwd_source,
                                      'ir.model.data', 'read',
                                      source_id, ['name'])
    if xml_id_source:
        return xml_id_source
    else:
        return None


def get_xml_id_destination(xml_id_source, username_to, pwd_to, dbname_to,
                           model):

    sock_to, uid_to = get_socket(username_to, pwd_to, dbname_to, 8169)
    xml_id_source = xml_id_source['name']
    id_model_data = sock_to.execute(dbname_to, uid_to, pwd_to,
                                    'ir.model.data', 'search',
                                    [('name', '=', xml_id_source)])
    if id_model_data:
        xml_id_data = sock_to.execute(dbname_to, uid_to, pwd_to,
                                      'ir.model.data',
                                      'read', id_model_data,
                                      ['name'])
        return xml_id_data[0]
    return None


def get_socket(username, pwd, dbname, port):

    str_common = 'http://localhost:%s/xmlrpc/common' % port
    str_object = 'http://localhost:%s/xmlrpc/object' % port
    sock_common = xmlrpclib.ServerProxy(str_common)
    uid = sock_common.login(dbname, username, pwd)
    sock = xmlrpclib.ServerProxy(str_object)
    return sock, uid


#get_dependencies(username, pwd, dbname, models, excluded_models)
