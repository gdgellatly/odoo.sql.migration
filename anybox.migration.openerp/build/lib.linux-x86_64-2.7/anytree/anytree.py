import xmlrpclib

#def get_ordre_import(models):
#    ordre_models = []
#    for model in models:
#        if ordre_models.count(model) == 0:
#            ordre_models.append(model)
#            fields = sock.execute(dbname, uid, pwd, model, 'fields_get')
#            for field in fields:
#                if(fields[field]['type'] == 'many2one'):
#                    related_model = fields[field]['relation']
#                    if ordre_models.count(related_model) == 0:
#                        ordre_models.append(related_model)
#                    if models.count(related_model) == 0:
#                        models.append(related_model)
#    return ordre_models

#   ordre_models = get_ordre_import(models)

""" Method to find out the dependencies tree of the models """


def get_ordre_importation(username, pwd, dbname, model, seen):

    sock_common = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/common')
    uid = sock_common.login(dbname, username, pwd)
    sock = xmlrpclib.ServerProxy('http://localhost:8069/xmlrpc/object')

    res = []
    if seen is None:
        seen = set()
    seen.add(model)
    m2o = set()
    deps = set()
    fields = sock.execute(dbname, uid, pwd, model, 'fields_get')
    for field in fields:
        if fields[field]['type'] == 'many2one':
            m = fields[field]['relation']
            deps.add(m)
            # Cas des structures arborescentes (reflexives)
            if m not in seen:
                m2o.add(m)
                seen.add(m)
    for m in m2o:
        res += get_ordre_importation(m, seen)
    res.append(model)
    return res
