# coding: utf8

from gluon.custom_import import track_changes; track_changes(True)
from plugin_lookout import IS_VALID_SQL_TABLE_NAME

from gluon.storage import Storage

'''
permissions = dict(
    lettura/scrittura a utenti di un gruppo di appartenenza o tutti
    * dato condiviso (sì/no)
    * con chi (utenti loggati o uno specifico gruppo tra quelli cui si appartiene)
)

connessioni private ma gestite in modo da non creare doppioni di oggetti
'''

################################################################## CONNECTIONS #
from random import randint
db.define_table('plugin_lookout_connections',
    Field('alias', readable=False, required=True, writable=False),
    Field('dsn'), #  requires=IS_EXPR('value.count("%s")==1')
    Field('pwd', 'password', readable=False),
    Field('is_active', 'boolean', default=True),
#    Field('is_shared', 'boolean', default=False),
    auth.signature.created_by,
    format = '%(alias)s' # lambda r: r.dsn.replace('%s', randint(8,16)*'*')
)

db.plugin_lookout_connections.alias.requires = IS_NOT_IN_DB(db, 'plugin_lookout_connections.alias')
#db.plugin_lookout_connections.dsn.requires = IS_NOT_IN_DB(db, 'plugin_lookout_connections.dsn')
f_max = db.plugin_lookout_connections.id.max()
id_max = db().select(f_max).first()[f_max] or 0
db.plugin_lookout_connections.alias.default = 'db_%s' % (int(id_max) + 1)

def define_dbs():
    plugin_lookout_dbs = Storage()
    for conn in db(db.plugin_lookout_connections.is_active==True).select():
        if conn.dsn.count('%s')==1:
            dsn = conn.dsn % conn.pwd
        else:
            dsn = conn.dsn
        try:
            plugin_lookout_dbs[conn.alias] = DAL(dsn)
        except Exception, error:
            pass
    return plugin_lookout_dbs

for k,v in define_dbs().items():
    if k not in globals().keys() + ['db']:
        exec('%s=v' % k)


####################################################################### TABLES #

#def wich_db(r):
#    if r.connection:
#        mydb = globals()[db(db.plugin_lookout_connections.id==r.vars.connection)\
#            .select(db.plugin_lookout_connections.alias).first().alias]
#        return mydb
#    else:
#        return db

db.define_table('plugin_lookout_tables',
    Field('table_name', label=T('Table name'), required=True,
        ondelete='CASCADE'
    ),
    Field('table_migrate', 'boolean', default=False, readable=False, writable=False,
        label='Migrate', comment=T('Create the table?')),
    Field('table_singular', label=T('Singular')),
    Field('table_plural', label=T('Plural')),
    Field('restricted', 'boolean', default=False),
    Field('connection', db.plugin_lookout_connections, required=True,
        requires = IS_IN_DB(db(db.plugin_lookout_connections.created_by==auth.user_id), 'plugin_lookout_connections.id', '%(dsn)s')
    ),
    Field('is_active', 'boolean', default=True, label=T('Active'), comment=T('Let the table be recognized from db?')),
    Field('is_view', 'boolean', label=T('View'), default=False, writable=False, readable=False),
    auth.signature.created_by,
    format='%(tab_name)s',
    singular="Tabella", plural="Tabelle"
)
#db.plugin_lookout_tables.table_name.requires = IS_VALID_SQL_TABLE_NAME(wich_db(request), check_reserved=('common', 'postgres', ))


####################################################################### FIELDS #

field_types = [('string', 'String'),
    ('integer', 'Integer'),
    ('double', 'Double'),
    ('boolean', 'Boolean (i.e. only True or False)'),
    ('date', 'Date'),
    ('datetime', 'Date with time'),
    ('time', 'Time'),
    ('id', 'Id'),
    ('geometry', 'Geometry'),
    ('geography', 'Geography')]

db.define_table('plugin_lookout_fields',
    Field('tables', 'list:reference db.plugin_lookout_tables', label=T('Table name'),
        required=True, requires=IS_IN_DB(db, 'plugin_lookout_tables.id', '%(table_name)s', multiple=True)),
    Field('field_name', label=T('Field name'), required=True,
        requires=IS_MATCH('^[a-z0-9_]*$', error_message='Nome campo non valido.')),
    Field('field_type', label=T('Field type'), comment=T('default: "string"'),
        requires=IS_EMPTY_OR(IS_IN_SET(field_types))),
    Field('field_format', length=25, label=T('Format'),
        comment = T('Date/time format (Optional. Only for date and datetime field type)')),
#            'Formato data/ora (es: "%H:%M:%S - %d/%m/%Y"). \
#            Utile SOLO nei casi di dato un formato date, datetime o time. \
#            Attenzione! Se la tabella esiste in database il formato deve \
#            rispettare quello in uso. Verificare nei dati esistenti.'),
    Field('field_length', 'integer', label=T('Length'),
        comment=T('Field lenght')),
    Field('field_label', label=T('Label')),
    Field('field_comment', label=T('Comment'))
)

#def setup_permission(grop_id, name, table_name):
#    where = (db.auth_permission.group_id==group_id)\
#        &(db.auth_permission.name==name)\
#        &(db.auth_permission.tab_name==table_name)
#    res = db(where).select()
#    if not res:
#        auth.add_permission(group_id, name, table_name)

def define_tables(fake_migrate=False):
    try:
        import ppygis
    except:
        geom_representation = lambda value: value
    else:
        geom_representation = lambda value: str(ppygis.Geometry.read_ewkb(value))

    join = db.plugin_lookout_tables.connection == db.plugin_lookout_connections.id
    where = (db.plugin_lookout_tables.is_active==True)&(db.plugin_lookout_connections.is_active==True)
    res_tables = db(join&where).select(db.plugin_lookout_connections.ALL, db.plugin_lookout_tables.ALL)
    res_fields = db(db.plugin_lookout_fields).select(orderby=db.plugin_lookout_fields.tables)

    translate = dict(
        field_type = 'type',
        field_label = 'label',
        field_length = 'length',
        field_comment = 'comment'
    )

    validators = dict(
        date = IS_DATE,
        datetime = IS_DATETIME,
        time = IS_TIME
    )

    for rec_table in res_tables:
        field_list = list()
        geoms = dict(geometry=[], geography=[])
        for rec_field in res_fields.find(lambda row: rec_table.plugin_lookout_tables.id in row.tables):

            # DUCK DEBUG
            #+ n_r_f: Not Required Fields. Ovvero campi non obbligatori
            n_r_f = [i for i in rec_field.as_dict() if not db.plugin_lookout_fields[i].required]
            kwargs = dict([(translate[k],rec_field[k]) for k in n_r_f if rec_field[k] not in ('', None, ) and translate.get(k)])

            if rec_field.field_type in ('date', 'datetime', 'time', ) and rec_field.field_format:
                kwargs['requires'] = IS_EMPTY_OR(validators.get(rec_field.field_type)(format=rec_field.field_format))
            elif rec_field.field_type in ('geometry', 'geography', ): 
                if not hasattr(Field, 'st_asgeojson'):
                    geoms[rec_field.field_type].append(rec_field.field_name)
                    kwargs['type'] = 'text'
                    kwargs['writable'] = False # if unsupported geometryes are managed as visible only text record
                    kwargs['represent'] = lambda value,row: '%s ...' % geom_representation(value)[:50]
            field_list.append(Field(rec_field.field_name, **kwargs))

        mydb = globals()[rec_table.plugin_lookout_connections.alias]
#        import ipdb; ipdb.set_trace()
        table = mydb.Table(mydb, rec_table.plugin_lookout_tables.table_name, *field_list)

        if not hasattr(mydb, rec_table.plugin_lookout_tables.table_name.lower()):
            t_kwargs = dict([(k,rec_table.plugin_lookout_tables['table_%s' %k]) for k in ('singular', 'plural', ) if rec_table.plugin_lookout_tables['table_%s' %k] not in ('', None, )])

            mydb.define_table(rec_table.plugin_lookout_tables.table_name,
                migrate=rec_table.plugin_lookout_tables.table_migrate, fake_migrate=fake_migrate,
                *field_list,
                **t_kwargs
            )
            if rec_table.plugin_lookout_tables.table_migrate:
                for k,v in geoms.items():
                    for i in v:
                        r = mydb.executesql("select data_type from information_schema.columns where table_name='%s' AND column_name='%s'" % (rec_table.plugin_lookout_tables.tab_name, i))
                        if r[0][0] != 'text':
                            mydb.executesql('ALTER TABLE %s ALTER COLUMN %s TYPE %s;' % (t.tab_name, i, k))

define_tables()

def get_table_restriction(table_id, action='w'):
    rec_table = db(db.plugin_lookout_tables.id==table_id).select().first()
    if rec_table:
        t_hash = '%s_%s' % (globals()[rec_table.connection.alias]._uri_hash, rec_table.table_name)
    else:
        return False
    if rec_table.restricted:
        if action=='w':
            role = 'write_%s' % t_hash
        elif action=='r':
            role = 'read_%s' % t_hash
        else:
            return False
        return auth.has_membership(user_id=auth.user_id, role=role)
    else:
        return True

def set_ownership(table_name):
    for row in db(db[table_name]).select():
        owner = row.created_by
        for irow in db(db[table_name].created_by==owner)\
            .select():
            restricted = irow.get('restricted') or irow.restricted
            if restricted:
                if not auth.has_permission(table_name=table_name,
                    user_id=owner,
                    record_id=irow.id):
                    group_id = db(db.auth_group.role=='user_%s' % owner)\
                        .select(db.auth_group.id).first().id
                    auth.add_permission(table_name=table_name,
                        group_id=group_id,
                        record_id=irow.id)
                        
#set_ownership('plugin_lookout_connections')
#set_ownership('plugin_lookout_tables')


def clean_permission():
    for table_name in ('plugin_lookout_connections', 'plugin_lookout_tables', ):
        ids = db(db[table_name])._select(db[table_name].id)
        db((db.auth_permission.table_name==table_name)&(~db.auth_permission.record_id.belongs(ids))).delete()
clean_permission()



response.menu+=[
    (T('plugin lookout'), False, '', [
        (T('Connections'), False, URL('plugin_lookout','plugin_lookout_connections')),
        (T('Add/Edit tables'), False, URL('plugin_lookout','plugin_lookout_tables')),
        (T('Remove tables'), False, URL('plugin_lookout','plugin_lookout_table_remove')),
        (T('Manage fields'), False, URL('plugin_lookout','plugin_lookout_fields')),
    ]),
]
