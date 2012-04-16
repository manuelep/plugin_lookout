
# coding: utf8

#    This file is part of plugin_lookout.

#    Plugin_lookout is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.

#    Plugin_lookout is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#    You should have received a copy of the GNU General Public License
#    along with Plugin_lookout.  If not, see <http://www.gnu.org/licenses/>.

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
    Field('alias', readable=False, required=True, notnull=True, writable=False),
    Field('dsn', required=True),
    Field('pwd', 'password', readable=False),
    Field('is_active', 'boolean', default=True),
    auth.signature.created_by,
    format = '%(dsn)s' #lambda r: ('%(alias)s %(dsn)s' % r).replace('%s', randint(8, 16*'*'))
)

db.plugin_lookout_connections.alias.requires = IS_NOT_IN_DB(db, 'plugin_lookout_connections.alias')
f_max = db.plugin_lookout_connections.id.max()
id_max = db().select(f_max).first()[f_max] or 0
db.plugin_lookout_connections.alias.default = 'db_%s' % (int(id_max) + 1)
db.plugin_lookout_connections.dsn.represent = lambda v, r: '%s: %s' % (r.alias, v.replace('%s', randint(8, 16)*'*'))

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
        del v


####################################################################### TABLES #

from plugin_lookout import db_got_table

db.define_table('plugin_lookout_tables',
    Field('table_name', label=T('Table name'), required=True,
        ondelete='CASCADE'
    ),
    Field('table_migrate', 'boolean',
        compute = lambda row: not db_got_table(globals()[db.plugin_lookout_connections[row['connection_id']].alias], row['table_name'])[0]),
    Field('table_singular', label=T('Singular')),
    Field('table_plural', label=T('Plural')),
    Field('restricted', 'boolean', default=False),
    Field('is_active', 'boolean', default=True, label=T('Active'), comment=T('Let the table be recognized from db?')),
    Field('is_view', 'boolean', label=T('View'), default=False, writable=False, readable=False),
    Field('connection_id', db.plugin_lookout_connections, required=True,
        requires = IS_IN_DB(db(db.plugin_lookout_connections.created_by==auth.user_id), 'plugin_lookout_connections.id', '%(alias)s: %(dsn)s')
    ),
    Field('connection_name', compute=lambda row: db.plugin_lookout_connections[row['connection_id']].alias),
    auth.signature.created_by,
    format='%(table_name)s',
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
    Field('table_id', db.plugin_lookout_tables, label=T('Table name'),
        required=True, requires=IS_IN_DB(db, 'plugin_lookout_tables.id', '%(table_name)s')),
    Field('field_name', label=T('Field name'), required=True,
        requires=IS_MATCH('^[a-z0-9_]*$', error_message='Nome campo non valido.')),
    Field('field_type', label=T('Field type'), comment=T('default: "string"'),
        requires=IS_EMPTY_OR(IS_IN_SET(field_types))),
    Field('field_format', length=25, label=T('Format'),
        comment = T('Date/time format (Optional. Only for date and datetime field types)')),
#            'Formato data/ora (es: "%H:%M:%S - %d/%m/%Y"). \
#            Utile SOLO nei casi di dato un formato date, datetime o time. \
#            Attenzione! Se la tabella esiste in database il formato deve \
#            rispettare quello in uso. Verificare nei dati esistenti.'),
    Field('field_length', 'integer', label=T('Length'),
        comment=T('Field lenght')),
    Field('field_label', label=T('Label')),
    Field('field_comment', label=T('Comment'))
)


from plugin_lookout import geom_representation
def define_tables(fake_migrate=False):

    join = db.plugin_lookout_tables.connection_id == db.plugin_lookout_connections.id
    where = (db.plugin_lookout_tables.is_active==True)&(db.plugin_lookout_connections.is_active==True)
    res_tables = db(join&where).select(db.plugin_lookout_connections.ALL, db.plugin_lookout_tables.ALL)
    res_fields = db(db.plugin_lookout_fields).select(orderby=db.plugin_lookout_fields.table_id)

    translate = dict(
        field_type = 'type',
        field_label = 'label',
        field_length = 'length',
        field_comment = 'comment'
    )

    validators = dict(
        date = IS_DATE,
        datetime = IS_DATETIME,
        time = IS_TIME,
        text = IS_LENGTH(65536)
    )

    for rec_table in res_tables:
        field_list = list()
        geoms = dict(geometry=[], geography=[])
        for rec_field in res_fields.find(lambda row: rec_table.plugin_lookout_tables.id == row.table_id):

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
                    kwargs['requires'] = IS_EMPTY_OR(validators.get('text'))
                    kwargs['writable'] = False # if unsupported geometryes are managed as visible only text record
                    kwargs['represent'] = lambda value,row: '%s ...' % geom_representation(value)
            field_list.append(Field(rec_field.field_name, **kwargs))

        mydb = globals()[rec_table.plugin_lookout_connections.alias]
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
                        r = mydb.executesql("select data_type from information_schema.columns where table_name='%s' AND column_name='%s'" % (rec_table.plugin_lookout_tables.table_name, i))
                        if r[0][0] == 'text':
                            mydb.executesql('ALTER TABLE %s ALTER COLUMN %s TYPE %s;' % (rec_table.plugin_lookout_tables.table_name, i, k))

define_tables()

######################################################################## UTILS #

import os
uploadfolder=os.path.join(request.folder,'uploads/')

plugin_lookout_datafiles_types = ['xlsx', 'zip', 'egg', 'jar', 'tar', 'gz', 'tgz', 'bz2', 'tz2']
db.define_table('plugin_lookout_datafiles',
    Field('file_name', 'upload', uploadfolder=uploadfolder, autodelete=True),
    Field('extension', compute=lambda r: r['file_name'].split('.')[-1], 
        requires = IS_IN_SET(plugin_lookout_datafiles_types)
    ),
    Field('table_id', 'reference plugin_lookout_tables', writable=False, readable=False, unique=True, notnull=True)
)

def control_permission(table_id, reading=False, default=False):
    rec_table = db.plugin_lookout_tables[table_id]
#    import ipdb; ipdb.set_trace()
    if not rec_table: return default
    
    if not rec_table.restricted:
        return True
    else:
        t_hash = '%s_%s' % (globals()[rec_table.connection_name]._uri_hash, rec_table.table_name)
        if reading:
            role = 'read_%s' % t_hash
        else:
            role = 'write_%s' % t_hash
        return auth.has_permission(role, 'plugin_lookout_tables', table_id, auth.user_id)

def set_data_permission(table_name, record_id, role='any', group_id=None):
    '''
    roles: read_<db._uri_hash>_<table_name> 
           write_<db._uri_hash>_<table_name> 
    '''
    db.auth_permission.update_or_insert(
        group_id = group_id or auth.id_group('user_%s' % auth.user_id),
        name = role,
        table_name = table_name,
        record_id = record_id
    )

def get_table_set(view_only=True):
    '''deve restituire il set dei record delle tabelle registrate:
    .1. view_only=True:
        condivise in lettura,
        non ristrette
        
    .2. view_only=False
        condivise in scrittura
        non ristrette
    '''
    
    base_condition = db.auth_permission.table_name=='plugin_lookout_tables'
    
    owner_condition = db.auth_permission.group_id.belongs(auth.user_groups.keys())
    owner_query = db(base_condition&owner_condition)._select(db.auth_permission.record_id)
    
    # not restricted table set
    others_query = db(base_condition&~owner_condition)._select(db.auth_permission.record_id)
    not_restricted_cond = ~db.plugin_lookout_tables.id.belongs(others_query)
    
    # shared table for reading
    reading_permission_condition = db.auth_permission.name.contains('read_')
    r_q = db(base_condition&owner_condition&reading_permission_condition)._select(db.auth_permission.record_id)
    shared_for_reading_condition = db.plugin_lookout_tables.id.belongs(r_q)
    
    # shared table for writing
    wrinting_permission_condition = db.auth_permission.name.contains('write_')
    w_q = db(base_condition&owner_condition&wrinting_permission_condition)._select(db.auth_permission.record_id)
    shared_for_writing_condition = db.plugin_lookout_tables.id.belongs(w_q)

    if view_only:
        return not_restricted_cond|shared_for_reading_condition
    else:
        return not_restricted_cond|shared_for_writing_condition
    
def get_connection_set():
    ids = db((db.auth_permission.table_name=='plugin_lookout_connections')\
        &(~db.auth_permission.group_id.belongs(auth.user_groups.keys())))\
        ._select(db.auth_permission.record_id)
    return (~db.plugin_lookout_connections.id.belongs(ids))

def share_data(table, read_only=True, users=None):
    from gluon.dal import Row
    if not users:
        users = [auth.user]
    elif isinstance(users, Row):
        users = [users]
    t_hash = '%s_%s' % (table._db._uri_hash, table._tablename)
    if read_only:
        key = 'read_%s' % t_hash
    else:
        key = t_hash
    groups = db(db.auth_group.role.contains(key)).select(db.auth_group.id)
    for group in groups:
        for user in users:
            if not auth.has_membership(group.id, user.id):
                auth.add_membership(group.id, user.id)

######################################################################### MENU #

response.menu+=[
    (T('plugin lookout'), False, URL('plugin_lookout','index'), [
        (T('Connections'), False, URL('plugin_lookout','plugin_lookout_connections')),
        (T('Browse tables'), False, URL('plugin_lookout','plugin_lookout_tables', vars=dict(only_view=True))),
        (T('Add/Edit/Remove tables'), False, URL('plugin_lookout','plugin_lookout_tables')),
        (T('Manage fields'), False, URL('plugin_lookout','plugin_lookout_fields')),
        (T('Import table structure from xls'), False, URL('plugin_lookout','import_struct', args=['new'])),
        (T('Import ESRI shape file'), False, URL('plugin_lookout','import_shp')),
    ]),
]
