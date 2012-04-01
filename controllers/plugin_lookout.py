
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
#    along with Nome-Programma.  If not, see <http://www.gnu.org/licenses/>.

# prova qualcosa come
def index(): return dict(message="hello from plugin_lookout.py")

#        # condizione: esiste tabella con lo stesso nome in una connessione con stesso alias
#        alias = db(db.plugin_lookout_connections.id==form.vars.connection)\
#            .select(db.plugin_lookout_connections.alias).first().alias
#        subquery = db(db.plugin_lookout_connections.alias==alias)\
#            ._select(db.plugin_lookout_connections.id)
#        res = db(db.plugin_lookout_tables.connection.belongs(subquery)\
#            &(db.plugin_lookout_tables.table_name==form.vars.table_name)).select()
#        if len(res)==1:
#            set_table_restrictions(res.first())
#            db.commit()

def conn_onvalidation(form):
    '''
    The callback function for the connections form
    '''
    if form.vars.pwd:
        uri = form.vars.dsn % form.vars.pwd
    else:
        uri = form.vars.dsn
    
    if db._uri == uri:
        form.vars.alias = 'db'
    
    res = db(db.plugin_lookout_connections.dsn==form.vars.dsn).select()

    if res:
        form.vars.alias = res.first().alias
        group_id = auth.user_group(auth.user_id)

        if not auth.has_permission(table_name='plugin_lookout_connections',
            user_id=auth.user_id,
            record_id=res.first().id
        ):
            auth.add_permission(table_name='plugin_lookout_connections',
                group_id=group_id, record_id=res.first().id)

#        for row in res:
#            if not auth.has_permission(table_name='plugin_lookout_tables', user_id=auth.user_id, record_id=row.id):
#                auth.add_permission(table_name='plugin_lookout_tables', group_id=group_id, record_id=row.id)

        db_hash = globals()[form.vars.alias]
        groups = db(db.auth_group.role.contains(db_hash)).select(db.auth_group.id)
        for group in groups:
            if not auth.has_membership(group.id, auth.user_id):
                auth.add_membership(group.id, auth.user_id)

def get_data_with_permissions(table_name):
    ids = db((db.auth_permission.table_name==table_name)\
        &(~db.auth_permission.group_id.belongs(auth.user_groups.keys())))\
        ._select(db.auth_permission.record_id)

    return (~db[table_name].id.belongs(ids))

@auth.requires_login()
def plugin_lookout_connections():
    db.plugin_lookout_connections.dsn.represent = lambda v, r: '%s: %s' % (r.alias, v.replace('%s', '<password>'))
    
    form = SQLFORM.smartgrid(db.plugin_lookout_connections,
        onvalidation = conn_onvalidation,
        linked_tables = [],
        constraints = dict(plugin_lookout_connections=get_data_with_permissions('plugin_lookout_connections'))
    )
    set_ownership('plugin_lookout_connections')

#    res_conn = db(db.plugin_lookout_connections).select()
#    dbs = dict([(x.alias, globals()[x.alias].tables) for x in res_conn])

    return dict(form=form) #, dbs=dbs)

def tab_onvalidation(form):
    '''
    The callback function for the tables form
    '''
    # on create
    from plugin_lookout import db_got_table
    if 'new' in request.args:
        db_alias = db(db.plugin_lookout_connections.id==form.vars.connection).select(db.plugin_lookout_connections.alias).first().alias
        mydb = globals()[db_alias]
        tab_in_sqldb, msg = db_got_table(mydb, form.vars.table_name)
        
        if tab_in_sqldb:
            form.vars.table_migrate = False
        else:
            form.vars.table_migrate = True
        form.errors.table_name = IS_VALID_SQL_TABLE_NAME(
            globals()[db(db.plugin_lookout_connections.id==request.vars.connection)\
                .select(db.plugin_lookout_connections.alias).first().alias],
            check_reserved=('common', 'postgres', )
        )(form.vars.table_name)[1]
        
        t_hash = '%s_%s' % (globals()[db_alias]._uri_hash, form.vars.table_name)
        if form.vars.restricted:
            r_role = 'read_%s' % t_hash
            r_group_id = db.auth_group.update_or_insert(role=r_role)
            if not r_group_id:
                r_group_id = db(db.auth_group.role==r_role).select().first().id
            if not auth.has_membership(r_group_id, auth.user_id):
                auth.add_membership(r_group_id, auth.user_id)
            
            w_role = 'write_%s' % t_hash
            w_group_id = db.auth_group.update_or_insert(role=w_role)
            if not w_group_id:
                w_group_id = db(db.auth_group.role==w_role).select().first().id
            if not auth.has_membership(w_group_id, auth.user_id):
                auth.add_membership(w_group_id, auth.user_id)
        
@auth.requires_login()
def plugin_lookout_tables():
    '''
    funzioni da aggiungere:
    * aggiungi campo a tabella
    * rimuovi campo dalla tabella
    '''
    if 'edit' in request.args:
        db.plugin_lookout_tables.table_name.writable = False
        db.plugin_lookout_tables.connection.writable = False
    db.plugin_lookout_tables.table_name.represent = lambda val,row: A(row.table_name, _href=URL('plugin_lookout_external_tables', vars=dict(id=row.id)))
    form = SQLFORM.smartgrid(db.plugin_lookout_tables,
        onvalidation=tab_onvalidation,
        deletable=False,
        linked_tables=[],
        constraints = dict(plugin_lookout_tables=get_data_with_permissions('plugin_lookout_tables'))
    )
    set_ownership('plugin_lookout_tables')
    return dict(form=form)


@auth.requires_login()
def plugin_lookout_table_remove():
    '''
    Controller for removing configured tables
    '''
    message = T('This controller is for table deletion. Warning! Only active table can be deleted.')
    form = SQLFORM.factory(
        Field('table_id',
            label=T('Table name'),
            comment=T('Choose the table to delete.'),
            requires=IS_IN_DB(db((db.plugin_lookout_tables.is_active==True)&get_data_with_permissions('plugin_lookout_tables')),
                'plugin_lookout_tables.id', '%(table_name)s')
            )
    )

    if form.accepts(request, session, formname='form_one'):
        tab = db(db.plugin_lookout_tables.id==form.vars.table_id).select().first()
        
        key = '%s_%s' % (globals()[tab.connection.alias]._uri_hash, tab.table_name)
        db(db.auth_group.role.contains(key)).delete()
        
        for row in db(db.plugin_lookout_fields.tables.contains(form.vars.table_id)).select():
            ids = row.tables
            ids.remove(int(form.vars.table_id))
            row.update_record(tables=ids)
        
        if tab.table_migrate:
            try:
                globals()[tab.connection.alias][tab.table_name].drop()
            except Exception, error:
                session.flash = T('Table not removed: %s') % str(error)
            else:
                db(db.plugin_lookout_tables.id==form.vars.table_id).delete()
        elif tab.is_view:
            db.executesql('DROP VIEW %s CASCADE;' % tab.table_name)
            db(db.plugin_lookout_tables.id==form.vars.table_id).delete()
        else:
            db(db.plugin_lookout_tables.id==form.vars.table_id).delete()

        unusefull_fields_set = db(db.plugin_lookout_fields.tables==[])
        if unusefull_fields_set.count() > 0:
            unusefull_fields_set.delete()
        redirect(URL('plugin_lookout_tables'))

    if request.extension == 'load':
        return dict(form=form)
    else:
        return dict(form=form, message=message)


db.plugin_lookout_fields.tables.represent = lambda id,row: CAT(*[CAT(A('%s ' %i.table_name,
    _href = URL('plugin_lookout_tables',
        args = ['plugin_lookout_tables', 'view','plugin_lookout_tables', i.id],
        user_signature = True)), BR()
    ) for i in db(db.plugin_lookout_tables.id.belongs(id))\
        .select(db.plugin_lookout_tables.id, db.plugin_lookout_tables.table_name)])

@auth.requires_login()
def plugin_lookout_fields():
    form = SQLFORM.smartgrid(db.plugin_lookout_fields, linked_tables=['plugin_lookout_tables'], editable=False, deletable=False)
    return locals()


@auth.requires(get_table_restriction(request.vars.id or session.plugin_lookout_external_tables_id, action='r'), requires_login=True)
def plugin_lookout_external_tables():
    '''
    Controller for manage data inside table that are not part of the model
    '''
    message = 'Here you can see the date inside the tables you have configured'
    table_id = request.vars.id or session.plugin_lookout_external_tables_id or redirect(URL('plugin_lookout_tables'))
    session.plugin_lookout_external_tables_id = table_id
#    table_id = int(request.args(0)) or redirect(URL('plugin_lookout_tables'))
    
    check_message = IS_IN_DB(db, 'plugin_lookout_tables.id')(table_id)[1]
    if check_message:
#        session.flash = 'Non puoi accedere alla tabella "%s" attraverso questa risorsa. %s' %(tab_name, check_message)
        session.flash = T('You cannot have access to the table "%s" through this resource. %s' %(table_id, check_message))
        redirect(URL('plugin_lookout_tables'))
    
    rec_table = db(db.plugin_lookout_tables.id==table_id).select().first()
    mydb = globals()[rec_table.connection.alias]
    if rec_table.table_name not in mydb.tables:
        session.flash = T('Table "%s" is not recognized from db model. maybe it\'s not active') % rec_table.table_name
        session.flash = 'La tabella "%s" non riconosciuta in database o non attiva.' % rec_table.table_name
        redirect(URL('plugin_lookout_tables'))
    
    writable=get_table_restriction(table_id, action='w')
    grid=SQLFORM.smartgrid(mydb[rec_table.table_name], deletable=writable, editable=writable, create=writable)
    if request.extension == 'load':
        return dict(grid=grid)
    else:
        return dict(grid=grid, message=message)
