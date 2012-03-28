# coding: utf8
# prova qualcosa come
def index(): return dict(message="hello from plugin_lookout.py")

@auth.requires_login()
def plugin_lookout_connections():
    db.plugin_lookout_connections.dsn.represent = lambda v, r: '%s: %s' % (r.alias, v.replace('%s', '<password>'))
    form = SQLFORM.smartgrid(db.plugin_lookout_connections)
    
    res_conn = db(db.plugin_lookout_connections).select()
    dbs = dict([(x.alias, globals()[x.alias].tables) for x in res_conn])
    
    return dict(form=form, dbs=dbs)


def tab_onvalidation(form):
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

@auth.requires_login()
def plugin_lookout_tables():
    
    db.plugin_lookout_tables.table_name.represent = lambda val,row: A(row.table_name, _href=URL('plugin_lookout_external_tables', vars=dict(id=row.id)))
    form = SQLFORM.smartgrid(db.plugin_lookout_tables, onvalidation=tab_onvalidation, deletable=False, linked_tables=['plugin_lookout_connections'])
    return locals()


@auth.requires_login()
def plugin_lookout_table_remove():
    '''
    Controller for removing configured tables
    '''
    message = T('This controller is for table deletion. Warning! Only active table can be deleted.')
    form = SQLFORM.factory(
        Field('table_id', label=T('Table name'), comment=T('Choose the table to delete.'),
            requires=IS_IN_DB(db(db.plugin_lookout_tables.is_active==True), 'plugin_lookout_tables.id', '%(table_name)s'))
    )

    if form.accepts(request, session, formname='form_one'):
        tab = db(db.plugin_lookout_tables.id==form.vars.table_id).select().first()
        if tab:
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
                # controllo forse inutile: le viste hanno migrate=False per cui
                #+ il file non esiste
#                if db[tab.tab_name]._dbt:
#                    db._adapter.file_delete(db[tab.tab_name]._dbt)
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
    form = SQLFORM.smartgrid(db.plugin_lookout_fields, linked_tables=['plugin_lookout_tables'])
    return locals()


@auth.requires_login()
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
        session.flash = 'La tabella "%s" non riconosciuta in database o non attiva.' % rec_table.table_name
        redirect(URL('plugin_lookout_tables'))
    
    grid=SQLFORM.smartgrid(mydb[rec_table.table_name], deletable=False, editable=True, create=rec_table.table_migrate)
    if request.extension == 'load':
        return dict(grid=grid)
    else:
        return dict(grid=grid, message=message)
