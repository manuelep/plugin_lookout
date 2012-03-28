
#!/usr/bin/env python
# coding: utf8
from gluon import *

from dal import regex_python_keywords
from validators import Validator, translate
import re

class IS_VALID_SQL_TABLE_NAME(Validator):
    '''
    Checks if the field's value is a valid SQL table name.
    
    Arguments:
    
    db: an instance of the dal class that represent the database that will
        contain the table name
    
    check_reserved: list of adapters to check tablenames and column names
         against sql reserved keywords. (Default ("common", ))
        * "common" List of sql keywords that are common to all database types
            such as "SELECT, INSERT". (recommended)
        * "all" Checks against all known SQL keywords. (not recommended)
            <adaptername> Checks against the specific adapters list of keywords
            (recommended)
    
    Examples:
    
        #Check if text string is a good sql table name:
        INPUT(_type='text', _name='name', requires=IS_VALID_SQL_TABLE_NAME(db))
        
        #Check if text string is a good sql table name specific for postgres dialect:
        INPUT(_type='text', _name='name', requires=IS_VALID_SQL_TABLE_NAME(db, check_reserved=('postgres', )))
        
        >>> IS_VALID_SQL_TABLE_NAME(db)('')
        ('', 'invalid table name: ')
        >>> IS_VALID_SQL_TABLE_NAME(db)('foo')
        ('foo', None)
        >>> IS_VALID_SQL_TABLE_NAME(db)('test')
        ('test', 'table/attribute name already defined: test')
        >>> IS_VALID_SQL_TABLE_NAME(db)('select')
        ('select', 'invalid table/column name "select" is a "COMMON" reserved SQL keyword')
    
    '''
    def __init__(self, db, check_reserved=('common', )):
        self.db = db
        self.check_reserved = check_reserved
        if self.check_reserved:
            from reserved_sql_keywords import ADAPTERS as RSK
            self.RSK = RSK

    def __call__(self, value):
    
        if re.compile('[^0-9a-zA-Z_]').findall(value):
            return (value, translate('only [0-9a-zA-Z_] allowed in table and field names, received %s' % value))
        elif value.startswith('_') or regex_python_keywords.match(value) or not value:
            return (value, translate('invalid table name: %s' % value))
        elif value.lower() in self.db.tables or hasattr(self.db,value.lower()):
            return (value, translate('table/attribute name already defined: %s' % value))
        elif self.check_reserved:
            # Validates ``name`` against SQL keywords
            #+ Uses self.check_reserve which is a list of
            #+ operators to use.
            #+ self.check_reserved
            #+ ['common', 'postgres', 'mysql']
            #+ self.check_reserved
            #+ ['all']
            for backend in self.check_reserved:
                if value.upper() in self.RSK[backend]:
                    return (value, translate('invalid table/column name "%s" is a "%s" reserved SQL keyword' % (value, backend.upper())))
            return (value, None)

def db_got_table(db, table):
    '''
    db: the database connection where to look for
    table: the table name to look for
    '''
    db_type = db._uri[:8]
    msg = ''
    sql_src = "SELECT 1 FROM %s WHERE 1=2" % table
    try:
        db.executesql(sql_src)
    except db._adapter.driver.OperationalError, error:
        msg = str(error)
        if msg == 'no such table: %s' % table:
            answare = False
        else:
            raise db._adapter.driver.OperationalError(msg)
    else:
        answare = True
    return answare, msg
