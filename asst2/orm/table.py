#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#
import collections
from orm import field
import inspect

class MetaTable(type):
    table_names = collections.OrderedDict()
    reserved = [ 'pk', 'version', 'delete', 'save']

    def __init__(cls, name, bases, attrs):
        if name == "Table":
            return 

        if name in MetaTable.table_names:
            raise AttributeError

        MetaTable.table_names[name] = cls
        cls.col = []
        cls.field = []

        for attr in attrs:
            if isinstance(attrs[attr], (field.String, field.Integer, field.Float, 
            field.Foreign, field.DateTime, field.Coordinate) ):
                if "_" in attr:
                    raise AttributeError
                if attr in MetaTable.reserved:
                    raise AttributeError
                cls.col.append(attr)
                cls.field.append(attrs[attr])            

    # Returns an existing object from the table, if it exists.
    #   db: database object, the database to get the object from
    #   pk: int, primary key (ID)
    def get(cls, db, pk):
        return None

    # Returns a list of objects that matches the query. If no argument is given,
    # returns all objects in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def filter(cls, db, **kwarg):
        return list()

    # Returns the number of matches given the query. If no argument is given, 
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwarg):
        return list()
    
    @classmethod
    def __prepare__ (mcs, name, bases, **kwargs):
        return collections.OrderedDict() 

class Table(object, metaclass=MetaTable):
    def __init__(self, db, **kwargs):
        self.db = db
        self.pk = None
        self.version = None
        for col in self.col:
            field = getattr(type(self), col)
            if col not in kwargs:
                if field.blank == False:
                    raise AttributeError
                else:
                    setattr(self, col, field.default)
            else:
                setattr(self, col, kwargs[col])

    # Save the row by calling insert or update commands.
    # atomic: bool, True for atomic update or False for non-atomic update
    def save(self, atomic=True):
        values = []
        index = 0
        for col in self.col:
            if type(self.field[index]) == field.Foreign:
                foreign_obj = getattr(self, col)
                values.append((self.db.scan(type(foreign_obj).__name__, 2, self.field[index].table.col[0], getattr(foreign_obj, self.field[index].table.col[0])))[0])
            else:
                values.append(getattr(self, col))
            index += 1
        
        if self.pk is not None: 
            if atomic:
                ver = self.version
            else:
                ver = 0
            self.version = self.db.update(type(self).__name__, self.pk, values, ver)
        else:
            self.pk, self.version = self.db.insert(type(self).__name__, values)
        
    # Delete the row from the database.
    def delete(self):
        self.db.drop(type(self).__name__, self.pk)
        self.version = None
        self.pk = None

