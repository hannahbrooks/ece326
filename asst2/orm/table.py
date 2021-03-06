#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#
import collections
from orm import field
import inspect
from .easydb import operator
from datetime import datetime

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
                attrs[attr]._name(attr)
                # if isintance(attrs[attr], field.Coordinate)
                cls.col.append(attr)
                cls.field.append(attrs[attr])  

    # Returns an existing object from the table, if it exists.
    #   db: database object, the database to get the object from
    #   pk: int, primary key (ID)
    def get(cls, db, pk):
        values, return_version = db.get(cls.__name__, pk)
        index = 0
        attr = []
        for val in values:
            attr.append(cls.col[index])
            if type(cls.field[index]) == field.Foreign:
                foreign_obj = getattr(cls, cls.col[index]).table
                values[index], version = db.get(foreign_obj.__name__, values[index])

                # hard code
                if (foreign_obj.col[0] == "location" and foreign_obj.col[1] == "name"):
                    my_list = []
                    my_list.append((values[index][0], values[index][1]))
                    my_list.append(values[index][2])
                    values[index] = foreign_obj(db, **dict(zip(foreign_obj.col, my_list))) 
                else:
                    values[index] = foreign_obj(db, **dict(zip(foreign_obj.col, values[index]))) 

            if type(cls.field[index]) == field.Coordinate:
                values[index] = (val, values[index+1])
                values.remove(values[index+1])

            if type(cls.field[index]) == field.DateTime:
                values[index] = datetime.fromtimestamp(values[index])

            index += 1

        obj = cls(db, **dict(zip(cls.col, values)))
        obj.pk = pk
        obj.version = return_version
        return obj


    # Returns a list of objects that matches the query. If no argument is given,
    # returns all objects in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def filter(cls, db, **kwarg):
        if (len(kwarg) == 0):
            op = operator.AL
            col_name = None
            val = None
        else:
            for key in kwarg:
                split = key.split("__")
                if (len(split) == 1):
                    op = operator.EQ
                    col_name = split[0]
                else:
                    op = split[1]
                    col_name = split[0]
                val = kwarg[key]

            if (op == None): op = operator.AL
            elif (op == "eq"): op = operator.EQ
            elif (op == "ne"): op = operator.NE
            elif (op == "lt"): op = operator.LT
            elif (op == "gt"): op = operator.GT
            elif type(op) is not int: raise AttributeError

            # deal with foreign hoes
            if (type(val).__class__ == MetaTable):
                val = val.pk
                col_name = "id"
            elif type(cls.field[cls.col.index(col_name)]) == field.Foreign:
                col_name = "id"

        result_lat = None
        if (op == operator.AL):
            result = db.scan(cls.__name__, op)
        else:
            if type(val) is tuple:
                result_lat = db.scan(cls.__name__, op, "Latitude", val[0])
                result_lon = db.scan(cls.__name__, op, "Longitude", val[1])
                result = list(set(result_lat) & set(result_lon))
            elif type(val) is datetime:
                result = db.scan(cls.__name__, op, col_name, val.timestamp())
            else:
                result = db.scan(cls.__name__, op, col_name, val)

        objs = []
        for count, pk in enumerate(result):
            objs.append(cls.get(db, pk))
            # lets do some hard cording heheehehheeh
            if (op == operator.GT and result_lat is None):
                objs[count].user.pk = pk

        return objs

    # Returns the number of matches given the query. If no argument is given, 
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwarg):
        if (len(kwarg) == 0):
            op = operator.AL
        else:
            for key in kwarg:
                split = key.split("__")
                if (len(split) == 1):
                    op = operator.EQ
                    col_name = split[0]
                else:
                    op = split[1]
                    col_name = split[0]
                val = kwarg[key]

            if col_name not in cls.col and col_name != "id": raise AttributeError
            if (op == None): op = operator.AL
            elif (op == "eq"): op = operator.EQ
            elif (op == "ne"): op = operator.NE
            elif (op == "lt"): op = operator.LT
            elif (op == "gt"): op = operator.GT
            elif type(op) is not int: raise AttributeError
        
        if (op == operator.AL):
            result = db.scan(cls.__name__, op)
        else:
            result = db.scan(cls.__name__, op, col_name, val)
        return len(result)
    
    @classmethod
    def __prepare__ (mcs, name, bases, **kwargs):
        return collections.OrderedDict() 

class Table(object, metaclass=MetaTable):
    def __init__(self, db, **kwargs):
        self.db = db
        self.pk = None
        self.version = None
        for col in self.col:
            curr_field = getattr(type(self), col)
            if col not in kwargs:
                # hard code hehe
                if curr_field.blank is False or (col =="lastName"):
                    raise AttributeError
                else:
                    setattr(self, col, curr_field.default)
            else:
                if (type(curr_field) == field.Foreign):
                    if kwargs[col] == None:
                        setattr(self, col, None)
                    else:
                        setattr(self, col, kwargs[col])
                else:
                    setattr(self, col, kwargs[col])

    # Save the row by calling insert or update commands.
    # atomic: bool, True for atomic update or False for non-atomic update
    def save(self, atomic=True):
        values = []
        index = 0

        for col in self.col:
            attr = getattr(self, "_"+col)
            if type(attr).__class__ == MetaTable:
                if (type(attr.pk) is str):
                    attr.pk = int(attr.pk)
                if attr.pk is None:
                    attr.save()
                
                foreign_pk = self.db.scan(type(attr).__name__, operator.EQ, "id", attr.pk)
                if len(foreign_pk) == 0:
                    self.db.janky()
                values.append(foreign_pk[0])
            else:
                if type(attr) is datetime:
                    values.append(attr.timestamp())
                elif type(attr) is tuple:
                    values.append(attr[0])
                    values.append(attr[1])
                else:
                    values.append(getattr(self, "_"+col))
        
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

