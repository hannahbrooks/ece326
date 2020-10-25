#!/usr/bin/python3
#
# fields.py
#
# Definitions for all the fields in ORM layer
#

from datetime import datetime
import orm

class Integer():   
    def __init__(self, blank=None, default=0, choices=None):
        if default is not None:
            if type(default) is not int:
                raise TypeError("'{}' is not an integer.".format(default))
        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if type(choice) is not int:
                    raise TypeError("'{}' is not an integer.".format(default))
        self.blank = blank
        self.choices = choices
        self.default = default
        self.type = int

    def _name (self, col):
        self.name = "_"+col
    
    def __set__(self, instance, value):
        if isinstance(value, int):
            return setattr(instance, self.name, value)
        elif not isinstance(value, self.type):
            raise TypeError
        elif self.choices is not None:
            if value not in self.choices:
                raise ValueError

    def __get__(self, instance, owner):
        if instance is not None:
            return getattr(instance, self.name)
        else: 
            return self

class Float: 
    def __init__(self, blank=None, default=0.0, choices=None):
        if default is not 0.0:
            if not type(default) in [int, float]:
                raise TypeError("'{}' is not an integer or float.".format(default))
        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if not type(choice) in [int, float]:
                    raise TypeError("'{}' is not an integer or float.".format(default))
        self.blank = blank
        self.choices = choices
        if callable(default):
            self.default = default()
        else:
            self.default = default 
        self.type = float 
        
    def __get__(self, instance, owner):
        if instance is not None:
            return getattr(instance, self.name)
        else: 
            return self
    
    def __set__(self, instance, value):
        if isinstance(value, int) or isinstance(value, float):
            return setattr(instance, self.name, float(value))
        elif not isinstance(value, self.type):
            raise TypeError
        elif self.choices is not None:
            if value not in self.choices:
                raise ValueError

    def _name (self, col):
        self.name = "_"+col

class String:
    def __init__(self, blank=None, default="", choices=None):
        self.blank = blank
        self.choices = choices
        self.default = default 

        self.type = str
        if default is not "":
            self.blank = True
            if type(default) is not str:
                raise TypeError("'{}' is not a string.".format(default))
        
        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if type(choice) is not str:
                    raise TypeError("'{}' is not a string.".format(default))

    def _name (self, col):
        self.name = "_"+col

    def __get__(self, instance, owner):
        if instance is not None:
            return getattr(instance, self.name)
        else:
            return self
    
    def __set__(self, instance, value):
        if self.choices is not None:
            if value not in self.choices:
                raise ValueError
        
        if isinstance(value, str):
            return setattr(instance, self.name, value)
        elif not isinstance(value, self.type):
            raise TypeError

class Foreign:
    def __init__(self, table="", blank=None):
        if blank == True:
            self.table = table
            self.type = table.__name__
            self.blank = True
        elif blank == False and table=="":
            raise TypeError
        elif (type(table) != orm.table.MetaTable):
            raise TypeError("'{}' is not a table.".format(table))
        elif (table.__name__ in orm.table.MetaTable.table_names):
            self.table = table
            self.type = table
            self.blank = False
        else:
            raise TypeError("'{}' is not a table.".format(table))
        self._default = None

    def _name (self, col):
        self.name = "_"+col

    def __get__(self, instance, owner):
        if instance is not None:
            return getattr(instance, self.name)
        elif self.blank == True:
            return None
        else: 
            return self
    
    def __set__(self, instance, value):
        if isinstance(value.__class__, orm.table.MetaTable):
            return setattr(instance, self.name, value)
        elif self.blank == True:
            return setattr(instance, self.name, None)
        else:
            raise TypeError
                  
class DateTime:
    implemented = True

    def __init__(self, blank=None, default=0, choices=None):
        if default is not 0: default = default()
        
        if default is not 0:
            if type(default) is not datetime:
                raise TypeError("`{}` is not a datetime.".format(default))

        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if type(choice) is not datetime:
                    raise TypeError("'{}' is not a datetime.".format(default))

        self.blank = blank
        self.choices = choices
        self.default = default
    
    def _name (self, col):
        self.name = "_"+col


class Coordinate:
    implemented = True
    def __init__(self, blank=None, default=(0, 0), choices=None):
        if default is not (0, 0):
            if (len(default) != 2):
                raise TypeError
            if default[0] <-90.0 or default[0] > 90.0:
                raise ValueError
            if default[1] <-180.0 or default[1] > 180.0:
                raise ValueError

        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if type(choice) is not float:
                    raise TypeError("`{}` is not a valid coordinate.".format(default))

        self.blank = blank
        self.choices = choices
        self.default = default

    def __set__ (self, obj, val):
        if val is None:
            setattr(obj, self.lat, None)
            setattr(obj, self.lon, None)
            setattr(obj, self.name, val)
        elif (len(val) != 2):
                raise TypeError
        elif val[0] <-90.0 or val[0] > 90.0:
            raise TypeError
        elif val[1] <-180.0 or val[1] > 180.0:
            raise ValueError
        elif self.choices is not None:
            if val not in self.choices and self.blank==True:
                raise TypeError
        else:
            setattr(obj, self.lat, val[0])
            setattr(obj, self.lon, val[1])
            setattr(obj, self.name, val)

    def __get__ (self, instance, owner):
        if instance is not None:
            lat = getattr(instance, self.lat)
            lon = getattr(instance, self.lon)
            return (lat, lon)
        else:
            return self

    def _name (self, col):
        self.lat = "_lat"
        self.lon = "_lon"
        self.name = "_"+col





