#!/usr/bin/python3
#
# fields.py
#
# Definitions for all the fields in ORM layer
#

from datetime import datetime
import orm

class Integer:   
    def __init__(self, blank=False, default=0, choices=None):
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

class Float: 
    def __init__(self, blank=False, default=0.0, choices=None):
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
        self.default = default

class String:
    def __init__(self, blank=False, default="", choices=None):
        if default is not None:
            if type(default) is not str:
                raise TypeError("'{}' is not a string.".format(default))
        
        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if type(choice) is not str:
                    raise TypeError("'{}' is not a string.".format(default))

        self.blank = blank
        self.choices = choices
        self.default = default

class Foreign:
    def __init__(self, table="", blank=False):
        if blank == True:
            self.table = None
        elif blank == False and table=="":
            raise TypeError
        elif (type(table) != orm.table.MetaTable):
            raise TypeError("'{}' is not a table.".format(table))
        elif (table.__name__ in orm.table.MetaTable.table_names):
            self.table = table
        else:
            raise TypeError("'{}' is not a table.".format(table))
                  


class DateTime:
    implemented = True

    def __init__(self, blank=False, default=0, choices=None):
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


class Coordinate:
    implemented = True

    def __init__(self, blank=False, default=None, choices=None):
        if default is not None:
            if type(default) not in (list, tuple) or len(default) != 2:
                raise TypeError("`{}` is not a valid coordinate.".format(default))
            if type(default[0]) not in (int, float) or type(default[1]) not in (int, float):
                raise TypeError("`{}` are not valid coordinate types.".format(default))
            if not (-90 <= default[0] <= 90) or not (-180 <= default[1] <= 180):
                raise TypeError("`{}` does not contain valid coordinate values.".format(default))

        if choices is not None:
            if default not in choices and blank==True:
                raise TypeError
            for choice in choices:
                if type(choice) is not datetime:
                    raise TypeError("`{}` is not a valid coordinate.".format(default))

        self.blank = blank
        self.choices = choices
        self.default = default





