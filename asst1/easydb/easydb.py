#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):
        # TODO: implement me
        pass
        
    def connect(self, host, port):
        # TODO: implement me
        return False

    def close(self):
        # TODO: implement me
        pass

    def insert(self, table_name, values):
        # TODO: implement me
        pass

    def update(self, table_name, pk, values, version=None):
        # TODO: implement me
        pass

    def drop(self, table_name, pk):
        # TODO: implement me
        pass
        
    def get(self, table_name, pk):
        # TODO: implement me
        pass

    def scan(self, table_name, op, column_name=None, value=None):
        # TODO: implement me
        pass
                        
