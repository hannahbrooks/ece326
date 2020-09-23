#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
import ctypes
import sys


class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):
        table_names = []

        for table_name, cols in tables:
            print(table_name)
            print(cols)
            column_names = []
            for col_name, col_type in cols:
                print('\n')
                print(col_name)
                print(col_type)
                column_names.append(col_name)
            table_names.append(table_name) # prevent cyclical references

        self.tables = [None] + list(tables) # considered dict, but need preserve order
        self.table_names = [None] + table_names

        pass

    def connect(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((host, port))
        self.socket.connect((host, int(port))) 
               
        pass

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR) 
        self.socket.close()
        sys.exit()

        pass

    def insert(self, table_name, values):
        # TODO: implement me
        #for table_name_i, col in self.tables: 
         #   if table_name_i is table_name: 
		  #      
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
                        
