#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
import ctypes
import sys
import io
import struct

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
        #self.socket.bind((host, port))
        self.socket.connect((host, int(port))) 
               
        pass

    def close(self):
        req = struct.pack('>iii', 6, 1, 0)
        self.socket.send(req)
        self.socket.shutdown(socket.SHUT_RDWR) 
        self.socket.close()
        sys.exit()

    # Insert row into table 
    # @param self: DB object 
    # @param table_name: string name of table 
    # @param values: list of values to be inserted 
    def insert(self, table_name, values):

        # Initial variables set
        table = 1 if table_name == "User" else 2
        values_list = []
        packet_types = ""

        # Set packet bytes for values paramater 
        for i in range(0,len(values)):
            buf = "none"
            if (type(values[i]) is str):
                typ = 3 
                size = 4
                buf = str.encode(values[i])
                packet_char = 'p'
            elif (type(values[i]) is int):
                typ = 1
                size = 8
                packet_char = 'i'
            elif (type(values[i]) is float):
                typ = 2
                size = 8
                packet_char = 'f'

            values_list.append(typ)             # type (int)
            values_list.append(size)            # size (int)
            if buf is "none":
                values_list.append(values[i])   # literal value (byte form)
            else:
                values_list.append(buf)         # literal value 

            packet_types += 'ii'+ packet_char   # packet types string 

        # Send request
        req = struct.pack('>iii'+packet_types, 1, table, len(values), *values_list)
        self.socket.send(req)

        # Receieve request
        response = struct.unpack('>i', self.socket.recv(1024))
                
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
                        