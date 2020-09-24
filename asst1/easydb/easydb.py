#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
import ctypes
import sys
import struct
import io


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
        self.socket.connect((host, int(port))) 
               
        pass

    def close(self):
        # create a string of bits everytime i wanna do anythign
        # send a big string
        # send what u wanna do (the instruction)
        # send a 6 and a 1
        # struct pack
        # we usually send ints, floats, longs
        # struct pack format "iii, 2, 3, 4"

        req = struct.pack('>iii', 6, 1, 0)
        self.socket.send(req)
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        sys.exit()

        pass

    def insert(self, table_name, values):
        # TODO: implement me
        table = 0
        if (table_name == "User"):
            table = 1
        else:
            table = 2

        values_list = []
        packet_types = ""


        for i in range(0,len(values)):
            buf = "none"

            print(values[i])

            if (type(values[i]) is str):
                typ = 3 
                size = 4
                buf = str.encode(values[i])
                packet_char = 's'
            elif (type(values[i]) is int):
                typ = 1
                size = 8
                packet_char = 'i'
            elif (type(values[i]) is float):
                typ = 2
                size = 8
                packet_char = 'f'

            values_list.append(typ)
            values_list.append(size)


            if buf is "none":
                values_list.append(values[i])
            else:
                values_list.append(buf)

            packet_types += 'ii'+ packet_char

        req = struct.pack('>iii' + packet_types, 1, table, len(values), *values_list)

        self.socket.send(req)

        conn, addr = self.socket.accept()

        with conn:
            while True:
                data = conn.recv(4096)


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

    def get_table_id(self, table_name):
        return self.table_names.index(table_name)
                        
