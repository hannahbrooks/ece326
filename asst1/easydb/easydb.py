#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import socket
import ctypes
import math
import sys
import struct
from .exception import *
import io

class Database:
    def get_table_id(self, table_name):
        return self.table_names.index(table_name)

    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):
        table_names = []

        for table_name, cols in tables:
            column_names = []
            for col_name, col_type in cols:
                column_names.append(col_name)
            table_names.append(table_name) # prevent cyclical references

        self.tables = [None] + list(tables) # considered dict, but need preserve order
        self.table_names = [None] + table_names

        pass

    def connect(self, host, port):

        # Connect to server using socket 
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.connect((host, int(port))) 

        # Check server connection status 
        data = self.socket.recv(4096)
        response = struct.unpack('!i', data)
        if (response[0] is 10):
            raise Exception(10)             # SERVER_BUSY
            self.close() 
               
    def close(self):

        req = struct.pack('>iii', 6, 1, 0)
        self.socket.send(req)
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        sys.exit()

        return

    # Inserts row into given table 
    # @param self: database object 
    # @param table_name: name of specified table to be inserted into 
    # @param values: values: column values to be inserted 
    # !STATUS: done, error handling done, working condition, not tested for corner cases
    def insert(self, table_name, values):

        # Table does not exist
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE
        
        # Get table number 
        table = self.get_table_id(table_name)

        # Row does not have enough column values
        if (len(self.tables[table][1]) != len(values)):
            raise PacketError(7)                # BAD_ROW

        # Set packet bytes for values paramater 
        req = b''
        for i, value in enumerate(values):
            # Column value is not of correct type 
            if (type(value) is not self.tables[table][1][i][1]):
                raise PacketError(6)            # BAD_VALUE

            if (type(value) is str):
                if (len(value) < 1): 
                    raise PacketError(6)            # BAD_VALUE 

                size = 4 * math.ceil(len(value)/4)
                buf = value.encode('ASCII') + (b'\x00'*(size - len(value)) if size > len(value) else b'') 
                req = req + struct.pack('>ii', 3, size) + buf
            elif (type(value) is int):
                req = req + struct.pack('>iiq', 1, 8, value)
            elif (type(value) is float):
                req = req + struct.pack('>iid', 2, 8, value)

        # Send request
        send_req = struct.pack('>iii', 1, table, len(values)) + req
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4096)
        response = struct.unpack('!iqq', data)
        
        # Handle request
        if (response[0] is not 1):
            raise Exception(10)                 # SERVER_BUSY
        
        return response[1], response[2]
        
                
    def update(self, table_name, pk, values, version=None):
        # TODO: implement me
        pass

    def drop(self, table_name, pk):
        # Table does not exist
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE
        
        # Get table number 
        table = self.get_table_id(table_name)

        # Send request
        send_req = struct.pack('>iiq', 3, table, pk)
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4096)
        response = struct.unpack('!i', data)

        # Handle request
        if (response[0] is not 1):
            raise Exception(10)                 # SERVER_BUSY

        return
        
        
    def get(self, table_name, pk):
        # Table does not exist
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE
        
        # Get table number 
        table = self.get_table_id(table_name)

        # Send request
        send_req = struct.pack('>iiq', 4, table, pk)
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4096)
        print(data)
        
        return

    def scan(self, table_name, op, column_name=None, value=None):
        # TODO: implement me
        pass

    def get_table_id(self, table_name):
        return self.table_names.index(table_name)
                        
