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
            self.close()
            return False
        
        return True
               
    def close(self):

        req = struct.pack('>iii', 6, 1, 0)
        self.socket.send(req)
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        sys.exit()

        return

    def insert(self, table_name, values):

        # Incorrect argument 
        if (type(values) is not list):
            raise PacketError(4)                # BAD_QUERY

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
        data = self.socket.recv(4)
        response = struct.unpack('!i', data)
        
        # Handle request
        if (response[0] is not 1):
            raise Exception(response[0])                 # SERVER_BUSY
        data = self.socket.recv(16)
        response = struct.unpack('!qq', data)
        
        return response[0], response[1]
        
                
    def update(self, table_name, pk, values, version=None):

        # Error handling 
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE
        elif (type(pk) != int or type(values) != list):
            raise PacketError(4)                # BAD_QUERY
        
        # Get table number 
        table = self.get_table_id(table_name)
        
        # Check values are valid 
        if len(values) is not len(self.tables[table][1]): 
            raise PacketError(7)                # BAD_ROW

        # Prepare bytes to be sent 
        req = b''
        for i, value in enumerate(values):
            # Column value is not of correct type 
            if (type(value) is not self.tables[table][1][i][1]):
                raise PacketError(6)            # BAD_VALUE

            if (type(value) is str):
                if (len(value) < 1): 
                    raise PacketError(6)        # BAD_VALUE 

                size = 4 * math.ceil(len(value)/4)
                buf = value.encode('ASCII') + (b'\x00'*(size - len(value)) if size > len(value) else b'') 
                req = req + struct.pack('>ii', 3, size) + buf
            elif (type(value) is int):
                req = req + struct.pack('>iiq', 1, 8, value)
            elif (type(value) is float):
                req = req + struct.pack('>iid', 2, 8, value)

        # Send request
        if (version == None):
            version = 0 
        send_req = struct.pack('>iiqqi', 2, table, pk, version, len(values)) + req
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4)
        response = struct.unpack('!i', data)

        # Handle request
        if (response[0] is not 1):
            raise Exception(response[0])          # SERVER_BUSY
        data = self.socket.recv(1024)
        response = struct.unpack('!q', data)

        return response[0]

    def drop(self, table_name, pk):

        if (type(pk) != int or type(table_name) != str):
            raise PacketError(4)                # BAD_QUERY
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
        
        if (type(pk) != int or type(table_name) != str):
            raise PacketError(4)                # BAD_QUERY
        # Table does not exist
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE
        
        # Get table number 
        table = self.get_table_id(table_name)

        # Send request
        send_req = struct.pack('>iiq', 4, table, pk)
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4)
        response = struct.unpack('>i', data)
        print(response[0])
        if (response[0] is not 1):
            raise Exception(response[0])                 # SERVER ERROR

        data = self.socket.recv(12)
        response = struct.unpack('>qi', data)
        version = response[0]
        values = []
        for i in range(0, response[1]):
            data = self.socket.recv(8)
            response = struct.unpack('>ii', data)
            if (response[0] is 1): # int
                data = self.socket.recv(8)
                value = struct.unpack('>q', data)
                values.append(value[0])
            elif (response[0] is 2): # float 
                data = self.socket.recv(8)
                value = struct.unpack('>d', data)
                values.append(value[0])
            elif (response[0] is 3): 
                data = self.socket.recv(response[1])
                data = data.decode()
                data = data.strip("\x00")
                values.append(data)

        return values, version

    class operator:
        AL = 1  # everything
        EQ = 2  # equal 
        NE = 3  # not equal
        LT = 4  # less than
        GT = 5  # greater than 
        LE = 6  # you do not have to implement the following two
        GE = 7

    def scan(self, table_name, op, column_name=None, value=None):
        
        if (type(op) is not int or type(table_name) is not str):
            raise PacketError(4)                # BAD_QUERY
        if (column_name != None and value == None):
            raise PacketError(4)                # BAD_QUERY
        # Table does not exist
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE
        # Get table number 
        table = self.get_table_id(table_name)

        if (op == 1 or column_name == None):
            col = 0 
        else:
            if (column_name, type(value)) in self.tables[table][1]:
                col = self.tables[table][1].index((column_name, type(value)))
            else:
                raise PacketError(4)                # BAD_QUERY
        
        # prepare struct value 
        if (col == 0):
            req = struct.pack('>ii', 0, 0)
        elif (type(value) is int):
            req = struct.pack('>iiq', 1, 8, value)
        elif (type(value) is float):
            req = struct.pack('>iid', 2, 8, value)
        elif type(value) is str:
            size = 4 * math.ceil(len(value)/4)
            buf = value.encode('ASCII') + (b'\x00'*(size - len(value)) if size > len(value) else b'') 
            req = struct.pack('>ii', 3, size) + buf

        send_req = struct.pack('>iiii', 5, table, col, op) + req
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4)
        response = struct.unpack('!i', data)

        # Handle request
        if (response[0] is not 1):
            raise Exception(response[0])                 # SERVER ERROR

        data = self.socket.recv(4)
        response = struct.unpack('!i', data)
        data = self.socket.recv(4096)
        response = struct.unpack('!'+('q'*response[0]), data)

        return list(response)
        
                        
