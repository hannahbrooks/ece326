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

        if len(tables) < 2 and len(tables) != 0 :
            raise IndexError
        for table_name, cols in tables:
            if table_name[0] == "_":
                raise ValueError
            if type(table_name) is not str:
                raise TypeError
            if table_name in table_names:
                raise ValueError


            column_names = []
            for item in cols:
                if len(item) != 2:
                    raise IndexError
            for col_name, col_type in cols:
                if type(col_name) is not str:
                    raise TypeError
                if col_name[0].isdigit():
                    raise ValueError
                if col_name in column_names:
                    raise ValueError
                if col_name in ("id", "pk", "save", "delete", "get", "filter", "count"):
                    raise ValueError

                if col_type not in (str, float, int):
                    if col_type in (tuple, list):
                        raise ValueError
                    if col_type not in table_names:
                        raise IntegrityError


                column_names.append(col_name)
            table_names.append(table_name)

        self.tables = [None] + list(tables)
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
        return

    def insert(self, table_name, values):
        if (type(values) is not list):
            raise PacketError(4)                # BAD_QUERY
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
            # check foreign
            if self.tables[table][1][i][1] in self.table_names:
                try:
                    check_row, version = self.get(self.tables[table][1][i][1], value)
                except:
                    raise InvalidReference(2)
                if len(check_row) == 0:
                    raise InvalidReference(9)   #BAD_FOREIGN
                req = req + struct.pack('>iiq', 4, 8, value)
                continue 

            # Column value is not of correct type 
            if (type(value) != self.tables[table][1][i][1]):
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
            raise TransactionAbort                 # SERVER_BUSY
        data = self.socket.recv(16)
        response = struct.unpack('!qq', data)
        
        return response[0], response[1]
        
                
    def update(self, table_name, pk, values, version=None):
        if not (table_name in self.table_names):
            raise PacketError(3)                    # BAD_TABLE
        elif (type(pk) != int or type(values) != list):
            raise PacketError(4)                    # BAD_QUERY
        if pk > len(self.tables):
            raise ObjectDoesNotExist
        if type(version) is not int:
            if version is not None:
                raise PacketError(4)
        
        # Get table number 
        table = self.get_table_id(table_name)
        
        # Check values are valid 
        if len(values) is not len(self.tables[table][1]): 
            raise PacketError(7)                    # BAD_ROW

        # Prepare bytes to be sent 
        req = b''
        for i, value in enumerate(values):
            if self.tables[table][1][i][1] in self.table_names:
                try:
                    check_row, v = self.get(self.tables[table][1][i][1], value)
                except:
                    raise InvalidReference(2)
                if len(check_row) == 0:
                    raise InvalidReference(9)       #BAD_FOREIGN
                req = req + struct.pack('>iiq', 4, 8, value)
                continue

            # Column value is not of correct type 
            if (type(value) is not self.tables[table][1][i][1]):
                raise PacketError(6)                # BAD_VALUE

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
        if (version is None):
            version = 0
        send_req = struct.pack('>iiqqi', 2, table, pk, version, len(values)) + req
        self.socket.send(send_req)

        # Receieve request
        data = self.socket.recv(4)
        response = struct.unpack('!i', data)

        # Handle request
        if (response[0] is not 1):
            raise TransactionAbort                     # SERVER_BUSY
        data = self.socket.recv(1024)
        response = struct.unpack('!q', data)

        return response[0]

    def drop(self, table_name, pk):
        if (type(pk) != int or type(table_name) != str):
            raise PacketError(4)                         # BAD_QUERY
        if table_name not in self.table_names:
            raise PacketError(3)                         # BAD_TABLE
        if pk > len(self.tables):
            raise ObjectDoesNotExist

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
            raise ObjectDoesNotExist(response[0])                 # SERVER_BUSY

        return
        
        
    def get(self, table_name, pk):
        if (type(pk) != int):
            raise PacketError(4)                # BAD_QUERY
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

        if (response[0] is not 1):
            raise ObjectDoesNotExist(response[0])       # SERVER ERROR

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
            elif (response[0] is 4):
                data = self.socket.recv(8)
                value = struct.unpack('>q', data)
                values.append(value[0])

        return values, version

    def scan(self, table_name, op, column_name=None, value=None):
        if op not in range(1, 8):
            raise PacketError(4)
        if (type(op) is not int or type(table_name) is not str):
            raise PacketError(4)                # BAD_QUERY
        if (column_name != None and value == None):
            raise PacketError(4)                # BAD_QUERY
        if column_name is None and op != 1:
                raise PacketError(4)
        if not (table_name in self.table_names):
            raise PacketError(3)                # BAD_TABLE

        table = self.get_table_id(table_name)

        if (op == 1 or column_name == None or column_name == "id"):
            col = 0 
        else:
            if (column_name, type(value)) in self.tables[table][1]:
                col = self.tables[table][1].index((column_name, type(value))) + 1
            else:
                raise PacketError(4)                # BAD_QUERY
        
        # prepare struct value 
        if (col == 0):
            if (column_name == "id"):
                req = struct.pack('>iiq', 4, 8, value)
            else:
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
            raise PacketError(response[0])                 # SERVER ERROR

        data = self.socket.recv(4)
        response = struct.unpack('!i', data)
        if (response[0] == 0):
            return list([])

        data = self.socket.recv(4096)
        response = struct.unpack('!'+('q'*response[0]), data)

        return list(response)
        
    def janky(self):
        raise InvalidReference
