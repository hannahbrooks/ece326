#!/usr/bin/python3
#
# orm.py
#
# Definition for setup and export function
#

from .easydb import Database
import inspect

# Return a database object that is initialized, but not yet connected.
#   database_name: str, database name
#   module: module, the module that contains the schema
def setup(database_name, module):
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented"%(
            str(database_name)))         

    return Database(make_schema(module))

# Return a string which can be read by the underlying database to create the 
# corresponding database tables.
#   database_name: str, database name
#   module: module, the module that contains the schema
def export(database_name, module):

    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented"%(
            str(database_name)))

    schema = make_schema(module)

    text = ""
    for table_name, types in schema:
        text += table_name + " {\n"

        for column_name, t in types:
            text += "\t" + column_name + ": "
            if (t == float):
                text += "float;\n"
            elif t == int:
                text += "integer;\n"
            elif t == str:
                text += "string;\n"
            # elif t == Foreign:
                # not sure what to do

        text += "}\n\n"

    return text

def make_schema(module):
    schema = []
    
    for name, obj in inspect.getmembers(module):
        if '__' == name[:2] or name == 'datetime' or name == 'orm':
            continue

        if inspect.isclass(obj) and name != 'datetime':
            types = []
            for member, t in obj.__dict__.items():
                if '__' == member[:2]:
                    continue

                if 'String' in str(t):
                    types.append((member, str))
                elif 'Integer' in str(t):
                    types.append((member, int))
                elif 'Float' in str(t):
                    types.append((member, float))
                elif 'Coordinate' in str(t):
                    types.append(('Latitude', float))
                    types.append(('Longitude', float))
                elif 'Datetime' in str(t):
                    types.append((member, float))
                elif 'Foreign' in str(t):
                    # idk what to do
                    continue

        schema.append((name, tuple(types)))

    return schema

