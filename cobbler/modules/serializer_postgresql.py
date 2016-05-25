"""
Cobbler's Postgresql >=9.4 database based object serializer.
Experimental version.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
02110-1301  USA
"""

import ConfigParser
import distutils.sysconfig
import json

pgsql_loaded = False
try:
    import psycopg2
    pgsql_loaded = True
except:
    # FIXME: log message
    pass

import sys
import yaml

plib = distutils.sysconfig.get_python_lib()
mod_path = "%s/cobbler" % plib
sys.path.insert(0, mod_path)

from cexceptions import CX

defaults = {"host": "localhost",
            "user": "cobbler",
            "password": "cobbler",
            "db": "cobbler"}
cp = ConfigParser.ConfigParser(defaults)
cp.read("/etc/cobbler/pgsql.conf")

host = cp.get("connection", "host")
user = cp.get("connection", "user")
password = cp.get("connection", "password")
db = cp.get("connection", "db")


def __connect():
    # TODO: detect connection error
    global pgsql_conn
    global pgsql_cur
    try:
        pgsql_conn = psycopg2.connect("""dbname=%s
                                          host=%s
                                          user=%s
                                          password=%s""" % (db, host, user,
                                                            password))
        pgsql_cur = pgsql_conn.cursor()
    except Exception as e:
        # FIXME: log error
        raise CX("Unable to connect to DB server")


def register():
    """
    The mandatory cobbler module registration hook.
    """
    # FIXME: only run this if enabled.
    if not pgsql_loaded:
        return ""
    return "serializer"


def what():
    """
    Module identification function
    """
    return "serializer/postgresql"


def serialize_item(collection, item):
    """
    Save a collection item to database

    @param Collection collection collection
    @param Item item collection item
    """
    if item.name is None or item.name == "":
        raise exceptions.RuntimeError("name unset for item!")

    _dict = item.to_datastruct()

    __connect()
    query = "SELECT id FROM cobbler WHERE doc->>'name' = %s"
    pgsql_cur.execute(query, (item.name,))
    if pgsql_cur.rowcount:
        ins_id = pgsql_cur.fetchone()[0]
        query = "UPDATE cobbler SET doc = %s WHERE id = %s;"
        pgsql_cur.execute(query, (json.dumps(_dict), ins_id))
    else:
        query = "INSERT INTO cobbler (type, doc) VALUES (%s, %s);"
        pgsql_cur.execute(query,
                          (collection.collection_type(), json.dumps(_dict)))
    pgsql_conn.commit()


def serialize_delete(collection, item):
    """
    Delete a collection item from database

    @param Collection collection collection
    @param Item item collection item
    """

    __connect()

    query = "DELETE FROM cobbler WHERE type = %s and doc->>'name' = %s"
    pgsql_cur.execute(query, (collection.collection_type(), item.name))
    pgsql_conn.commit()


def serialize(collection):
    """
    Save a collection to database

    @param Collection collection collection
    """

    # TODO: error detection
    ctype = collection.collection_type()
    if ctype != "settings":
        for x in collection:
            serialize_item(collection, x)


def deserialize_raw(collection_type):

    # FIXME: code to load settings file should not be replicated in all
    #   serializer subclasses
    if collection_type == "settings":
        fd = open("/etc/cobbler/settings")
        _dict = yaml.safe_load(fd.read())
        fd.close()

        # include support
        for ival in _dict.get("include", []):
            for ifile in glob.glob(ival):
                with open(ifile, 'r') as fd:
                    _dict.update(yaml.safe_load(fd.read()))

        return _dict
    else:
        results = []
        __connect()
        query = "SELECT doc FROM cobbler WHERE type = %s;"
        ret = pgsql_cur.execute(query, (collection_type,))
        for x in pgsql_cur.fetchall():
            results.append(x[0])
        return results


def deserialize(collection, topological=True):
    """
    Populate an existing object with the contents of datastruct.
    Object must "implement" Serializable.
    """
    datastruct = deserialize_raw(collection.collection_type())
    if topological and type(datastruct) == list:
        datastruct.sort(__depth_cmp)
    collection.from_datastruct(datastruct)
    return True


def __depth_cmp(item1, item2):
    d1 = item1.get("depth", 1)
    d2 = item2.get("depth", 1)
    return cmp(d1, d2)

# EOF
