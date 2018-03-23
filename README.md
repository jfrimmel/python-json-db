# FriDB
A simple single-file-base database operating on a JSON file. 
Does not support SQL.

## Table of Contents
- [Overview](#overview)
- [Example](#example)
- [Database access methods](#database-access-methods)
    - [create](#create)
    - [connect](#connect)
    - [disconnect](#disconnect)
    - [create_table](#create_table)
    - [tables](#tables)
    - [drop_table](#drop_table)
    - [save](#save)
    - [read](#read)
    - [insert](#insert)
    - [delete](#delete)
    - [update](#update)
- [Testing](#testing)
- [Links](#links)

## Overview
This database provides an easy-to-use interface to a single-file database. The
database does not support SQL, but offers some similar methods, that can be
called on the database object instead.

The database is stored in a single file. The file format is JSON. Each table
is represented trough an item/property in the JSON file and has an (maybe
empty) array as its value. The rows inserted into a table are stored in those
array one after another (so the order won't be changed, even if the database is
disconnected and reconnected later).

The database can easily store more than 10,000 entries in a single table. There
are no limits given by the database (only by the available memory and the run-
time) for both tables and the rows in a table.

## Example
The example creates a simple database to store customers and orders in a shop.
```python
import fridb

# create a new database with two tables in it
db = fridb.create('shop.db')
db.create_table('customers')
db.create_table('orders')

# insert an order and a customer
db.insert('customers', 'John Smith')
db.insert('orders', 'item #1')
db.insert('orders', 'item #2')

# insert another order and a new customer
db.insert('customers', 'Julia Miller')
db.insert('orders', 'item #1')

# print the last two orders
print(db.read('orders', limit=-2))
```
In reality you should add something like an ID to each data set in order to be
able to differentiate between those sets. But for a short demonstration this
should be enough.

## Database access methods
This section gives you a short overview what methods to interact with the
database are available and how they can be used.
### create
This function creates a new database with no entries or tables in it. You have
to specify a file, where the database is stored to. Note that an existing
database with the same filename is overridden.

It returns a FriDB object, that can be used to interact with the database.

### connect
This function connects to an existing database. If there is no database file at
the path available or the file is empty a new database is created. The existing
content is loaded into the memory database.

It returns a FriDB object, that can be used to interact with the database.

### disconnect
This method closes the connection to the database. The data is written to the
database file and the file is closed. After a call to this method there is no
access to the data in the database and every other call to a public method of
the object fails.

Note that you have to call at least either disconnect() or save() to store the
database state or your data will be lost. Although a call to save() and one to
close() on the file descriptor does nearly the same work it is recommended to
use disconnect().

### create_table
This method creates a new table, that can hold database entries. It can take
any string as its name.

At least one table is required if any data should be stored in the database.

This is the equivalent to the SQL command `CREATE TABLE ...`.

### tables
This method returns a list of all tables that are stored inside the database.

This is the equivalent to the SQL command `SHOW TABLES;`.

### drop_table
This method deletes an entire table and all of its entries.

Note that the change is not directly written to the file but stored in the
memory. The change is written if save() or disconnect() is called.

This is the equivalent to the SQL command `DROP TABLE ...`.

### save
This method writes the changes made to the database (which are done only in the
memory) to the database file. 

Note that you have to call at least either disconnect() or save() to store the
database state or your data will be lost. Although a call to save() and one to
close() on the file descriptor does nearly the same work it is recommended to
use disconnect().

### read
This method returns a list of all entries in a table. The number of returned
entries can be unlimited (limit=0, default) or limited to the first n (limit>0)
or the last n entries (limit<0).

This is roughly equivalent to the SQL 
`SELECT * FROM ... [SORT ASC|DESC] [LIMIT ]` statement, but you can only
query all statements, with the option to limit the number of rows returned.
Currently you cannot specify a filter in the statement.

### select
Similar to read(), but for each row a tuple of two is returned, where the first
entry is the ID and the second one is the data. This can be used for specifing
an entry in a table (e.g. for updating or deleting).

### insert
Insert a new data set into a table. It is always inserted at the end of the
table entries.

Note that the change is not directly written to the file but stored in the
memory. The change is written if save() or disconnect() is called.

This is the equivalent to the SQL command `INSERT ... INTO TABLE ...`.

### delete
Delete an entry from a table.

This method removes a row with a given ID from the database. The same
circumstances as by insert() apply.

### update
Update an existing row with new data.

The row has to be existent. It is identified over the ID of the row,
which can be queried using select(). It throws an DBError if the ID is not in
the table or the table doesn't exists.

This is the limited equivalent to the SQL `UPDATE` statement.

## Testing
The module is tested using the python module 'doctest'. The docstrings for the
module and some functions include the docstring tests. All methods of the
class FriDB are tested in the module test, but they don't have a test in their
own docstrings.

To invoke the tests type the following command in the directory with the file
fridb.py:
```bash
$ python -m doctest fridb.py
```
Alternatively you can add the flag `--verbose` to get more information about
the tests.

## Links
The [FriServer](https://github.com/jfrimmel/FriServer) project uses this 
database in one of its plug-ins.
