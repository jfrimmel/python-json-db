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

### connect

### disconnect

### create_table

### tables

### drop_table

### save

### read

### insert

### delete
This is not implemented yet.

### update
This is not implemented yet.

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
Alternatively you can add the flag --verbose to get more information about the
tests.

## Links
The [FriServer](https://github.com/jfrimmel/FriServer) project uses this 
database in one of its plug-ins.
