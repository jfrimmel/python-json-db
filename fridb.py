"""
Provide a simple single-file-base database.

This module allows a very simple interface to a single-file-database. The
entire data is stored in one JSON file.
The provided interface to the database is similar to some of the common SQL
commands, but keep in mind, that it is in fact no SQL database.
There are several simple methods, that operate on the data. Those are:
* create(): create a new database
* connect(): connect to an existing database
* disconnect(): closes the connection to the database. All data is stored.
* create_table(): create a new table in the database
* drop_table(): delete a table with all of its entries
* tables(): return a list of all tables
* insert(): insert a data set into a table
* update(): changes the content of a specified row
* delete(): delete a specified row
* read(): query data sets from a table
* save(): save the memory to the file. Automatically called on disconnect().
* select(): query data and their IDs from a table

Usage:
    The basic usage is very simple: you have to import this module. After that
    you can either connect to a existing database or create a new one and store
    the return value in a variable, which you can use to interact with the
    database.
    You can insert data sets and read the existing ones back. You may limit the
    number of data sets to read using the optional parameter 'limit'.

Example:
    import fridb

    db = fridb.create('test.db')
    db.create_table('test')
    db.insert('test', 'item #1')
    db.insert('test', 'item #2')
    db.save()
    db.insert('test', 'item #3')
    db.disconnect()

    db = fridb.connect('test.db')
    print(db.tables())
    print(db.read('test'))

Tests:
    You can find rudimentary tests of the database in this section. Each set of
    test is prefixed with a short explanation what is tested an why.

    A newly created database has no entries in it.
    >>> db = create('test.db')
    >>> len(db.tables())
    0

    A created table increments the number of tables. The table returned has the
    same name as specified. It is empty after the creation.
    >>> db.create_table('customers')
    >>> db.tables()[0]
    'customers'
    >>> len(db.tables())
    1
    >>> len(db.read('customers'))
    0

    A new table with the same name as an already existing one raises an
    exception.
    >>> db.create_table('customers')
    Traceback (most recent call last):
      ...
    fridb.DBError: Table already exists.

    If an data set is inserted, the number of returned rows is incremented by
    one. The database returns exactly the inserted string.
    >>> db.insert('customers', 'hello, world!')
    >>> len(db.read('customers'))
    1
    >>> db.read('customers')[0]
    'hello, world!'

    You can neither read from or write to an non-existing table.
    >>> db.insert('non-existing table', 'data')
    Traceback (most recent call last):
      ...
    fridb.DBError: Table does not exist.
    >>> db.read('non-existing table')
    Traceback (most recent call last):
      ...
    fridb.DBError: Table does not exist.

    Test the limit option and ensure that the data sets are returned on the
    right order.
    >>> db.insert('customers', '2nd string')
    >>> len(db.read('customers'))
    2
    >>> len(db.read('customers', 1))
    1
    >>> len(db.read('customers', -1))
    1
    >>> db.read('customers')[0]
    'hello, world!'
    >>> db.read('customers')[1]
    '2nd string'
    >>> db.read('customers', -1)[0]
    '2nd string'

    Select returns the ID and the data as a tuple. The IDs start with zero.
    >>> db.select('customers')
    [(0, 'hello, world!'), (1, '2nd string')]

    Select raises an DBError if a non-existing table should be queried.
    >>> db.select('no such table')
    Traceback (most recent call last):
      ...
    fridb.DBError: Table does not exist.

    Update updates the right entry.
    >>> db.update('customers', 1, 'a new string')
    >>> db.select('customers')
    [(0, 'hello, world!'), (1, 'a new string')]

    Update raises an DBError if a non-existing table should be updated or if
    the ID to update is non-existent.
    >>> db.update('no such table', 0, '')
    Traceback (most recent call last):
      ...
    fridb.DBError: Table does not exist.
    >>> db.update('customers', 2, 'a newer string')
    Traceback (most recent call last):
      ...
    fridb.DBError: Modifying of an entry that is not in the database.
    >>> db.update('customers', -1, 'a newer string')
    Traceback (most recent call last):
      ...
    fridb.DBError: Modifying of an entry that is not in the database.

    The data is inserted in the right table.
    >>> db.create_table('orders')
    >>> db.insert('orders', 'item #1')
    >>> db.insert('customers', 'item #2')
    >>> db.read('orders', limit=-1)[0]
    'item #1'
    >>> db.read('customers', limit=-1)[0]
    'item #2'

    Ensure that the file object is closed, if the database connection is
    closed. All calls to the public methods of the database object except for
    disconnect() are raising exceptions after that point. disconnect() has no
    effect in that case.
    >>> db.disconnect()
    >>> db.insert('customers', 'should not be inserted')
    Traceback (most recent call last):
      ...
    fridb.DBError: Database file is closed
    >>> db.read('customers')
    Traceback (most recent call last):
      ...
    fridb.DBError: Database file is closed
    >>> db.disconnect()

    The existing database has the same entries as the one before the saving.
    >>> db = connect('test.db')
    >>> db.read('customers')
    ['hello, world!', 'a new string', 'item #2']
    >>> db.read('orders')
    ['item #1']

    Delete removes an existing row from an table. All other rows and their IDs
    remain unchanged. Both the table and the ID must exist.
    >>> db.delete('customers', 1)
    >>> db.select('customers')
    [(0, 'hello, world!'), (2, 'item #2')]
    >>> db.delete('unknown-table', 0)
    Traceback (most recent call last):
      ...
    fridb.DBError: Table does not exist.
    >>> db.delete('customers', 3)
    Traceback (most recent call last):
      ...
    fridb.DBError: Deleting an entry that doesn't exist.
    >>> db.delete('customers', -1)
    Traceback (most recent call last):
      ...
    fridb.DBError: Deleting an entry that doesn't exist.
    >>> db.delete('customers', 0)
    >>> db.select('customers')
    [(2, 'item #2')]

    Update and insert are both working even after the deletion of any items.
    >>> db.update('customers', 2, 'new item #2')
    >>> db.insert('customers', 'item #3')
    >>> db.select('customers')
    [(2, 'new item #2'), (3, 'item #3')]

    A table can be dropped. The table and its content is no more available.
    >>> db.drop_table('orders')
    >>> db.tables()
    ['customers']
    >>> db.read('orders')
    Traceback (most recent call last):
      ...
    fridb.DBError: Table does not exist.

    The database has to be capable of storing at least 10,000 data sets in a
    single table. The following lines first create a list with 20,000 entries,
    then stores the list entries on after another. After that the database is
    read and the two lists (the original and the one returned from the read())
    have to be equal. Every 1000 entries the list is stored and reloaded to
    simulate the disk-I/O.
    >>> def test_10thousand_entries():
    ...     db = create('test.db')
    ...     db.create_table('test')
    ...     entries = [i for i in range(20000)]
    ...     for index, entry in enumerate(entries):
    ...         if index % 1000 == 0:
    ...             db.save()
    ...         db.insert('test', entry)
    ...     returned_entries = db.read('test')
    ...     db.disconnect()
    ...     return returned_entries == entries
    >>> test_10thousand_entries()
    True

    Test, if an empty table initializes the highest ID with the right value.
    >>> db = create('test.db')
    >>> db.create_table('test')
    >>> db.disconnect()
    >>> db = connect('test.db')
    >>> db.insert('test', 'item at position 0')
    >>> db.select('test')
    [(0, 'item at position 0')]

    This is not a test. The following two statements clean up the test
    environment.
    >>> import os
    >>> os.remove('test.db')
"""
import os
import json


def connect(db_file):
    """
    Connect to a database file.

    This function tries to open the file 'db_file' in append mode (current
    content is kept) and sets the read pointer for this file to position 0.
    This is necessary, because the pointer is initially set to the position of
    the end of the file in order to write data directly to it without
    overwriting the existing data. In our case we want to write to the file as
    well as read from it, so the position is set to the beginning of the file.
    On success a new object of the class FriDB is created, which can be used
    to access the database-file. In the case of an error a DBError is raised.
    Note that the file is created, if it does not exist yet.
    :param db_file: The file that holds the database.
    :return: a FriDB object with the database file loaded.
    :rtype: FriDB
    :exception DBError: if the file could not be accessed.
    """
    try:
        file_pointer = open(str(db_file), 'a+')
        file_pointer.seek(0, os.SEEK_SET)
        if not os.path.isfile(str(db_file)):
            raise FileNotFoundError('File could not be created')
        return FriDB(file_pointer)
    except (FileNotFoundError, IOError, OSError):
        # pass to prevent exception during exception handling
        pass
    raise DBError('Could not access or create the database file.')


def create(db_file):
    """
    Create an empty database.

    This function creates a new database file in the file specified by the
    parameter. If a file with that name exists it will be overridden.
    On success a new object of the class FriDB is created, which can be used
    to access the database-file. In the case of an error a DBError is raised.
    :param db_file: The file that should hold the database.
    :return: a FriDB object with the database file loaded.
    :rtype: FriDB
    :exception DBError: if the file could not be accessed.
    """
    try:
        file_pointer = open(str(db_file), 'w+')
        if not os.path.isfile(str(db_file)):
            raise FileNotFoundError('File could not be created')
        return FriDB(file_pointer)
    except (FileNotFoundError, IOError, OSError):
        # pass to prevent exception during exception handling
        pass
    raise DBError('Could not access or create the database file.')


class FriDB:
    """
    A simple JSON-based database.

    The class provides methods to operate on a single JSON file. This file
    holds the entire database with all of its tables and their entries. The
    database provides no SQL interface but the user can call methods with
    similar names, which have the same (but mostly reduced) functionality.
    The class should not be created directly. It's recommended to use one of
    the two functions 'connect()' or 'create()' from this module.

    Note that every method of this class, except for disconnect, may raise an
    DBError if the file-object, that is used in the database, is closed. This
    exception is not listed in the method descriptions!
    """

    def __init__(self, fp):
        """
        Construct the database object.

        This method takes an open file pointer. At first it checks, if the
        passed parameter is in fact open and raises an exception if not. If the
        file is open and the file-size is zero a new database file is created,
        otherwise the existing one is loaded.

        Note, that it is recommended to use one of the two functions
        'fridb.connect()' or 'fridb.create()' instead of creating an object of
        this class directly. Those two functions do everything needed to have
        access to a database of a given file.
        :param fp: A open file object to the database file.
        """
        self._file = fp
        self._check_fp()
        self._db = None
        self._highest_id = {}
        if _get_file_size(fp) == 0:
            self._create_new_db()
        else:
            self._load_db()

    def _create_new_db(self):
        """Create a new database into an empty file."""
        self._db = {}
        self._highest_id = {}

    def _load_db(self):
        """
        Load the database from an existing file.

        The content of the database file is read and assumed as a JSON file and
        parsed in that way. If the parser succeeds the private variable '_db'
        is set up with the values of the existing database. On failure a
        DBError is raised.
        The highest ID in every table is set up in the list that stores the
        currently highest ID per table.
        The JSON is directly loaded into the _db variable, but it cannot
        differentiate between tuples and lists, so the tuple of ID and content
        has to be restored manually.
        :exception DBError: if the database file could not be decoded. The file
        is assumed to be corrupt in that case.
        """
        okay = True
        content = self._file.read()
        try:
            self._db = json.loads(content)
        except json.JSONDecodeError:
            okay = False
        else:
            for table in self._db:
                self._db[table] = [(row[0], row[1]) for row in self._db[table]]

            for table in self._db:
                self._highest_id[table] = int(max(
                    self._db[table],
                    key=lambda item: item[1]
                )[0]) if self._db[table] else -1

        if not okay:
            raise DBError('Database file corrupt.')

    def save(self):
        """
        Store the database to a file.

        This methods takes the current database content and writes it to the
        file. The file is flushed afterwards to ensure that the data is not
        kept in a buffer but is written directly to the file.
        The data is written in the JSON format, where every table is an top-
        level item in the JSON and has an array of rows in it.

        It is recommended to call this method from time to time. If not the
        data is only written on a call to disconnect. If the user do not
        specify at least on of both calls the data is fully lost.
        """
        self._check_fp()
        self._file.seek(0)
        self._file.truncate()
        json.dump(self._db, self._file, sort_keys=False, indent=2)
        self._file.flush()

    def _check_fp(self):
        """Check, if the file is still open and raise a DBError if not."""
        if self._file.closed:
            raise DBError('Database file is closed')

    def _check_table(self, table):
        """Raise a DBError if the table doesn't exist."""
        key = str(table)
        if key not in self._db:
            raise DBError('Table does not exist.')

    def _is_id_in_table(self, row_id, table):
        """Return whether a given ID is in the given table."""
        return len([i for i in self._db[table] if i[0] == row_id]) != 0

    def create_table(self, tablename):
        """
        Create a new table in the database.

        This method creates a new table to store items to in the database. At
        least one table is required if any data should be stored inside the
        database.
        If the table does not exist yet it is created and has no items in it.
        If a table with the same name exists already a DBError is raised.
        :param tablename: The name of the table to create.
        :exception DBError: is raised if the table already exists.
        """
        self._check_fp()
        table = str(tablename)
        if table in self._db:
            raise DBError('Table already exists.')
        self._db[table] = []
        self._highest_id[table] = -1

    def tables(self):
        """Return a list of all existing tables."""
        return [key for key in self._db.keys()]

    def drop_table(self, tablename):
        """
        Delete a table.

        Deletes an entire table with all of its content. The table has to exist
        in the database.
        :param tablename: The name of the table to delete.
        :exception DBError: if the table does not exist.
        """
        self._check_fp()
        table = str(tablename)
        self._check_table(table)
        del self._db[table]

    def insert(self, table, data):
        """
        Insert one data set into a row of the database.

        The data is inserted as a string, so if you want to store an object,
        you will have to serialize it before (e.g. using the JSON format).
        The object takes a whole row for its own.
        The ID for the new row is the highest ID that was given in this session
        plus one.
        :param table: The table to store the entry into.
        :param data: The data to store.
        """
        self._check_fp()
        table = str(table)
        self._check_table(table)
        self._db[table].append((self._highest_id[table] + 1, data))
        self._highest_id[table] += 1

    def update(self, table, row_id, data):
        """
        Update an existing row with new data.

        The row has to be existent. It is identified over the ID of the row,
        which can be queried using select().
        :param table: The table to store the entry into.
        :param row_id: The ID of the entry to modify.
        :param data: The data that should be written.
        :exception DBError: if the ID is not in the table or the table doesn't
        exists.
        """
        self._check_fp()
        table = str(table)
        self._check_table(table)
        if not self._is_id_in_table(row_id, table):
            raise DBError('Modifying of an entry that is not in the database.')
        self._db[table] = [
            row if row[0] != row_id else (row_id, data)
            for row in self._db[table]
        ]

    def select(self, table, limit=0):
        """
        Select the entries from the database.

        The method returns an array of up to 'limit' rows. There are three
        possible cases for the limit:
        1. If the limit is zero all rows are returned.
        2. If the limit is positive the first n rows are returned.
        3. If the limit is negative the last n rows are returned.
        This method only operates on the private variable '_db', that has to
        be up-to-date at every call to read(). The other methods have to ensure
        this.
        The difference to read() is that this function returns a tuple
        containing the ID and the data, whereas read() does not.
        :param table: the table to read from.
        :param limit: This optional parameter specifies the maximum number of
        rows returned.
        :return: An array of tuples containing the ID and the stored data.
        :rtype: tuple array (id, content)
        """
        self._check_fp()
        table = str(table)
        self._check_table(table)

        ret = self._db[table]
        if limit < 0:
            ret = ret[limit:]
        elif limit > 0:
            ret = ret[:limit]
        return ret.copy()

    def read(self, table, limit=0):
        """
        Read the entries from the database.

        This function is similar to select(), but the IDs are not returned.
        The IDs are stripped and only the data in each row is stored in a list.
        :param table: the table to read from.
        :param limit: This optional parameter specifies the maximum number of
        rows returned.
        :return: An array containing the stored data.
        :rtype: string array
        """
        rows = self.select(table, limit)
        return [row for _, row in rows]

    def delete(self, table, row_id):
        """
        Delete an entry from a table.

        This method removes a row with a given ID from the database. The same
        circumstances as by insert() apply.
        :param table: The table to delete the entry from.
        :param row_id: The ID of the entry, that should be deleted.
        :exception DBError: if the ID is not in the table or the table doesn't
        exists.
        """
        self._check_fp()
        table = str(table)
        self._check_table(table)
        if not self._is_id_in_table(row_id, table):
            raise DBError('Deleting an entry that doesn\'t exist.')
        self._db[table] = [row for row in self._db[table] if row[0] != row_id]

    def disconnect(self):
        """
        Disconnect for the database.

        This method closes the connection to the database file. The modified
        data since the last call to save() is stored and the file object for
        the database file is closed. After this call all operations on the
        object will fail, except the call to this method, which has no effect.
        The private variable '_db' is set to an empty dictionary in order to
        prevent the access to the data in the memory after the connection was
        closed.
        """
        if not self._file.closed:
            self.save()
        self._file.close()
        self._db = {}


def _get_file_size(file_pointer):
    """
    Return the size of a file in bytes.

    The parameter has to be the file pointer to an open file, the size of which
    should be queried.

    If the file is empty the value 0 is returned (as you would expect).
    >>> file_pointer = open('python.doctest', 'w')
    >>> _get_file_size(file_pointer)
    0
    >>> bytes_written = file_pointer.write('Hello World!')
    >>> _get_file_size(file_pointer) == bytes_written
    True
    >>> file_pointer.close()
    >>> import os
    >>> os.remove('python.doctest')

    :param file_pointer: The valid file pointer to an open file.
    :return: The size of the file in bytes.
    """
    old_file_position = file_pointer.tell()
    file_pointer.seek(0, os.SEEK_END)
    size = file_pointer.tell()
    file_pointer.seek(old_file_position, os.SEEK_SET)
    return size


class DBError(Exception):
    """
    Custom exception for the FriDB.

    This class is an exception that is thrown by the class FriDB. Note that the
    class itself is only derived from the class Exception and has an empty
    body.

    It may or may not have a message.
    >>> raise DBError
    Traceback (most recent call last):
      ...
    fridb.DBError
    >>> raise DBError('Test exception')
    Traceback (most recent call last):
      ...
    fridb.DBError: Test exception
    """
