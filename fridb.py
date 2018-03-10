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
* read(): query data sets from a table
* save(): save the memory to the file. Automatically called on disconnect().

update(), delete() etc. are missing entirely.
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

    The data is inserted in the right table.
    >>> db.create_table('orders')
    >>> db.insert('orders', 'item #1')
    >>> db.insert('customers', 'item #2')
    >>> db.read('orders', limit=-1)[0]
    'item #1'
    >>> db.read('customers', limit=-1)[0]
    'item #2'

    Ensure that the file object is closed, if the database connection is closed.
    All calls to the public methods of the database object except for disconnect()
    are raising exceptions after that point. disconnect() has no effect in that
    case.
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
    ['hello, world!', '2nd string', 'item #2']
    >>> db.read('orders')
    ['item #1']

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
    >>> def test_10thousand_entires():
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
    >>> test_10thousand_entires()
    True

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
        fp = open(str(db_file), 'a+')
        fp.seek(0, os.SEEK_SET)
        if not os.path.isfile(str(db_file)):
            raise FileNotFoundError('File could not be created')
        return FriDB(fp)
    except:
        # pass to prevent exception during exception handling
        pass
    raise DBError('Could not access or create the database file.')

def create(db_file):
    """
    Creates an empty database.

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
        fp = open(str(db_file), 'w+')
        if not os.path.isfile(str(db_file)):
            raise FileNotFoundError('File could not be created')
        return FriDB(fp)
    except:
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
        if _get_file_size(fp) == 0:
            self._create_new_db()
        else:
            self._load_db()

    def _create_new_db(self):
        """Create a new database into an empty file."""
        self._db = {}

    def _load_db(self):
        """
        Load the database from an existing file.

        The content of the database file is read and assumed as a JSON file and
        parsed in that way. If the parser succeeds the private variable '_db'
        is set up with the values of the existing database. On failure a 
        DBError is raised.
        """
        okay = True
        content = self._file.read()
        try:
            self._db = json.loads(content)
        except json.JSONDecodeError:
            okay = False

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
        """Check, if the file is still open and raise an exception if not."""
        if self._file.closed:
            raise DBError('Database file is closed')

    def _check_table(self, table):
        """Raise a DBError if the table doesn't exist."""
        key = str(table)
        if key not in self._db:
            raise DBError('Table does not exist.')

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
        table= str(tablename)
        self._check_table(table)
        del self._db[table]

    def insert(self, table, object):
        """
        Insert one data set into a row of the database.

        The object is inserted as a string, so if you want to store an object,
        you will have to serialize it before (e.g. using the JSON format).
        The object takes a whole row for its own.
        :param table: The table to store the entry into.
        :param object: The object to store.
        """
        self._check_fp()
        table = str(table)
        self._check_table(table)
        self._db[table].append(object)

    def read(self, table, limit=0):
        """
        Read the entries from the database.

        The method returns an array of up to 'limit' rows. There are three
        possible cases for the limit:
        1. If the limit is zero all rows are returned.
        2. If the limit is positive the first n rows are returned.
        3. If the limit is negative the last n rows are returned.
        This method only operates on the private variable '_db', that has to
        be up-to-date at every call to read(). The other methods have to ensure
        this.
        :param limit: This optional parameter specifies the maximum number of
        rows returned.
        :return: An array containing the stored data.
        :rtype: string array
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

def _get_file_size(fp):
    """
    Return the size of a file in bytes.

    The parameter has to be the file pointer to an open file, the size of which
    should be queried.

    If the file is empty the value 0 is returned (as you would expect).
    >>> fp = open('python.doctest', 'w')
    >>> _get_file_size(fp)
    0
    >>> bytes_written = fp.write('Hello World!')
    >>> _get_file_size(fp) == bytes_written
    True
    >>> fp.close()
    >>> import os
    >>> os.remove('python.doctest')

    :param fp: The valid file pointer to an open file.
    :return: The size of the file in bytes.
    """
    old_file_position = fp.tell()
    fp.seek(0, os.SEEK_END)
    size = fp.tell()
    fp.seek(old_file_position, os.SEEK_SET)
    return size

class DBError(Exception):
    """
    Custom exception for the FriDB.
    
    This class is an exception that is thrown by the class FriDB. Note that the
    class itself is only derived from the class Exception and has an empty body.

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
