"""
Provide a simple single-file-base database.

You can find examples that also work as rudimentary tests of the database in 
the following section.

A newly created database has no entries in it.
>>> db = create('test.db')
>>> len(db.read())
0

If an data set is inserted, the number of returned rows is incremented by one.
The database returns exactly the inserted string.
>>> db.insert('hello, world!')
>>> len(db.read())
1
>>> db.read()[0]
'hello, world!'

Test the limit option and ensure that the data sets are returned on the right
order.
>>> db.insert('2nd string')
>>> len(db.read())
2
>>> len(db.read(1))
1
>>> db.read()[0]
'hello, world!'
>>> db.read()[1]
'2nd string'

Ensure that the file object is closed, if the database connection is closed.
All calls to the public methods of the database object except for disconnect()
are raising exceptions after that point. disconnect() has no effect in that
case.
>>> db.disconnect()
>>> db.insert('should not be inserted')
Traceback (most recent call last):
  ...
fridb.DBError: Database file is closed
>>> db.read()
Traceback (most recent call last):
  ...
fridb.DBError: Database file is closed
>>> db.disconnect()
"""
import os

def connect(db_file):
    """Connect to a database file."""
    fp = open(str(db_file), 'a+')
    return FriDB(fp)

def create(db_file):
    """Creates an empty database."""
    fp = open(str(db_file), 'a+')
    return FriDB(fp)

class FriDB:
    """A simple JSON-based database."""

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
        if _get_file_size(fp) == 0:
            self._create_new_db()
        else:
            self._load_db()

    def _create_new_db(self):
        """Create a new database into an empty file."""
        print('Creating a new database...')
        # TODO

    def _load_db(self):
        """Load the database from an existing file."""
        print('Loading the existing database...')
        # TODO

    def _check_fp(self):
        """Check, if the file is still open and raise an exception if not."""
        if self._file.closed:
            raise DBError('Database file is closed')

    def insert(self, object):
        """
        Insert one data set into a row of the database.

        The object is inserted as a string, so if you want to store an object,
        you will have to serialize it before (e.g. using the JSON format).
        The object takes a whole row for its own.
        :param object: The object to store.
        """
        self._check_fp()
        # TODO

    def read(self, limit=0):
        """
        Read the entries from the database.

        The method returns an array of up to 'limit' rows. If the limit is zero
        all rows are returned.
        :param limit: This optional parameter specifies the maximum number of
        rows returned.
        :return: An array of strings containing the stored data.
        :rtype: string array
        """
        self._check_fp()
        if limit < 0:
            raise DBError('Limit less than zero.')
        # TODO
        return []
    
    def disconnect(self):
        """Disconnect for the database file and close the file object."""
        self._file.close()

def _get_file_size(fp):
    """Return the size of a file in bytes."""
    return os.fstat(fp.fileno()).st_size

class DBError(Exception):
    """Custom exception thrown from the class FriDB."""
