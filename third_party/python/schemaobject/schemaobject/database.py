from schemaobject.option import SchemaOption
from schemaobject.table import TableSchemaBuilder
from schemaobject.collections import OrderedDict


def DatabaseSchemaBuilder(instance):
    """
    Returns a dictionary loaded with all of the databases availale on
    the MySQL instance. ``instance`` must be an instance SchemaObject.

    .. note::
      This function is automatically called for you and set to
      ``schema.databases`` when you create an instance of SchemaObject

    """
    conn = instance.connection
    d = OrderedDict()
    sql = """
        SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME,
               DEFAULT_COLLATION_NAME
               FROM information_schema.SCHEMATA
        """
    if conn.db:
        sql += " WHERE SCHEMA_NAME = %s"
        # VoltDB patch: Made it a list, as required by conn.execute().
        params = [conn.db]
    else:
        params = None

    databases = conn.execute(sql, params)

    if not databases:
        return d

    for db_info in databases:

        name = db_info['SCHEMA_NAME']

        db = DatabaseSchema(name=name, parent=instance)
        db.options['charset'] = SchemaOption("CHARACTER SET", db_info['DEFAULT_CHARACTER_SET_NAME'])
        db.options['collation'] = SchemaOption("COLLATE", db_info['DEFAULT_COLLATION_NAME'])

        d[name] = db

    return d


class DatabaseSchema(object):
    """
    Object representation of a single database schema
    (as per `CREATE DATABASE Syntax <http://dev.mysql.com/doc/refman/5.0/en/create-database.html>`_).
    Supports equality and inequality comparison of DatabaseSchemas.

    ``name`` is the database name.
    ``parent`` is an instance of SchemaObject

        >>> for db in schema.databases:
        ...     print schema.databases[db].name
        ...
        sakila
        >>> schema.databases['sakila'].name
        'sakila'

    .. note::
        DatabaseSchema objects are automatically created for you
        by DatabaseSchemaBuilder and loaded under ``schema.databases``
    """

    def __init__(self, name, parent):
        self.parent = parent
        self.name = name
        self._options = None
        self._tables = None

    @property
    def tables(self):
        """
        Lazily loaded dictionary of all the tables within this database. See TableSchema for usage
          >>> len(schema.databases['sakila'].tables)
          16
        """
        if self._tables == None:
            self._tables = TableSchemaBuilder(database=self)
        return self._tables

    @property
    def options(self):
        """
        Dictionary of the supported MySQL database options. See OptionSchema for usage.

        * CHARACTER SET  == ``options['charset']``
        * COLLATE == ``options['collation']``
        """
        if self._options == None:
            self._options = OrderedDict()
        return self._options

    def select(self):
        """
        Generate the SQL to select this database
          >>> schema.databases['sakila'].select()
          'USE `sakila`;'
        """
        return "USE `%s`;" % self.name

    def alter(self):
        """
        Generate the SQL to alter this database
          >>> schema.databases['sakila'].alter()
          'ALTER DATABASE `sakila`'
        """
        return "ALTER DATABASE `%s`" % self.name

    def create(self):
        """
        Generate the SQL to create this databse
          >>> schema.databases['sakila'].create()
          'CREATE DATABASE `sakila` CHARACTER SET=latin1 COLLATE=latin1_swedish_ci;'
        """
        return "CREATE DATABASE `%s` %s %s;" % (self.name, self.options['charset'].create(), self.options['collation'].create())

    def drop(self):
        """
        Generate the SQL to drop this database
          >>> schema.databases['sakila'].drop()
          'DROP DATABASE `sakila`;'
        """
        return "DROP DATABASE `%s`;" % self.name

    def __eq__(self, other):
        if not isinstance(other, DatabaseSchema):
            return False
        return ((self.options['charset'] == other.options['charset'])
                and (self.options['collation'] == other.options['collation'])
                and (self.name == other.name)
                and (self.tables == other.tables))

    def __ne__(self, other):
        return not self.__eq__(other)
