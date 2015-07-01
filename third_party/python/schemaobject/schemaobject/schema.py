from schemaobject.connection import DatabaseConnection
from schemaobject.database import DatabaseSchemaBuilder


class SchemaObject(object):
    """
    Object representation of a single MySQL instance.
    If database name is not specified in ``connection_url``,
    all databases on the MySQL instance will be loaded.

    ``connection_url`` - the database url as per `RFC1738 <http://www.ietf.org/rfc/rfc1738.txt>`_

      >>> schema  = schemaobject.SchemaObject('mysql://username:password@localhost:3306/sakila')
      >>> schema.host
      'localhost'
      >>> schema.port
      3306
      >>> schema.user
      'username'
      >>> schema.version
      '5.1.30'
      >>> schema.selected.name
      'sakila'

    """

    def __init__(self, connection_url):

        self._databases = None

        self.connection = DatabaseConnection()
        self.connection.connect(connection_url)

        self.host = self.connection.host
        self.port = self.connection.port
        self.user = self.connection.user
        self.version = self.connection.version

    @property
    def selected(self):
        """
        Returns the DatabaseSchema object associated with the database name in the connection url

          >>> schema.selected.name
          'sakila'
        """
        if self.connection.db:
            return self.databases[self.connection.db]
        else:
            return None

    @property
    def databases(self):
        """
        Lazily loaded dictionary of the databases within this MySQL instance.

        See DatabaseSchema for usage::

          #if database name is specified in the connection url
          >>> len(schema.databases)
          1
          >>> schema.databases.keys()
          ['sakila']

        """
        if self._databases == None:
            self._databases = DatabaseSchemaBuilder(instance=self)
        return self._databases
