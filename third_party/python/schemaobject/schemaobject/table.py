import re
from schemaobject.collections import OrderedDict
from schemaobject.column import ColumnSchemaBuilder
from schemaobject.index import IndexSchemaBuilder
from schemaobject.foreignkey import ForeignKeySchemaBuilder
from schemaobject.option import SchemaOption

REGEX_MULTI_SPACE = re.compile('\s\s+')


def TableSchemaBuilder(database):
    """
    Returns a dictionary loaded with all of the tables available in the database.
    ``database`` must be an instance of DatabaseSchema.

    .. note::
      This function is automatically called for you and set to
      ``schema.databases[name].tables`` when you create an instance of SchemaObject
    """
    conn = database.parent.connection

    t = OrderedDict()
    sql = """
            SELECT TABLE_NAME, ENGINE, ROW_FORMAT, AUTO_INCREMENT,
                    CREATE_OPTIONS, TABLE_COLLATION, TABLE_COMMENT
            FROM information_schema.`TABLES`
            WHERE TABLE_SCHEMA='%s'
            AND not isnull(ENGINE)
        """
    tables = conn.execute(sql % database.name)

    if not tables:
        return t

    for table_info in tables:

        name = table_info['TABLE_NAME']

        if "TABLE_COLLATION" not in table_info:
            charset = None

        pos = table_info['TABLE_COLLATION'].find('_')

        if not pos:
            charset = table_info['TABLE_COLLATION']
        else:
            charset = table_info['TABLE_COLLATION'][:pos]

        table = TableSchema(name=name, parent=database)
        table.options['engine'] = SchemaOption('ENGINE', table_info['ENGINE'])
        table.options['charset'] = SchemaOption("CHARSET", charset)
        table.options['collation'] = SchemaOption("COLLATE", table_info['TABLE_COLLATION'])
        table.options['row_format'] = SchemaOption('ROW_FORMAT', table_info['ROW_FORMAT'])
        table.options['auto_increment'] = SchemaOption('AUTO_INCREMENT', table_info['AUTO_INCREMENT'])
        table.options['create_options'] = SchemaOption(None, table_info['CREATE_OPTIONS'])
        table.options['comment'] = SchemaOption('COMMENT', table_info['TABLE_COMMENT'])

        t[name] = table
    return t


class TableSchema(object):
    """
    Object representation of a single table
    (as per `CREATE TABLE Syntax <http://dev.mysql.com/doc/refman/5.0/en/create-table.html>`_).
    Supports equality and inequality comparison of TableSchema.

    ``name`` is the column name.
    ``parent`` is an instance of DatabaseSchema

    .. note::
      TableSchema objects are automatically created for you by TableSchemaBuilder
      and loaded under ``schema.databases[name].tables``

    .. note::
      table options ``auto_increment``, ``comment`` are ignored in ``__eq__``, ``__neq__`` comparisons.

    Example

      >>> schema.databases['sakila'].tables.keys()
      ['actor', 'address', 'category', 'city', 'country', 'customer', 'film',
        'film_actor', 'film_category', 'film_text', 'inventory', 'language',
        'payment', 'rental', 'staff', 'store']
      >>> schema.databases['sakila'].tables['rental'].options.keys()
      ['engine', 'charset', 'collation', 'row_format', 'auto_increment', 'create_options', 'comment']
      >>> schema.databases['sakila'].tables['rental'].indexes.keys()
      ['PRIMARY', 'rental_date', 'idx_fk_inventory_id', 'idx_fk_customer_id', 'idx_fk_staff_id']
      >>> schema.databases['sakila'].tables['rental'].foreign_keys.keys()
      ['fk_rental_customer', 'fk_rental_inventory', 'fk_rental_staff']

    Table Attributes

      >>> schema.databases['sakila'].tables['rental'].name
      'rental'

    Table Options

    * ENGINE  == ``options['engine']``
    * CHARSET, CHARACTER SET == ``options['charset']``
    * COLLATE  == ``options['collation']``
    * ROW_FORMAT  == ``options['row_format']``
    * AUTO_INCREMENT  == ``options['auto_increment']``
    * CREATE_OPTIONS == ``options['create_options']``
    * COMMENT  == ``options['comment']``
    """

    def __init__(self, name, parent):
        self.parent = parent
        self.name = name
        self._options = None
        self._columns = None
        self._indexes = None
        self._foreign_keys = None

    @property
    def columns(self):
        """
        Lazily loaded dictionary of all the columns within this table. See ColumnSchema for usage

          >>> len(schema.databases['sakila'].tables['rental'].columns)
          7
          >>> schema.databases['sakila'].tables['rental'].columns.keys()
          ['rental_id', 'rental_date', 'inventory_id', 'customer_id', 'return_date', 'staff_id', 'last_update'
        """
        if self._columns == None:
            self._columns = ColumnSchemaBuilder(table=self)
        return self._columns

    @property
    def indexes(self):
        """
        Lazily loaded dictionary of all the indexes within this table. See IndexSchema for usage

          >>> len(schema.databases['sakila'].tables['rental'].indexes)
          5
          >>> schema.databases['sakila'].tables['rental'].indexes.keys()
          ['PRIMARY', 'rental_date', 'idx_fk_inventory_id', 'idx_fk_customer_id', 'idx_fk_staff_id']
        """
        if self._indexes == None:
            self._indexes = IndexSchemaBuilder(table=self)
        return self._indexes

    @property
    def foreign_keys(self):
        """
        Lazily loaded dictionary of all the foreign keys within this table. See ForeignKeySchema for usage

          >>> len(schema.databases['sakila'].tables['rental'].foreign_keys)
          3
          >>> schema.databases['sakila'].tables['rental'].foreign_keys.keys()
          ['fk_rental_customer', 'fk_rental_inventory', 'fk_rental_staff']
        """
        if self._foreign_keys == None:
            self._foreign_keys = ForeignKeySchemaBuilder(table=self)
        return self._foreign_keys

    @property
    def options(self):
        """
        Dictionary of the supported MySQL table options. See OptionSchema for usage.

        * ENGINE  == ``options['engine']``
        * CHARSET, CHARACTER SET == ``options['charset']``
        * COLLATE  == ``options['collation']``
        * ROW_FORMAT  == ``options['row_format']``
        * AUTO_INCREMENT  == ``options['auto_increment']``
        * CREATE_OPTIONS == ``options['create_options']``
        * COMMENT  == ``options['comment']``
        """
        if self._options == None:
            self._options = OrderedDict()
        return self._options

    def alter(self):
        """
        Generate the SQL to alter this table
          >>> schema.databases['sakila'].tables['rental'].alter()
          'ALTER TABLE `rental`'
        """
        return "ALTER TABLE `%s`" % (self.name)

    def create(self):
        """
        Generate the SQL to create a this table
          >>> schema.databases['sakila'].tables['rental'].create()
          'CREATE TABLE `rental` (
          `rental_id` int(11) NOT NULL AUTO_INCREMENT,
          `rental_date` datetime NOT NULL,
          `inventory_id` mediumint(8) unsigned NOT NULL,
          `customer_id` smallint(5) unsigned NOT NULL,
          `return_date` datetime DEFAULT NULL,
          `staff_id` tinyint(3) unsigned NOT NULL,
          `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`rental_id`),
          UNIQUE KEY `rental_date` (`rental_date`,`inventory_id`,`customer_id`),
          KEY `idx_fk_inventory_id` (`inventory_id`),
          KEY `idx_fk_customer_id` (`customer_id`),
          KEY `idx_fk_staff_id` (`staff_id`),
          CONSTRAINT `fk_rental_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`) ON UPDATE CASCADE,
          CONSTRAINT `fk_rental_inventory` FOREIGN KEY (`inventory_id`) REFERENCES `inventory` (`inventory_id`) ON UPDATE CASCADE,
          CONSTRAINT `fk_rental_staff` FOREIGN KEY (`staff_id`) REFERENCES `staff` (`staff_id`) ON UPDATE CASCADE)
          ENGINE=InnoDB DEFAULT CHARSET=utf8;'
        """
        cursor = self.parent.parent.connection
        result = cursor.execute("SHOW CREATE TABLE `%s`.`%s`" % (self.parent.name, self.name))
        sql = result[0]['Create Table'] + ';'
        sql = sql.replace('\n', '')
        return REGEX_MULTI_SPACE.sub(' ', sql)

    def drop(self):
        """
        Generate the SQL to drop this table
          >>> schema.databases['sakila'].tables['rental'].drop()
          'DROP TABLE `rental`;'
        """
        return "DROP TABLE `%s`;" % (self.name)

    def __eq__(self, other):
        if not isinstance(other, TableSchema):
            return False

        return ((self.options['engine'] == other.options['engine'])
                and (self.options['collation'] == other.options['collation'])
                and (self.options['row_format'] == other.options['row_format'])
                and (self.indexes == other.indexes)
                and (self.columns == other.columns)
                and (self.foreign_keys == other.foreign_keys))

    def __ne__(self, other):
        return not self.__eq__(other)
