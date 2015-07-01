from schemaobject.collections import OrderedDict


def ColumnSchemaBuilder(table):
    """
    Returns a dictionary loaded with all of the columns availale in the table.
    ``table`` must be an instance of TableSchema.

    .. note::
      This function is automatically called for you and set to
      ``schema.databases[name].tables[name].columns``
      when you create an instance of SchemaObject
    """
    conn = table.parent.parent.connection
    cols = OrderedDict()
    sql = """
          SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT,
                IS_NULLABLE, COLUMN_TYPE, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, COLUMN_COMMENT
          FROM information_schema.COLUMNS
          WHERE TABLE_SCHEMA='%s'
          AND TABLE_NAME='%s'
          ORDER BY ORDINAL_POSITION
          """
    columns = conn.execute(sql % (table.parent.name, table.name))
    if not columns:
        return cols

    for col in columns:
        field = col['COLUMN_NAME']
        column = ColumnSchema(name=field, parent=table)

        column.ordinal_position = col['ORDINAL_POSITION']
        column.field = col['COLUMN_NAME']
        column.type = col['COLUMN_TYPE']
        column.charset = col['CHARACTER_SET_NAME']
        column.collation = col['COLLATION_NAME']

        column.key = col['COLUMN_KEY']
        column.default = col['COLUMN_DEFAULT']
        column.extra = col['EXTRA']
        column.comment = col['COLUMN_COMMENT']

        if col['IS_NULLABLE'] == "YES":
            column.null = True
        else:
            column.null = False

        cols[field] = column

    return cols


class ColumnSchema(object):
    """
    Object representation of a single column.
    Supports equality and inequality comparison of ColumnSchema.

    ``name`` is the column name.
    ``parent`` is an instance of TableSchema

    .. note::
      ColumnSchema objects are automatically created for you by ColumnSchemaBuilder
      and loaded under ``schema.databases[name].tables[name].columns``

    .. note::
      Attributes ``key``, ``comment`` are ignored in ``__eq__``, ``__neq__`` comparisons.

    Example

      >>> schema.databases['sakila'].tables['rental'].columns.keys()
      ['rental_id', 'rental_date', 'inventory_id', 'customer_id', 'return_date', 'staff_id', 'last_update']

    Column Attributes
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].name
      'rental_id'
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].field
      'rental_id'
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].ordinal_position
      1L
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].type
      'INT(11)'
      >>> schema.databases['sakila'].tables['staff'].columns['password'].charset
      'utf8'
      >>> schema.databases['sakila'].tables['staff'].columns['password'].collation
      'utf8_bin'
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].null
      False
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].key
      'PRI'
      >>> schema.databases['sakila'].tables['rental'].columns['last_update'].default
      'CURRENT_TIMESTAMP'
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].extra
      'auto_increment'
      >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].comment
      ''
    """

    def __init__(self, name, parent):

        self.parent = parent
        self.name = name
        self.field = name #alias for name, following mysql spec
        self.ordinal_position = 0
        self.type = None
        self.charset = None
        self.collation = None
        self.null = None
        self.key = None
        self.default = None
        self.extra = None
        self.comment = None

    def define(self, after=None, with_comment=False):
        """
        Generate the SQL for this column definition.

        ``after`` is the name(string) of the column this should appear after.
        If ``after`` is None, ``FIRST`` is used.

        ``with_comment`` boolean, add column comment to sql statement

          >>> schema.databases['sakila'].tables['rental'].columns['last_update'].define(after="staff_id")
          '`last_update` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP AFTER `staff_id`'
          >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].define()
          '`rental_id` INT(11) NOT NULL auto_increment FIRST'
        """
        sql = []
        sql.append("`%s` %s" % (self.field, self.type))

        if (self.collation and
            self.charset and
            (self.parent.options['charset'].value != self.charset or
             self.parent.options['collation'].value != self.collation)):
            sql.append("CHARACTER SET %s COLLATE %s" % (self.charset, self.collation))

        if not self.null:
            sql.append("NOT NULL")
        else:
            sql.append("NULL")

        if (self.default != None and
            isinstance(self.default, basestring) and
            self.default != 'CURRENT_TIMESTAMP'):
            sql.append("DEFAULT '%s'" % self.default)
        elif self.default != None:
            sql.append("DEFAULT %s" % self.default)

        if self.extra:
            sql.append(self.extra)

        if with_comment and self.comment:
            sql.append("COMMENT '%s'" % self.comment)

        if after:
            sql.append("AFTER `%s`" % (after))
        else:
            sql.append("FIRST")

        return ' '.join(sql)

    def create(self, *args, **kwargs):
        """
        Generate the SQL to create (ADD) this column.

        ``after`` is the name(string) of the column this should appear after.
        If ``after`` is None, ``FIRST`` is used.

        ``with_comment`` boolean, add column comment to sql statement

          >>> schema.databases['sakila'].tables['rental'].columns['last_update'].create(after="staff_id")
          'ADD COLUMN `last_update` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP AFTER `staff_id`'
          >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].create()
          'ADD COLUMN `rental_id` INT(11) NOT NULL auto_increment FIRST'
        """
        return "ADD COLUMN %s" % self.define(*args, **kwargs)

    def modify(self, *args, **kwargs):
        """
        Generate the SQL to modify this column.

        ``after`` is the name(string) of the column this should appear after.
        If ``after`` is None, ``FIRST`` is used.x

        ``with_comment`` boolean, add column comment to sql statement

          >>> schema.databases['sakila'].tables['rental'].columns['customer_id'].define(after="inventory_id")
          '`customer_id` SMALLINT(5) UNSIGNED NOT NULL AFTER `inventory_id`'
          >>> schema.databases['sakila'].tables['rental'].columns['customer_id'].default = 123
          >>> schema.databases['sakila'].tables['rental'].columns['customer_id'].modify(after="inventory_id")
          'MODIFY COLUMN `customer_id` SMALLINT(5) UNSIGNED NOT NULL DEFAULT 123 AFTER `inventory_id`'
        """
        return "MODIFY COLUMN %s" % self.define(*args, **kwargs)

    def drop(self):
        """
        Generate the SQL to drop this column::

          >>> schema.databases['sakila'].tables['rental'].columns['rental_id'].drop()
          'DROP COLUMN `rental_id`'
        """
        return "DROP COLUMN `%s`" % (self.field)

    def __eq__(self, other):
        if not isinstance(other, ColumnSchema):
            return False

        return ((self.field == other.field)
                and (self.type == other.type)
                and (self.null == other.null)
                and (self.default == other.default)
                and (self.extra == other.extra)
                and (self.collation == other.collation))

    def __ne__(self, other):
        return not self.__eq__(other)
