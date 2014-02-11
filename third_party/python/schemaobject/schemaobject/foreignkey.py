import re
from schemaobject.collections import OrderedDict


REGEX_FK_REFERENCE_OPTIONS = r"""
    `%s`(?:.(?!ON\ DELETE)(?!ON\ UPDATE))*
    (?:\sON\sDELETE\s(?P<on_delete>(?:RESTRICT|CASCADE|SET\ NULL|NO\ ACTION)))?
    (?:\sON\sUPDATE\s(?P<on_update>(?:RESTRICT|CASCADE|SET\ NULL|NO\ ACTION)))?
    """


def ForeignKeySchemaBuilder(table):
    """
    Returns a dictionary loaded with all of the foreign keys available in the table.
    ``table`` must be an instance of TableSchema.

    .. note::
      This function is automatically called for you and set to
      ``schema.databases[name].tables[name].foreign_keys`` when you create an instance of SchemaObject
    """
    conn = table.parent.parent.connection
    fkeys = OrderedDict()

    sql = """
            SELECT K.CONSTRAINT_NAME,
                   K.TABLE_SCHEMA, K.TABLE_NAME, K.COLUMN_NAME,
                   K.REFERENCED_TABLE_SCHEMA, K.REFERENCED_TABLE_NAME, K.REFERENCED_COLUMN_NAME,
                   K.POSITION_IN_UNIQUE_CONSTRAINT
            FROM information_schema.KEY_COLUMN_USAGE K, information_schema.TABLE_CONSTRAINTS T
            WHERE K.CONSTRAINT_NAME = T.CONSTRAINT_NAME
            AND T.CONSTRAINT_TYPE = 'FOREIGN KEY'
            AND K.CONSTRAINT_SCHEMA='%s'
            AND K.TABLE_NAME='%s'
            """
    constraints = conn.execute(sql % (table.parent.name, table.name))

    if not constraints:
        return fkeys

    table_def = conn.execute("SHOW CREATE TABLE `%s`.`%s`" % (table.parent.name, table.name))[0]['Create Table']

    for fk in constraints:
        n = fk['CONSTRAINT_NAME']

        if n not in fkeys:
            FKItem = ForeignKeySchema(name=n, parent=table)

            FKItem.symbol = n
            FKItem.table_schema = fk['TABLE_SCHEMA']
            FKItem.table_name = fk['TABLE_NAME']
            FKItem.referenced_table_schema = fk['REFERENCED_TABLE_SCHEMA']
            FKItem.referenced_table_name = fk['REFERENCED_TABLE_NAME']

            reference_options = re.search(REGEX_FK_REFERENCE_OPTIONS % n, table_def, re.X)
            if reference_options:
                #If ON DELETE or ON UPDATE are not specified, the default action is RESTRICT.
                FKItem.update_rule = reference_options.group('on_update') or 'RESTRICT'
                FKItem.delete_rule = reference_options.group('on_delete') or 'RESTRICT'

            fkeys[n] = FKItem

        #columns for this fk
        if fk['COLUMN_NAME'] not in fkeys[n].columns:
            fkeys[n].columns.insert(fk['POSITION_IN_UNIQUE_CONSTRAINT'], fk['COLUMN_NAME'])

        #referenced columns for this fk
        if fk['REFERENCED_COLUMN_NAME'] not in fkeys[n].referenced_columns:
            fkeys[n].referenced_columns.insert(fk['POSITION_IN_UNIQUE_CONSTRAINT'], fk['REFERENCED_COLUMN_NAME'])

    return fkeys


class ForeignKeySchema(object):
    """
    Object representation of a single foreign key.
    Supports equality and inequality comparison of ForeignKeySchema.

    ``name`` is the column name.
    ``parent`` is an instance of TableSchema

    .. note::
      ForeignKeySchema objects are automatically created for you by ForeignKeySchemaBuilder
      and loaded under ``schema.databases[name].tables[name].foreign_keys``

    Example

      >>> schema.databases['sakila'].tables['rental'].foreign_keys.keys()
      ['fk_rental_customer', 'fk_rental_inventory', 'fk_rental_staff']


    Foreign Key Attributes
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].name
      'fk_rental_inventory'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].symbol
      'fk_rental_inventory'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].table_schema
      'sakila'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].table_name
      'rental'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].columns
      ['inventory_id']
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].referenced_table_name
      'inventory'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].referenced_table_schema
      'sakila'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].referenced_columns
      ['inventory_id']
      #match_option will always be None in MySQL 5.x, 6.x
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].match_option
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].update_rule
      'CASCADE'
      >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].delete_rule
      'RESTRICT'
    """

    def __init__(self, name, parent):
        self.parent = parent

        #name of the fk constraint
        self.name = name
        self.symbol = name

        #primary table info
        self.table_schema = None
        self.table_name = None
        self.columns = []

        #referenced table info
        self.referenced_table_schema = None
        self.referenced_table_name = None
        self.referenced_columns = []

        #constraint options
        self.match_option = None #will always be none in mysql 5.0-6.0
        self.update_rule = None
        self.delete_rule = None

    @classmethod
    def _format_referenced_col(self, field, length):
        """
        Generate the SQL to format referenced columns in a foreign key
        """
        if length:
            return "`%s`(%d)" % (field, length)
        else:
            return "`%s`" % (field)

    def create(self):
        """
        Generate the SQL to create (ADD) this foreign key

          >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].create()
          'ADD CONSTRAINT `fk_rental_inventory`
          FOREIGN KEY `fk_rental_inventory` (`inventory_id`)
          REFERENCES `inventory` (`inventory_id`)
          ON DELETE RESTRICT ON UPDATE CASCADE'

        .. note:
          match_option is ignored when creating a foreign key.
        """
        sql = []
        sql.append("ADD CONSTRAINT `%s`" % self.symbol)

        sql.append("FOREIGN KEY `%s` (%s)" % (self.symbol, ",".join([("`%s`" % c) for c in self.columns])))

        if self.referenced_table_schema != self.table_schema:
            sql.append("REFERENCES `%s`.`%s`" % (self.referenced_table_schema, self.referenced_table_name))
        else:
            sql.append("REFERENCES `%s`" % self.referenced_table_name)

        sql.append("(%s)" % ",".join([("`%s`" % c) for c in self.referenced_columns]))

        if self.delete_rule:
            sql.append("ON DELETE %s" % self.delete_rule)

        if self.update_rule:
            sql.append("ON UPDATE %s" % self.update_rule)

        return ' '.join(sql)

    def drop(self):
        """
        Generate the SQL to drop this foreign key

          >>> schema.databases['sakila'].tables['rental'].foreign_keys['fk_rental_inventory'].drop()
          'DROP FOREIGN KEY `fk_rental_inventory`'
        """
        return "DROP FOREIGN KEY `%s`" % (self.symbol)

    def __eq__(self, other):
        if not isinstance(other, ForeignKeySchema):
            return False

        # table_schema and referenced_table_schema are ignored
        return  ((self.table_name == other.table_name)
                and (self.referenced_table_name == other.referenced_table_name)
                and (self.update_rule == other.update_rule)
                and (self.delete_rule == other.delete_rule)
                and (self.columns == other.columns)
                and (self.referenced_columns == other.referenced_columns))

    def __ne__(self, other):
        return not self.__eq__(other)
