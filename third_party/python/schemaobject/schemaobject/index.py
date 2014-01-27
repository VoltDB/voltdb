from schemaobject.collections import OrderedDict


def IndexSchemaBuilder(table):
    """
    Returns a dictionary loaded with all of the indexes available in the table.
    ``table`` must be an instance of TableSchema.

    .. note::
      This function is automatically called for you and set to
      ``schema.databases[name].tables[name].indexes`` when you create an instance of SchemaObject
    """
    conn = table.parent.parent.connection

    idx = OrderedDict()
    indexes = conn.execute("SHOW INDEXES FROM `%s`.`%s`" % (table.parent.name, table.name))

    if not indexes:
        return idx

    for index in indexes:
        n = index['Key_name']
        if n not in idx:
            Indexitem = IndexSchema(name=n, parent=table)
            Indexitem.non_unique = (bool(index['Non_unique'])) # == not unique
            Indexitem.table_name = index['Table']

            key_type = index['Index_type'].upper()

            if index['Key_name'].upper() == "PRIMARY":
                Indexitem.kind = "PRIMARY"
            elif not Indexitem.non_unique:
                Indexitem.kind = "UNIQUE"
            elif key_type in ('FULLTEXT', 'SPATIAL'):
                Indexitem.kind = key_type
            else:
                Indexitem.kind = "INDEX"

            if key_type in ('BTREE', 'HASH', 'RTREE'):
                Indexitem.type = key_type

            Indexitem.collation = index['Collation']
            Indexitem.comment = index['Comment']

            idx[n] = Indexitem

        if index['Column_name'] not in idx[n].fields:
            idx[n].fields.insert(index['Seq_in_index'], (index['Column_name'], index['Sub_part'] or 0))

    return idx


class IndexSchema(object):
    """
    Object representation of a single index.
    Supports equality and inequality comparison of IndexSchema.

    ``name`` is the column name.
    ``parent`` is an instance of TableSchema

    .. note::
      IndexSchema objects are automatically created for you by IndexSchemaBuilder
      and loaded under ``schema.databases[name].tables[name].indexes``

    Example

      >>> schema.databases['sakila'].tables['rental'].indexes.keys()
      ['PRIMARY', 'rental_date', 'idx_fk_inventory_id', 'idx_fk_customer_id', 'idx_fk_staff_id']

    Index Attributes

      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].name
      'idx_fk_customer_id'
      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].table_name
      'rental'
      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].non_unique
      True
      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].fields
      [('customer_id', None)]
      #possible types: BTREE, RTREE, HASH
      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].type
      'BTREE'
      #possible kinds: PRIMARY, UNIQUE, FULLTEXT, SPATIAL, INDEX
      >>> schema.databases['sakila'].tables['rental'].indexes['rental_date'].kind
      'UNIQUE'
      >>> schema.databases['sakila'].tables['film_text'].indexes['idx_title_description'].kind
      'FULLTEXT'
      #fields is a list of tuples (field_name, sub_part_length)
      >>> schema.databases['sakila'].tables['film_text'].indexes['idx_title_description'].fields
      [('title', 0), ('description', 0)]
      #collation will always be A in MySQL 5.x - 6.x
      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].collation
      'A'
      >>> schema.databases['sakila'].tables['rental'].indexes['idx_fk_customer_id'].comment
      ''
    """

    def __init__(self, name, parent):
        self.parent = parent
        self.name = name
        self.table_name = None
        self.non_unique = False
        self.fields = []
        self.type = None
        self.kind = None
        self.collation = None #ignored by the parser. mysql 5.0+ all == A (ASC)
        self.comment = None

    @classmethod
    def format_sub_part(self, field, length):
        """
        Generate the SQL to format the sub_part length of an indexed column name

          >>> schemaobjects.index.IndexSchema.format_sub_part('column', 0)
          '`column`'
          >>> schemaobjects.index.IndexSchema.format_sub_part('column', 5)
          '`column`(5)'
        """
        if isinstance(length, int) and length > 0:
            return "`%s`(%d)" % (field, length)
        else:
            return "`%s`" % (field)

    def create(self):
        """
        Generate the SQL to create (ADD) this index

          >>> schema.databases['sakila'].tables['film_text'].indexes['idx_title_description'].create()
          'ADD FULLTEXT INDEX `idx_title_description` (`title`, `description`)'
          >>> schema.databases['sakila'].tables['rental'].indexes['rental_date'].create()
          'ADD UNIQUE INDEX `rental_date` (`rental_date`, `inventory_id`, `customer_id`) USING BTREE'

        .. note:
          Collation is ignored when creating an index (MySQL default is 'A').
        """
        sql = []

        if self.kind == "PRIMARY":
            sql.append("ADD PRIMARY KEY")
        elif self.kind == "UNIQUE":
            sql.append("ADD UNIQUE INDEX `%s`" % self.name)
        elif self.kind in ('FULLTEXT', 'SPATIAL'):
            sql.append("ADD %s INDEX `%s`" % (self.kind, self.name))
        else:
            sql.append("ADD INDEX `%s`" % self.name)

        sql.append("(%s)" % ", ".join([self.format_sub_part(f, l) for f, l in self.fields]))

        if self.type in ('BTREE', 'HASH', 'RTREE'):
            sql.append("USING %s" % self.type)

        return ' '.join(sql)

    def drop(self):
        """
        Generate the SQL to drop this index

          >>> schema.databases['sakila'].tables['rental'].indexes['PRIMARY'].drop()
          'DROP PRIMARY KEY'
          >>> schema.databases['sakila'].tables['rental'].indexes['rental_date'].drop()
          'DROP INDEX `rental_date`'
        """
        if self.name == 'PRIMARY':
            return "DROP PRIMARY KEY"
        else:
            return "DROP INDEX `%s`" % (self.name)

    def __eq__(self, other):
        if not isinstance(other, IndexSchema):
            return False

        return ((self.name == other.name)
                and (self.table_name == other.table_name)
                and (self.type == other.type)
                and (self.kind == other.kind)
                and (self.collation == other.collation)
                and (self.non_unique == other.non_unique)
                and (self.fields == other.fields))

    def __ne__(self, other):
        return not self.__eq__(other)
