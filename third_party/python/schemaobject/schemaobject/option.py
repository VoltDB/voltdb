class SchemaOption(object):
    """
    Object representation of a database or table option
      >>> schema.databases['sakila'].tables['rental'].options['engine'].name
      'ENGINE'
      >>> schema.databases['sakila'].tables['rental'].options['engine'].value
      'InnoDB'
      >>> schema.databases['sakila'].tables['rental'].options['engine'].create()
      'ENGINE=InnoDB'
    """

    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    def _get_value(self):
        return self._value

    def _set_value(self, val):
        self._value = val

    value = property(fget=_get_value, fset=_set_value)

    def create(self):
        """
        Generate the SQL for this option
          >>> schema.databases['sakila'].options['charset'].create()
          'CHARSET=latin1'
          >>> schema.databases['sakila'].tables['rental'].options['engine'].create()
          'ENGINE=InnoDB'
          >>> schema.databases['sakila'].tables['rental'].options['auto_increment'].create()
          'AUTO_INCREMENT=1'
        """

        # MySQL stores misc options pre-formatted in 1 field (CREATE_OPTIONS)
        if not self.name:
            return self.value

        if self.name == "COMMENT":
            if not self.value:
                self.value = ''
            return "%s='%s'" % (self.name, self.value)

        if not self.value:
            return ''

        if (isinstance(self.value, basestring) and ' ' in self.value):
            return "%s='%s'" % (self.name, self.value)

        return "%s=%s" % (self.name, self.value)

    def __eq__(self, other):
        if not isinstance(other, SchemaOption):
            return False
        return (self.value == other.value) and (self.name == other.name)

    def __ne__(self, other):
        return not self.__eq__(other)
