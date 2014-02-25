#!/usr/bin/python
import unittest
from schemaobject.option import SchemaOption

class TestOptionSchema(unittest.TestCase):

  def test_set_value(self):
    opt = SchemaOption('key')
    assert opt.value == None
    opt.value = "value"
    assert opt.value == "value"

  def test_get_value(self):
    opt = SchemaOption('key', 'value')
    assert opt.value == "value"

  def test_create_no_name(self):
    opt = SchemaOption(None, 'value')
    assert opt.create() == 'value'

  def test_create_no_value(self):
    opt = SchemaOption('key')
    assert opt.create() == ''

  def test_create_value_non_string(self):
    opt = SchemaOption('key', 1234)
    assert opt.create() == 'key=1234'

  def test_create_value_string_with_spaces(self):
    opt = SchemaOption('key', 'my value')
    assert opt.create() == "key='my value'"

  def test_create_value_string_without_spaces(self):
    opt = SchemaOption('key', 'value')
    assert opt.create() == 'key=value'

  def test_eq(self):
    opt = SchemaOption('key', 'value')
    opt2 = SchemaOption('key', 'value')
    assert opt == opt2

  def test_neq(self):
    opt = SchemaOption('key', 'value1')
    opt2 = SchemaOption('key', 'value2')
    assert opt != opt2


if __name__ == "__main__":
    unittest.main()