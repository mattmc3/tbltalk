#!/usr/bin/env python

import unittest
from unittest import mock
from collections import namedtuple
from tbltalk import DbRow


class DbRowTest(unittest.TestCase):
    def test_add(self):
        exp = DbRow()

        # test that it's not there
        self.assertFalse(hasattr(exp, 'abc'))
        with self.assertRaises(AttributeError):
            _ = exp.abc
        with self.assertRaises(KeyError):
            _ = exp['abc']

        # assign and test that it is there
        exp.abc = 123
        self.assertTrue(hasattr(exp, 'abc'))
        self.assertTrue('abc' in exp)
        self.assertEqual(exp.abc, 123)

    def test_delete_attribute(self):
        exp = DbRow()

        # not there
        self.assertFalse(hasattr(exp, 'abc'))
        with self.assertRaises(AttributeError):
            _ = exp.abc

        # set value
        exp.abc = 123
        self.assertTrue(hasattr(exp, 'abc'))
        self.assertTrue('abc' in exp)
        self.assertEqual(exp.abc, 123)

        # delete attribute
        delattr(exp, 'abc')

        # not there
        self.assertFalse(hasattr(exp, 'abc'))
        with self.assertRaises(AttributeError):
            delattr(exp, 'abc')

    def test_delete_key(self):
        exp = DbRow()

        # not there
        self.assertFalse('abc' in exp)
        with self.assertRaises(KeyError):
            _ = exp['abc']

        # set value
        exp['abc'] = 123
        self.assertTrue(hasattr(exp, 'abc'))
        self.assertTrue('abc' in exp)
        self.assertEqual(exp.abc, 123)

        # delete key
        del exp['abc']

        # not there
        with self.assertRaises(KeyError):
            del exp['abc']

    def test_change_value(self):
        exp = DbRow()
        exp.abc = 123
        self.assertEqual(exp.abc, 123)
        self.assertEqual(exp.abc, exp['abc'])

        # change attribute
        exp.abc = 456
        self.assertEqual(exp.abc, 456)
        self.assertEqual(exp.abc, exp['abc'])

        # change attribute
        exp['abc'] = 789
        self.assertEqual(exp.abc, 789)
        self.assertEqual(exp.abc, exp['abc'])

    def test_dbrow_dict_init(self):
        exp = DbRow({'abc': 123, 'xyz': 456})
        self.assertEqual(exp.abc, 123)
        self.assertEqual(exp.xyz, 456)

    def test_dbrow_named_arg_init(self):
        exp = DbRow(abc=123, xyz=456)
        self.assertEqual(exp.abc, 123)
        self.assertEqual(exp.xyz, 456)

    def test_dbrow_datatypes(self):
        exp = DbRow({'intval': 1, 'listval': [1, 2, 3], 'dictval': {'a': 1}})
        self.assertEqual(exp.intval, 1)
        self.assertEqual(exp.listval, [1, 2, 3])
        self.assertEqual(exp.listval[0], 1)
        self.assertEqual(exp.dictval, {'a': 1})
        self.assertEqual(exp.dictval['a'], 1)


if __name__ == '__main__' and __package__ is None:
    unittest.main()
