#!/usr/bin/env python3
# pylint: disable=missing-function-docstring, C0103

import unittest

import datetime
import os
from pathlib import Path
from random import seed

from acid_rain.acid_rain_utils import get_random_bool, get_random_string, is_date, list_to_string, \
    remove_repeated, string_to_date, string_to_list, check_action_blocked


class TestPyramidUtils(unittest.TestCase):

    def setUp(self):
        seed(1)
        self.script_dir = os.path.dirname(__file__)

    def test_list_to_string(self):
        self.assertEqual(list_to_string([]), "[]")
        self.assertEqual(list_to_string(['a', 'b', 'c']), "['a', 'b', 'c']")
        self.assertEqual(list_to_string([1, 2, 3]), "['1', '2', '3']")
        self.assertEqual(list_to_string([1.1, 2.2]), "['1.1', '2.2']")

    def test_string_to_list(self):
        self.assertEqual(string_to_list("[]"), [])
        self.assertEqual(string_to_list("['a', 'b', 'c']"), ['a', 'b', 'c'])
        self.assertEqual(string_to_list("['1', '2', '3']"), ['1', '2', '3'])

    def test_string_to_date(self):
        self.assertEqual(string_to_date('2020-05-17 19:59:41.308748'),
                         datetime.datetime(2020, 5, 17, 19, 59, 41, 308748))

    def test_is_date(self):
        self.assertTrue(is_date('2020-05-17 19:59:41.308748'))
        self.assertFalse(is_date('This is not a date'))

    def test_get_random_bool(self):
        self.assertTrue(get_random_bool())
        self.assertFalse(get_random_bool())
        self.assertFalse(get_random_bool())
        self.assertTrue(get_random_bool())
        self.assertTrue(get_random_bool())
        self.assertTrue(get_random_bool())
        self.assertFalse(get_random_bool())
        self.assertFalse(get_random_bool())
        self.assertTrue(get_random_bool())
        self.assertTrue(get_random_bool())

    def test_get_random_string(self):
        self.assertEqual(get_random_string(), 'eszycidp')
        self.assertEqual(get_random_string(), 'yopumzgd')

    def test_remove_repeated(self):
        self.assertEqual(remove_repeated([1, 1, 2, 3, 4, 3, 4, 5]), [2, 5])

    def test_check_action_blocked(self):

        page_with_block_path = Path(self.script_dir) / 'data/page_w_action_blocked.html'
        with open(page_with_block_path, 'r') as file:
            page_source = file.read().replace('\n', '')
            self.assertTrue(check_action_blocked(page_source))

        page_without_block_path = Path(self.script_dir) / 'data/page_wout_action_blocked.html'
        with open(page_without_block_path, 'r') as file:
            page_source = file.read().replace('\n', '')
            self.assertFalse(check_action_blocked(page_source))


if __name__ == '__main__':
    unittest.main()
