#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=missing-function-docstring, C0103

__author__ = "Josep-Arnau Claret"
__email__ = "joseparnau81@gmail.com"

import unittest

import datetime
import os
from pathlib import Path
import tempfile

from acid_rain.bot_event_register import BotEventRegister, EVENT_LOGIN, EVENT_LIKES, EVENT_FOLLOW, \
    EVENT_EXCEPTION, EVENT_BLOCK


class TestBotEventRegisterMethods(unittest.TestCase):

    def setUp(self):
        dirname = os.path.dirname(__file__)
        self.test_csv = Path(dirname) / 'data/bot_register_event_db.csv'
        self.register = BotEventRegister(self.test_csv)

    def test_save_book(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
            self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user', 2))
            self.register.save(temp_file.name)
            register_tmp = BotEventRegister(in_csv_path=temp_file.name)
            self.assertEqual(self.register.get_number_of_events(), 2)
            self.assertEqual(self.register.get_number_of_events(),
                             register_tmp.get_number_of_events())

    def test_do_book_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
            self.register.backup_folder = temp_dir
            backup_file_path = self.register.do_backup()
            register_backup = BotEventRegister(in_csv_path=backup_file_path)
            self.assertEqual(self.register.get_number_of_events(), 1)
            self.assertEqual(self.register.get_number_of_events(),
                             register_backup.get_number_of_events())

    # -----------------------------------------------------------------------
    # Read

    def test_get_event_timestamp(self):

        timestamp_0 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN, in_timestamp=timestamp_0))
        timestamp_1 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN, in_timestamp=timestamp_1))

        # Get last event
        timestamp_2 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN, in_timestamp=timestamp_2))
        self.assertEqual(self.register.get_number_of_events(), 3)
        self.assertEqual(self.register.get_event_timestamp(), timestamp_2)
        self.assertEqual(self.register.get_event_timestamp('a_bot'), timestamp_2)

        # Get previous event
        timestamp_01 = timestamp_0 + (timestamp_1 - timestamp_0) / 2
        self.assertEqual(self.register.get_event_timestamp(in_timestamp=timestamp_01),
                         timestamp_0)
        self.assertEqual(self.register.get_event_timestamp('a_bot', in_timestamp=timestamp_01),
                         timestamp_0)

        # No event
        self.assertIsNone(self.register.get_event_timestamp('another_bot'))

    def test_get_number_of_follows_since(self):
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_1'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_2'))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_FOLLOW, 'a_user_2'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_3'))
        timestamp_0 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_FOLLOW,
                                                'a_user_4',
                                                in_timestamp=timestamp_0))
        timestamp_1 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_FOLLOW,
                                                'a_user_5',
                                                in_timestamp=timestamp_1))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_6'))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_FOLLOW, 'a_user_6'))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_FOLLOW, 'a_user_7'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_7'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_8'))
        self.assertEqual(self.register.get_number_of_events(), 13)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 9)
        self.assertEqual(self.register.get_number_of_events('a_bot_2'), 4)

        timestamp_01 = timestamp_0 + (timestamp_1 - timestamp_0) / 2
        self.assertEqual(self.register.get_number_of_follows_since('a_bot', timestamp_01), 4)
        self.assertEqual(self.register.get_number_of_follows_since('a_bot_2', timestamp_01), 2)

    def test_get_number_of_likes_since(self):
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_1', 1))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_2', 2))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LIKES, 'a_user_2', 3))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_3', 4))
        timestamp_0 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_LIKES,
                                                'a_user_4',
                                                3,
                                                in_timestamp=timestamp_0))
        timestamp_1 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_LIKES,
                                                'a_user_5',
                                                1,
                                                in_timestamp=timestamp_1))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_6', 1))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LIKES, 'a_user_6', 2))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LIKES, 'a_user_7', 3))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_7', 4))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_8', 5))
        self.assertEqual(self.register.get_number_of_events(), 13)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 9)
        self.assertEqual(self.register.get_number_of_events('a_bot_2'), 4)

        timestamp_01 = timestamp_0 + (timestamp_1 - timestamp_0) / 2
        self.assertEqual(self.register.get_number_of_likes_since('a_bot', timestamp_01), 11)
        self.assertEqual(self.register.get_number_of_likes_since('a_bot_2', timestamp_01), 5)

    def test_get_first_timestamp_with_more_than_cumulative_likes(self):
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_1', 1))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_2', 2))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LIKES, 'a_user_2', 3))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_3', 4))
        timestamp_0 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_LIKES,
                                                'a_user_4',
                                                3,
                                                in_timestamp=timestamp_0))
        timestamp_1 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_LIKES,
                                                'a_user_5',
                                                4,
                                                in_timestamp=timestamp_1))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_6', 1))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LIKES, 'a_user_6', 2))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LIKES, 'a_user_7', 3))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_7', 4))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user_8', 5))
        self.assertEqual(self.register.get_number_of_events(), 13)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 9)
        self.assertEqual(self.register.get_number_of_events('a_bot_2'), 4)

        self.assertIsNone(self.register.get_first_timestamp_with_more_than_cumulative_likes('a_bot',
                                                                                            50))
        self.assertEqual(self.register.get_first_timestamp_with_more_than_cumulative_likes('a_bot',
                                                                                           15),
                         timestamp_0)

    # -----------------------------------------------------------------------
    # Write

    def test_remove_events_before(self):

        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_1'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_2'))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_FOLLOW, 'a_user_2'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_3'))
        timestamp_0 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_FOLLOW,
                                                'a_user_4',
                                                in_timestamp=timestamp_0))
        timestamp_1 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_FOLLOW,
                                                'a_user_5',
                                                in_timestamp=timestamp_1))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_6'))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_FOLLOW, 'a_user_6'))
        self.assertTrue(self.register.add_event('a_bot_2', EVENT_FOLLOW, 'a_user_7'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_7'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_8'))
        self.assertEqual(self.register.get_number_of_events(), 13)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 9)
        self.assertEqual(self.register.get_number_of_events('a_bot_2'), 4)

        timestamp_01 = timestamp_0 + (timestamp_1 - timestamp_0) / 2
        self.assertEqual(self.register.remove_events_before(timestamp_01), 7)

        self.assertEqual(self.register.get_number_of_events(), 6)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 4)
        self.assertEqual(self.register.get_number_of_events('a_bot_2'), 2)

    def test_add_event(self):

        self.assertFalse(self.register.add_event('a_bot', EVENT_LIKES))
        self.assertFalse(self.register.add_event('a_bot', EVENT_LIKES, in_username='a_user'))
        self.assertFalse(self.register.add_event('a_bot', EVENT_LIKES, in_num_likes=2))
        self.assertFalse(self.register.add_event('a_bot', EVENT_FOLLOW))
        self.assertFalse(self.register.add_event('a_bot', EVENT_EXCEPTION))
        self.assertFalse(self.register.add_event('a_bot', EVENT_BLOCK))

        self.assertEqual(self.register.get_number_of_events(), 0)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 0)

        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertEqual(self.register.get_number_of_events(), 1)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 1)
        self.assertEqual(self.register.get_number_of_events('another_bot'), 0)

        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user'))
        self.assertEqual(self.register.get_number_of_events(), 2)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 2)
        self.assertEqual(self.register.get_number_of_events('another_bot'), 0)

        self.assertTrue(self.register.add_event('a_bot', EVENT_LIKES, 'a_user', 2))
        self.assertEqual(self.register.get_number_of_events(), 3)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 3)
        self.assertEqual(self.register.get_number_of_events('another_bot'), 0)

        self.assertTrue(self.register.add_event('a_bot', EVENT_EXCEPTION, in_comments='exc'))
        self.assertEqual(self.register.get_number_of_events(), 4)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 4)
        self.assertEqual(self.register.get_number_of_events('another_bot'), 0)

        self.assertTrue(self.register.add_event('a_bot', EVENT_BLOCK, in_comments='block'))
        self.assertEqual(self.register.get_number_of_events(), 5)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 5)
        self.assertEqual(self.register.get_number_of_events('another_bot'), 0)

    def test_get_last_block_timestamp(self):

        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_1'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_EXCEPTION, in_comments='exc'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_LOGIN))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_2'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_3'))
        timestamp_0 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_BLOCK,
                                                in_timestamp=timestamp_0,
                                                in_comments='block'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_FOLLOW, 'a_user_2'))
        self.assertTrue(self.register.add_event('a_bot', EVENT_EXCEPTION, in_comments='exc'))
        self.assertEqual(self.register.get_number_of_events(), 9)
        self.assertEqual(self.register.get_number_of_events('a_bot'), 9)
        self.assertEqual(self.register.get_last_block_timestamp('a_bot'), timestamp_0)

        timestamp_1 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot',
                                                EVENT_BLOCK,
                                                in_timestamp=timestamp_1,
                                                in_comments='block'))
        self.assertEqual(self.register.get_last_block_timestamp('a_bot'), timestamp_1)

        timestamp_2 = datetime.datetime.now()
        self.assertTrue(self.register.add_event('a_bot_2',
                                                EVENT_BLOCK,
                                                in_timestamp=timestamp_2,
                                                in_comments='block'))
        self.assertEqual(self.register.get_last_block_timestamp('a_bot_2'), timestamp_2)
        self.assertEqual(self.register.get_last_block_timestamp('a_bot'), timestamp_1)


if __name__ == '__main__':
    unittest.main()
