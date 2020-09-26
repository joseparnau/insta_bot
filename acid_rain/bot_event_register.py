#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""File to manage the storing of all bot events in common a database"""

__author__ = "Josep-Arnau Claret"
__email__ = "joseparnau81@gmail.com"

import datetime
import os
from pathlib import Path

import pandas as pd

import acid_rain.acid_rain_settings
from acid_rain.acid_rain_utils import get_random_string

LABEL_TIMESTAMP = 'timestamp'
LABEL_BOT = 'bot'
LABEL_EVENT = 'event'
LABEL_USERNAME = 'username'
LABEL_NUM_LIKES = 'num_likes'
LABEL_COMMENTS = 'comments'
ALL_LABELS = [LABEL_TIMESTAMP, LABEL_BOT, LABEL_EVENT, LABEL_USERNAME, LABEL_NUM_LIKES,
              LABEL_COMMENTS]

EVENT_LOGIN = 'login'
EVENT_LIKES = 'likes'
EVENT_FOLLOW = 'follow'
EVENT_EXCEPTION = 'exception'
EVENT_BLOCK = 'block'
ALL_EVENTS = [EVENT_LOGIN, EVENT_LIKES, EVENT_FOLLOW, EVENT_EXCEPTION, EVENT_BLOCK]


class BotEventRegister:
    """
    Class that manages the storing of bot events in a database
    """

    def __init__(self, in_csv_path, in_database=None, in_backup_folder=None, in_verbose_on=False):
        """
        Event register constructor.
        Will use global database if global variable 'global_events_db' is not None

        Params:
            in_csv_path: string, a path to the csv with the data
            in_database: DataFrame, a database
            in_backup_folder: string, backup folder
            in_verbose: bool, set verbose on
        """

        self.source = in_csv_path
        filename_ext = os.path.basename(self.source)
        self.file_name, _ = os.path.splitext(filename_ext)

        self.backup_folder = in_backup_folder

        self.use_global = acid_rain.acid_rain_settings.use_global_database
        if self.use_global:
            self.database = acid_rain.acid_rain_settings.global_events_db
            print("bot event register: database set to global")
        else:
            if in_database is None:
                self.database = pd.read_csv(self.source)
                self.database[LABEL_TIMESTAMP] = pd.to_datetime(self.database[LABEL_TIMESTAMP])
            else:
                self.database = in_database

        self.verbose_on = in_verbose_on

        self.labels = ALL_LABELS

    @staticmethod
    def load_database(in_csv_path):
        with acid_rain.acid_rain_settings.global_bot_lock:
            print("Load database")
            tmp_database = pd.read_csv(in_csv_path)
            tmp_database[LABEL_TIMESTAMP] = pd.to_datetime(tmp_database[LABEL_TIMESTAMP])
            return tmp_database

    def bot_is_locked(self, function):
        if self.use_global:
            is_locked = None if acid_rain.acid_rain_settings.global_bot_lock is None \
                else acid_rain.acid_rain_settings.global_bot_lock.acquired
            owner = None if acid_rain.acid_rain_settings.global_bot_lock is None \
                else acid_rain.acid_rain_settings.global_bot_lock.owner
            lock_event_id = get_random_string()
            if is_locked:
                print(f"-{lock_event_id} ({owner})- '{function}'"
                      f"{' IS LOCKED' if is_locked == True else ''} ")
            return is_locked, datetime.datetime.now(), lock_event_id
        else:
            return False, datetime.datetime.now(), ''

    def print_lock_release(self, lock_data):
        if self.use_global:
            is_locked = lock_data[0]
            lock_ts = lock_data[1]
            owner = None if acid_rain.acid_rain_settings.global_bot_lock is None \
                else acid_rain.acid_rain_settings.global_bot_lock.owner
            lock_event_id = lock_data[2]
            if is_locked:
                print(f'-{lock_event_id} ({owner})-: lock released after '
                      f'{datetime.datetime.now() - lock_ts}')

    def save(self, in_file_path=None) -> bool:
        """
        Save the book as a csv in a file

        Params:
            in_file_path: None or string, path of the file to save the book as a csv;
                          if not specified, the default name is self.source;
                          if the latter is not specified it will return False

        Returns:
            bool, whether the book was saved or not
        """

        file_path = self.source if in_file_path is None else in_file_path
        if file_path is None:
            print('Please specify a file path')
            return False

        lock_data = self.bot_is_locked('save')
        with acid_rain.acid_rain_settings.global_bot_lock:
            self.print_lock_release(lock_data)
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            self.database.to_csv(file_path, index=False)
        if self.verbose_on:
            print('Book saved in: {}'.format(file_path))
        return True

    def do_backup(self) -> bool:
        """
        Saves a backup of the book as a csv in a file

        Returns:
            string, the name of the saved backup file
        """

        if self.backup_folder is None:
            print('Please specify a backup folder')
            return False

        time_stamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = Path(self.backup_folder) / (self.file_name + '_' + time_stamp + '.csv')
        self.save(file_path)
        return file_path

    # -----------------------------------------------------------------------
    # Read

    def get_number_of_events(self, in_bot=None) -> int:
        """
        Returns the number of events of the database

        Params:
            in_bot: str, name of the bot of the events; if None, all events will be retrieved

        Returns:
            int, number of events
        """
        lock_data = self.bot_is_locked('get_number_of_events')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            number_of_events = len(self.database) if in_bot is None \
                else len(self.database[self.database[LABEL_BOT] == in_bot])
            self.print_lock_release(lock_data)
            return number_of_events

    def get_event_timestamp(self, in_bot=None, in_timestamp=None) -> datetime:
        """
        Returns the timestamp of the previous event from 'in_timestamp' associated to the bot

        Params:
            in_bot: str, name of the bot
            in_timestamp: datetime, timestamp to look for previous events

        Returns:
            datetime or None, timestamp of the previous event of bot, None if not previous events
        """
        lock_data = self.bot_is_locked('get_event_timestamp')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            timestamp = datetime.datetime.now() if in_timestamp is None else in_timestamp
            condition = self.database[LABEL_TIMESTAMP] <= timestamp
            if in_bot is not None:
                condition &= self.database[LABEL_BOT] == in_bot
            results = self.database.loc[condition][LABEL_TIMESTAMP]
            event_timestamp = results.iloc[-1] if len(results) > 0 else None
            self.print_lock_release(lock_data)
            return event_timestamp

    def get_last_block_timestamp(self, in_bot) -> (None, datetime):
        """
        Returns the timestamp of the last block of bot

        Params:
            in_bot: str, name of the bot

        Returns:
            datetime or None, timestamp of the last block
        """
        lock_data = self.bot_is_locked('get_last_block_timestamp')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            condition = (self.database[LABEL_BOT] == in_bot) \
                        & (self.database[LABEL_EVENT] == EVENT_BLOCK)
            results = self.database.loc[condition][LABEL_TIMESTAMP]
            event_timestamp = results.iloc[-1] if len(results) > 0 else None
            self.print_lock_release(lock_data)
            return event_timestamp

    def get_number_of_follows_since(self, in_bot, in_timestamp) -> int:
        """
        Returns the number of follows done by the bot since the timestamp

        Params:
            in_bot: str, name of the bot
            in_timestamp: datetime, timestamp to look for previous events

        Returns:
            datetime, timestamp of the previous event of bot
        """
        lock_data = self.bot_is_locked('get_number_of_follows_since')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            condition = (self.database[LABEL_BOT] == in_bot) \
                        & (self.database[LABEL_EVENT] == EVENT_FOLLOW) \
                        & (self.database[LABEL_TIMESTAMP] >= in_timestamp)
            number_of_follows = len(self.database.loc[condition][LABEL_TIMESTAMP])
            self.print_lock_release(lock_data)
            return number_of_follows

    def get_number_of_likes_since(self, in_bot, in_timestamp) -> int:
        """
        Returns the number of likes done by the bot since the timestamp

        Params:
            in_bot: str, name of the bot
            in_timestamp: datetime, timestamp to look for previous events

        Returns:
            datetime, timestamp of the previous event of bot
        """
        lock_data = self.bot_is_locked('get_number_of_likes_since')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            condition = (self.database[LABEL_BOT] == in_bot) \
                        & (self.database[LABEL_EVENT] == EVENT_LIKES) \
                        & (self.database[LABEL_TIMESTAMP] >= in_timestamp)
            number_of_likes = self.database.loc[condition][LABEL_NUM_LIKES].sum()
            self.print_lock_release(lock_data)
            return number_of_likes

    def get_first_timestamp_with_more_than_cumulative_likes(
            self, in_bot, in_num_likes) -> (None, datetime.datetime):
        """
        Returns the first timestamp such that the number of likes is more than 'in_num_likes'

        Params:
            in_bot: str, name of the bot
            in_num_likes: int, the number of likes

        Returns:
            datetime, the timestamp, or None if no match is found
        """
        lock_data = self.bot_is_locked('get_first_timestamp_with_more_than_cumulative_likes')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            bot_likes_condition = (self.database[LABEL_BOT] == in_bot) \
                                  & (self.database[LABEL_EVENT] == EVENT_LIKES)
            bot_df = self.database[bot_likes_condition]
            reverse_cumulative_likes_df = bot_df.loc[::-1, LABEL_NUM_LIKES].cumsum()[::-1]
            cum_likes_match = \
                reverse_cumulative_likes_df[reverse_cumulative_likes_df > in_num_likes]
            first_timestamp = None if len(cum_likes_match) == 0 \
                else self.database.iloc[cum_likes_match.index[-1]][LABEL_TIMESTAMP]
            self.print_lock_release(lock_data)
            return first_timestamp

    # -----------------------------------------------------------------------
    # Write

    def remove_events_before(self, in_timestamp) -> int:
        """
        Remove all the events before the timestamp

        Params:
            in_timestamp: datetime, the timestamp

        Returns:
            int, number of events removed
        """
        lock_data = self.bot_is_locked('remove_events_before')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            previous_num_of_events = self.get_number_of_events()
            self.database = self.database.loc[self.database[LABEL_TIMESTAMP] >= in_timestamp]
            if self.use_global:
                acid_rain.acid_rain_settings.global_events_db = self.database
            num_events_removed = previous_num_of_events - self.get_number_of_events()
            self.print_lock_release(lock_data)
            return num_events_removed

    def add_event(self, in_bot, in_event_name, in_username=None, in_num_likes=None,
                  in_comments=None, in_timestamp=None) -> bool:
        """
        Adds an event to the database

        Params:
            in_bot: str, name of the bot that triggered the event
            in_event_name: timestamp to look for previous events.
                           Options:
                           - 'login'
                           - 'likes', requires 'in_username' and 'in_num_likes
                           - 'follow', requires 'in_username'
                           - 'exception', requires 'in_comments'
            in_username: str, username associated with the event
            in_num_likes: int, number of likes
            in_comments: str, comments
            in_timestamp: datetime, timestamp of the event

        Returns:
            bool, whether the event was succesfully written
        """

        if in_event_name == EVENT_LIKES and (in_username is None or in_num_likes is None):
            print("Event 'likes' requires 'in_username' and 'in_num_likes")
            return False
        elif in_event_name == EVENT_FOLLOW and in_username is None:
            print("Event 'follow' requires 'in_username'")
            return False
        elif in_event_name == EVENT_EXCEPTION and in_comments is None:
            print("Event 'exception' requires 'in_comments'")
            return False
        elif in_event_name == EVENT_BLOCK and in_comments is None:
            print("Event 'block' requires 'in_comments'")
            return False

        timestamp = datetime.datetime.now() if in_timestamp is None else in_timestamp

        data = {
            LABEL_TIMESTAMP: timestamp,
            LABEL_BOT: in_bot,
            LABEL_EVENT: in_event_name,
            LABEL_USERNAME: in_username,
            LABEL_NUM_LIKES: in_num_likes,
            LABEL_COMMENTS: in_comments
        }

        lock_data = self.bot_is_locked('add_event')
        with acid_rain.acid_rain_settings.global_bot_lock:
            if self.use_global:
                self.database = acid_rain.acid_rain_settings.global_events_db
            self.database = self.database.append(data, ignore_index=True)
            if self.use_global:
                acid_rain.acid_rain_settings.global_events_db = self.database
            self.print_lock_release(lock_data)
        return True
