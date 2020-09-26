#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""File to manage the storing of bot events in a database"""

__author__ = "Josep-Arnau Claret"
__email__ = "joseparnau81@gmail.com"

from datetime import datetime, timedelta
from random import uniform, shuffle
from time import sleep
from threading import Thread

import pandas as pd

import acid_rain.acid_rain_settings
from acid_rain.bot_event_register import BotEventRegister
from acid_rain.insta_bot import InstaBot, LIKES_MAX_PER_RUN, LIKES_MAX_PER_DAY, \
    LIKES_MAX_PER_HOUR, LIKES_MIN_X_PROFILE, LIKES_MAX_X_PROFILE, LIKES_MIN_SLEEP_TIME_S, \
    LIKES_MAX_SLEEP_TIME_S, LIKES_MIN_SECONDS_BETWEEN_PROFILES, \
    LIKES_MAX_SECONDS_BETWEEN_PROFILES, FOLLOWS_MAX_PER_RUN, FOLLOWS_MAX_PER_DAY, \
    FOLLOWS_MAX_PER_HOUR, FOLLOWS_MIN_SECONDS_BETWEEN_PROFILES, \
    FOLLOWS_MAX_SECONDS_BETWEEN_PROFILES, MAX_RUN_HOURS, WAIT_AFTER_BLOCK_HOURS


KEY_BOT_NAME = 'name'
KEY_PASSWORD = 'password'
KEY_DATA = 'data'
KEY_ACTION_PROBS = 'action_probs'

KEYS_CREDENTIALS = [KEY_BOT_NAME, KEY_PASSWORD]

DEFAULT_LAUNCH_MIN_WAIT_TIME_M = 5
DEFAULT_LAUNCH_MAX_WAIT_TIME_M = 20

FUNCTION_LIKE_FOLLOW = 'like_follow'
FUNCTION_ENGAGE = 'engage'


class BotMaster:
    """
    Class that manages multiple bots
    """

    def __init__(self, in_bots, in_target_profiles_file, in_excluded_profiles_file,
                 in_bot_events_database_file_path, in_test=False, in_with_shuffle=True,
                 in_log_folder=None):
        """
        Params:
            in_bots: list, list of bot dicts.
                     Each dict must be: { 'name': XXXXX, 'password': XXXXX}
            in_target_profiles_file: str, file path with user names
            in_excluded_profiles_file: str, file path with excluded names
            in_bot_events_database_file_path: str, file path with bot events
            in_test: bool, whether to run in test or not
            in_with_shuffle: bool, randomly sort bots
            in_log_folder: str, folder to log
        """

        self.test_on = in_test

        self.log_folder = in_log_folder

        if in_with_shuffle:
            shuffle(in_bots)
        self.bots_credentials = self.get_bots_credentials(in_bots)
        self.bots_data = self.get_bots_data(in_bots)

        self.num_of_bots = len(self.bots_credentials)
        self.bots = None
        self.bot_threads = None

        self.target_profiles_file = in_target_profiles_file
        self.excluded_profiles_file = in_excluded_profiles_file
        self.bot_events_database_file_path = in_bot_events_database_file_path

        self.launch_min_wait_time_m = DEFAULT_LAUNCH_MIN_WAIT_TIME_M
        self.launch_max_wait_time_m = DEFAULT_LAUNCH_MAX_WAIT_TIME_M

        self.likes_target_profiles_per_bot = None
        self.follow_target_profiles_per_bot = None
        self.probabilities_per_bot = None

        self.wait_after_block_hours = WAIT_AFTER_BLOCK_HOURS

        self.likes_max_run_hours = MAX_RUN_HOURS
        self.follows_max_run_hours = MAX_RUN_HOURS

        # Likes
        self.likes_limit = LIKES_MAX_PER_RUN
        self.likes_max_per_day = LIKES_MAX_PER_DAY
        self.likes_max_per_hour = LIKES_MAX_PER_HOUR
        self.likes_min_x_profile = LIKES_MIN_X_PROFILE
        self.likes_max_x_profile = LIKES_MAX_X_PROFILE
        self.likes_min_sleep_time_s = LIKES_MIN_SLEEP_TIME_S
        self.likes_max_sleep_time_s = LIKES_MAX_SLEEP_TIME_S
        self.likes_min_seconds_between_profiles = LIKES_MIN_SECONDS_BETWEEN_PROFILES
        self.likes_max_seconds_between_profiles = LIKES_MAX_SECONDS_BETWEEN_PROFILES

        # Follows
        self.follows_limit = FOLLOWS_MAX_PER_RUN
        self.follows_max_per_day = FOLLOWS_MAX_PER_DAY
        self.follows_max_per_hour = FOLLOWS_MAX_PER_HOUR
        self.follows_min_seconds_between_profiles = FOLLOWS_MIN_SECONDS_BETWEEN_PROFILES
        self.follows_max_seconds_between_profiles = FOLLOWS_MAX_SECONDS_BETWEEN_PROFILES

    @staticmethod
    def get_bots_credentials(in_bots):
        return [{k: x for k, x in bot.items() if k in KEYS_CREDENTIALS} for bot in in_bots]

    @staticmethod
    def get_bots_data(in_bots):
        return [bot[KEY_DATA] if KEY_DATA in bot else None for bot in in_bots]

    def initialize_bots(self):
        """
        Initializes the bots and the threads

        Returns:
            bool, whether the initialization was successful or not
        """

        if self.bots is None:
            acid_rain.acid_rain_settings.use_global_database = True
            acid_rain.acid_rain_settings.global_events_db = \
                BotEventRegister.load_database(self.bot_events_database_file_path)

            print('+++++ BOTS: {}'.format([x[KEY_BOT_NAME] for x in self.bots_credentials]))

            self.bots = []
            for credentials in self.bots_credentials:
                name = credentials[KEY_BOT_NAME]
                password = credentials[KEY_PASSWORD]
                bot = InstaBot(name, password,
                               self.excluded_profiles_file,
                               self.bot_events_database_file_path,
                               self.test_on,
                               in_log_folder=self.log_folder)
                print('+++++ BOT CREATED: {}'.format(name))

                # Parameters
                self.load_parameters_to_bot(bot)

                self.bots.append(bot)

    def load_parameters_to_bot(self, bot):

        # General
        bot.wait_after_block_hours = self.wait_after_block_hours

        # Follows
        bot.likes_limit = self.likes_limit
        bot.likes_max_per_day = self.likes_max_per_day
        bot.likes_max_per_hour = self.likes_max_per_hour
        bot.likes_min_x_profile = self.likes_min_x_profile
        bot.likes_max_x_profile = self.likes_max_x_profile
        bot.likes_min_sleep_time_s = self.likes_min_sleep_time_s
        bot.likes_max_sleep_time_s = self.likes_max_sleep_time_s
        bot.likes_min_seconds_between_profiles = self.likes_min_seconds_between_profiles
        bot.likes_max_seconds_between_profiles = self.likes_max_seconds_between_profiles

        # Likes
        bot.follows_limit = self.follows_limit
        bot.follows_max_per_day = self.follows_max_per_day
        bot.follows_max_per_hour = self.follows_max_per_hour
        bot.follows_min_seconds_between_profiles = self.follows_min_seconds_between_profiles
        bot.follows_max_seconds_between_profiles = self.follows_max_seconds_between_profiles

        print('+++++ PARAMETERS LOADED: {}'.format(bot.name))

    def load_profiles(self, in_load_num_profiles_likes=None, in_load_num_profiles_follows=None):
        """
        Loads the profiles from the databases
        """

        # choose target profiles
        profiles = pd.read_csv(self.target_profiles_file)
        excluded_profiles = pd.read_csv(self.excluded_profiles_file)
        print('+++++ Profiles: {}'.format(len(profiles)))
        print('+++++ Excluded: {}'.format(len(excluded_profiles)))

        public_profiles = profiles.loc[profiles['isPrivate'].isnull()]
        private_profiles = profiles.loc[profiles.isPrivate == 'Private']

        likes_targets \
            = public_profiles[~public_profiles.profileUrl.isin(excluded_profiles.profileUrl)]
        if in_load_num_profiles_likes is not None:
            likes_targets = likes_targets[0:in_load_num_profiles_likes]

        follow_targets = \
            private_profiles[~private_profiles.profileUrl.isin(excluded_profiles.profileUrl)]
        if in_load_num_profiles_follows is not None:
            follow_targets = follow_targets[0:in_load_num_profiles_follows]

        likes_target_profiles = likes_targets.profileUrl
        follow_target_profiles = follow_targets.profileUrl
        print('+++++ Like Profiles: {} / {}'.format(len(likes_target_profiles), len(profiles)))
        print('+++++ Follow Profiles: {} / {}'.format(len(follow_target_profiles), len(profiles)))

        # separate profiles by target
        num_profiles_likes = len(likes_target_profiles)
        num_profiles_follows = len(follow_target_profiles)
        likes_per_bot = int(num_profiles_likes / self.num_of_bots)
        follows_per_bot = int(num_profiles_follows / self.num_of_bots)

        self.likes_target_profiles_per_bot = []
        self.follow_target_profiles_per_bot = []
        self.probabilities_per_bot = []
        likes_last_idx = -1
        follows_last_idx = -1
        for i_bot in range(self.num_of_bots):
            likes_first_idx = likes_last_idx + 1
            likes_last_idx = likes_first_idx + likes_per_bot
            self.likes_target_profiles_per_bot.append(
                likes_target_profiles[likes_first_idx:likes_last_idx + 1])

            follows_first_idx = follows_last_idx + 1
            follows_last_idx = follows_first_idx + follows_per_bot
            self.follow_target_profiles_per_bot.append(
                follow_target_profiles[follows_first_idx:follows_last_idx + 1])

            if self.bots_data[i_bot] is None or KEY_ACTION_PROBS not in self.bots_data[i_bot]:
                self.probabilities_per_bot.append(None)
            else:
                self.probabilities_per_bot.append(self.bots_data[i_bot][KEY_ACTION_PROBS])
        # Report
        print('+++++ Print profiles')
        for i_bot, data in enumerate(zip(self.bots,
                                         self.likes_target_profiles_per_bot,
                                         self.follow_target_profiles_per_bot)):
            bot_name = data[0].name
            like_profiles = data[1]
            follow_profiles = data[2]
            print('  +++++ Bot: {}'.format(bot_name))
            if len(like_profiles) > 0:
                print('    +++++ - Likes:   first - {}'.format(like_profiles.values[0]))
                print('    +++++            last  - {}'.format(like_profiles.values[-1]))
            if len(follow_profiles) > 0:
                print('    +++++ - Follows: first - {}'.format(follow_profiles.values[0]))
                print('    +++++            last  - {}'.format(follow_profiles.values[-1]))

    def run(self, in_load_num_profiles_likes=None, in_load_num_profiles_follows=None):

        if self.test_on:
            print('+++++ TEST MODE')

        self.initialize_bots()
        self.load_profiles(in_load_num_profiles_likes, in_load_num_profiles_follows)

        self.bot_threads = []
        for i_bot, bot_data in enumerate(zip(self.bots,
                                             self.likes_target_profiles_per_bot,
                                             self.follow_target_profiles_per_bot,
                                             self.probabilities_per_bot)):
            bot_name = bot_data[0].name
            wait_time_s = uniform(i_bot * (self.launch_min_wait_time_m * 60),
                                  i_bot * (self.launch_max_wait_time_m * 60))
            print('+++++ ({}) Launch bot after {} s'.format(bot_name, timedelta(0, wait_time_s)))
            sleep(wait_time_s)

            bot_thread = Thread(target=self.bot_run_function, args=bot_data)
            bot_thread.start()
            self.bot_threads.append(bot_thread)

    def bot_run_function(self, bot, like_targets, follow_targets, probabilities=None,
                         in_function=FUNCTION_ENGAGE):

        if probabilities is None:
            probabilities = [0.80, 0.20]
        time_start = datetime.now()

        print('+++++ ({}) Start: {}'.format(bot.name, in_function.upper()))
        if in_function == FUNCTION_LIKE_FOLLOW:
            liked_profiles, followed_profiles = \
                bot.do_likes_and_follows(likes_targets=like_targets,
                                         likes_max_run_hours=self.likes_max_run_hours,
                                         follow_targets=follow_targets,
                                         follow_max_run_hours=self.follows_max_run_hours)
        elif in_function == FUNCTION_ENGAGE:
            liked_profiles, followed_profiles = \
                bot.engage(likes_targets=like_targets, follow_targets=follow_targets,
                           run_time_hours=self.follows_max_run_hours + self.likes_max_run_hours,
                           probabilities=probabilities)
        print('+++++ ({}) Liked profiles: {}'.format(bot.name, len(liked_profiles)))
        print('+++++ ({}) Followed profiles: {}'.format(bot.name, len(followed_profiles)))

        print('+++++ ({}) CLOSE after {}'.format(bot.name, datetime.now() - time_start))
        bot.close_session()

        print('+++++ ({}) GOOD BYE'.format(bot.name))


