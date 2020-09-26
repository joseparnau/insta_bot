#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 24 2020

a program that runs all the bots

@author: Josep-Arnau Claret
"""

import os
from pathlib import Path
import sys

ROOTDIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(ROOTDIR)

from acid_rain.bot_master import BotMaster, KEY_BOT_NAME, KEY_PASSWORD, KEY_DATA, KEY_ACTION_PROBS


def main():

    # Log folder
    log_folder = Path(ROOTDIR) / 'logs'

    # # TEST
    # test = True
    # # Databases
    # target_profiles_file = Path(ROOTDIR) / 'data_test/followers_all_test_jac.csv'
    # excluded_profiles_file = Path(ROOTDIR) / 'data_test/excluded_profiles_test_jac.csv'
    # bot_events_database_file_path = Path(ROOTDIR) / 'data_test/bot_register_event_db_test_jac.csv'
    # # Bot credentials
    # bots = [{KEY_BOT_NAME: 'TheBotName1', KEY_PASSWORD: 'TheBotPassword',
    #          KEY_DATA: {KEY_ACTION_PROBS: [0.8, 0.2]}},
    #          {KEY_BOT_NAME: 'TheBotName2', KEY_PASSWORD: 'TheBotPassword',
    #          KEY_DATA: {KEY_ACTION_PROBS: [0.8, 0.2]}}]  # [likes, follows]

    # REAL
    test = False
    # Databases
    target_profiles_file = Path(ROOTDIR) / 'data/input/followers_all.csv'
    excluded_profiles_file = Path(ROOTDIR) / 'data/excluded_profiles.csv'
    bot_events_database_file_path = Path(ROOTDIR) / 'data/bot_register_event_db.csv'
    # Bot credentials
    bots = [{KEY_BOT_NAME: 'TheBotName1', KEY_PASSWORD: 'TheBotPassword'},
            {KEY_BOT_NAME: 'TheBotName2', KEY_PASSWORD: 'TheBotPassword'}]

    # Initialization
    bot_master = BotMaster(bots,
                           target_profiles_file,
                           excluded_profiles_file,
                           bot_events_database_file_path,
                           test,
                           in_log_folder=log_folder)

    run_num_profiles_likes = 800
    run_num_profiles_follows = 800

    # Parameters
    bot_master.wait_after_block_hours = 6

    bot_master.launch_min_wait_time_m = 5
    bot_master.launch_max_wait_time_m = 10

    bot_master.likes_max_run_hours = 8
    bot_master.follows_max_run_hours = 8

    bot_master.likes_limit = 300
    bot_master.likes_max_per_day = 275
    bot_master.likes_max_per_hour = 40
    bot_master.likes_min_x_profile = 3
    bot_master.likes_max_x_profile = 5
    bot_master.likes_min_sleep_time_s = 10
    bot_master.likes_max_sleep_time_s = 15
    bot_master.likes_min_seconds_between_profiles = 0  # There is a already a 5 s sleep
    bot_master.likes_max_seconds_between_profiles = 5  # There is a already a 5 s sleep

    bot_master.follows_limit = 100
    bot_master.follows_max_per_day = 100
    bot_master.follows_max_per_hour = 10
    bot_master.follows_min_seconds_between_profiles = 4 * 60
    bot_master.follows_max_seconds_between_profiles = 8 * 60  # 5 min

    if test:
        # This values are for testing only
        run_num_profiles_likes = 5
        run_num_profiles_follows = 5

        bot_master.wait_after_block_hours = 0.01  # 36 s

        bot_master.launch_min_wait_time_m = 0.05  # 3 s
        bot_master.launch_max_wait_time_m = 0.1  # 6 s

        bot_master.likes_max_run_hours = 0.15  # 9 min
        bot_master.follows_max_run_hours = 0.15  # 9 min

        bot_master.likes_limit = 20
        bot_master.likes_max_per_day = 300
        bot_master.likes_max_per_hour = 750
        bot_master.likes_min_x_profile = 1
        bot_master.likes_max_x_profile = 4
        bot_master.likes_min_sleep_time_s = 1
        bot_master.likes_max_sleep_time_s = 2
        bot_master.likes_min_seconds_between_profiles = 5
        bot_master.likes_max_seconds_between_profiles = 10

        bot_master.follows_limit = 20
        bot_master.follows_max_per_day = 100
        bot_master.follows_max_per_hour = 50
        bot_master.follows_min_seconds_between_profiles = 5
        bot_master.follows_max_seconds_between_profiles = 10  # 5 min

    # Run
    bot_master.run(run_num_profiles_likes, run_num_profiles_follows)


if __name__ == "__main__":
    main()
