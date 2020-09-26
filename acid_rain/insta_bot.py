#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""File to manage the storing of bot events in a database"""

__author__ = "Josep-Arnau Claret"
__email__ = "joseparnau81@gmail.com"


from datetime import datetime, timedelta
from random import uniform, randint
from time import sleep

from acid_rain.bot_event_register import BotEventRegister, EVENT_LOGIN, EVENT_LIKES, EVENT_FOLLOW, \
    EVENT_EXCEPTION, EVENT_BLOCK
from acid_rain.acid_rain_constants import ONE_HOUR, ONE_DAY, ACTION_BLOCK
from acid_rain.insta_funcs import follow_profile, like_photos_profile, append_profile_as_row, \
    login, start_selenium, close_selenium
from acid_rain.acid_rain_utils import get_random_bool, select_idx_by_prob

ACTION_LIKE = 'like'
ACTION_FOLLOW = 'follow'
ACTION_LIST = [ACTION_LIKE, ACTION_FOLLOW]

EVENTS_KEEP_DURATION_S = 14 * 24 * 60 * 60  # 14 days
MAX_CONSECUTIVE_EXCEPTIONS = 3

SLEEP_AFTER_EXCEPTION_MIN_S = 3 * 60
SLEEP_AFTER_EXCEPTION_MAX_S = 5 * 60

MAX_RUN_HOURS = 3
WAIT_AFTER_BLOCK_HOURS = 24

# Likes
LIKES_MAX_PER_RUN = 100
LIKES_MAX_PER_DAY = 300
LIKES_MAX_PER_HOUR = 100
LIKES_MIN_X_PROFILE = 1
LIKES_MAX_X_PROFILE = 4
LIKES_MIN_SLEEP_TIME_S = 5
LIKES_MAX_SLEEP_TIME_S = 20
LIKES_MIN_SECONDS_BETWEEN_PROFILES = 5
LIKES_MAX_SECONDS_BETWEEN_PROFILES = 10

# Follow
FOLLOWS_MAX_PER_RUN = 300
FOLLOWS_MAX_PER_DAY = 100
FOLLOWS_MAX_PER_HOUR = 7
FOLLOWS_MIN_SECONDS_BETWEEN_PROFILES = 30
FOLLOWS_MAX_SECONDS_BETWEEN_PROFILES = 5 * 60  # 5 min

# Mock
MOCK_EXCEPTION_PROBABILITY = 0.1
MOCK_BLOCK_PROBABILITY = 0.1


def mock_like_photos_profile(name,
                             likes_min_x_profile, likes_max_x_profile,
                             likes_min_sleep_time_s, likes_max_sleep_time_s):
    exception_prob = MOCK_EXCEPTION_PROBABILITY
    block_prob = MOCK_BLOCK_PROBABILITY
    sleep(uniform(likes_min_sleep_time_s, likes_max_sleep_time_s))
    num_likes_done = randint(likes_min_x_profile, likes_max_x_profile)
    exception_found = get_random_bool(exception_prob)
    exception_cause = ACTION_BLOCK if exception_found and get_random_bool(block_prob) else ''
    print('({}) mock_like_photos_profile: {}'
          .format(name, 'EXCEPTION - ' + exception_cause if exception_found else num_likes_done))
    return num_likes_done, exception_found, exception_cause


def mock_follow_profile(name):
    exception_prob = MOCK_EXCEPTION_PROBABILITY
    block_prob = MOCK_BLOCK_PROBABILITY
    follow_done = get_random_bool(1.0 - exception_prob)
    exception_cause = ACTION_BLOCK if not follow_done and get_random_bool(block_prob) else ''
    print('({}) mock_follow_profile: {}'
          .format(name,
                  'followed' if follow_done else 'EXCEPTION - ' + exception_cause))
    return follow_done, exception_cause


class InstaBot:
    """
    Class that manages the bot
    """

    def __init__(self, in_username, in_password, in_excluded_profiles_file, in_events_file,
                 in_test, in_log_folder=None):
        """
        Bot constructor

        Params:
            username: str, username of the bot
            password: str, password of the bot
            in_excluded_profiles_file: str, file path with excluded profiles
            in_events_file: str, file path with the bot events
            in_test: bool, run in test or not
            in_log_folder: str, folder to log
        """

        self.test_on = in_test

        self.log_folder = in_log_folder

        self.name = in_username
        self.password = in_password

        self.time_start = None

        self.excluded_profiles_file = in_excluded_profiles_file
        self.events_file_path = in_events_file

        self.events_keep_duration_s = EVENTS_KEEP_DURATION_S
        self.max_run_hours = MAX_RUN_HOURS
        self.wait_after_block_hours = WAIT_AFTER_BLOCK_HOURS
        self.max_consecutive_exceptions = MAX_CONSECUTIVE_EXCEPTIONS

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

        # Initialize event register
        self.event_register = BotEventRegister(self.events_file_path)

        # Initialize bot
        self.bot = start_selenium()

        self.time_login = None

    def close_session(self):
        close_selenium(self.bot)

    def login(self):

        if self.time_login is None:
            if self.test_on:
                print('({}) Login mocked'.format(self.name))
            else:
                login(self.bot, self.name, self.password)
            self.time_login = datetime.now()

            keep_events_timestamp = self.time_login - timedelta(0, self.events_keep_duration_s)
            num_removed = self.event_register.remove_events_before(keep_events_timestamp)
            print('({}) Removed events: {}'.format(self.name, num_removed))

            self.event_register.add_event(self.name, EVENT_LOGIN)
            self.event_register.save()

    def do_likes(self, target_urls, in_jump_wait=False) -> list:
        """
        Sets the bot to do likes

        Params:
            target_urls: list, urls to like
            in_jump_wait: bool, jump the sleep after max count per hour reached

        Return:
            list, liked urls
        """

        print('({}) likes: START'.format(self.name))

        self.print_last_events('likes')

        if not self.waited_enough_after_last_block('likes'):
            return []

        self.login()

        time_start = datetime.now() if self.time_start is None else self.time_start

        likes_counter = 0
        num_consecutive_exceptions = 0
        profiles_liked = []
        for profile in target_urls:

            print(profile)

            if self.enough_counts_for_today('likes'):
                break

            # Wait conditions
            if self.wait_for_max_counts_per_hour('likes', in_jump_wait=in_jump_wait) \
                    and in_jump_wait:
                break
            self.sleep_for_next_profile('likes')

            # Like
            if self.test_on:
                num_likes_done, exception_found, exception_cause = \
                    mock_like_photos_profile(self.name,
                                             self.likes_min_x_profile,
                                             self.likes_max_x_profile,
                                             self.likes_min_sleep_time_s,
                                             self.likes_max_sleep_time_s)
            else:
                num_likes_done, exception_found, exception_cause = \
                    like_photos_profile(self.bot, profile,
                                        self.likes_min_x_profile, self.likes_max_x_profile,
                                        self.likes_min_sleep_time_s, self.likes_max_sleep_time_s,
                                        log_fail_folder=self.log_folder)

            # add profile to to already liked list
            profiles_liked.append(profile)
            append_profile_as_row(self.excluded_profiles_file, profile)

            if num_likes_done > 0:
                self.event_register.add_event(self.name, EVENT_LIKES, profile, num_likes_done)

            if exception_found:
                num_consecutive_exceptions += 1
                self.event_register.add_event(self.name, EVENT_EXCEPTION, in_comments='like')
                if exception_cause == ACTION_BLOCK:
                    self.event_register.add_event(self.name, EVENT_BLOCK, in_comments='like')
                self.wait_after_exception('like')
            else:
                num_consecutive_exceptions = 0

            self.event_register.save()

            likes_counter += num_likes_done

            run_time = datetime.now() - time_start
            print('({}) likes: Total: {} in {}'
                  .format(self.name,
                          self.event_register.get_number_of_likes_since(self.name,
                                                                        datetime.now() - run_time),
                          run_time))

            # Ending conditions
            if self.max_consecutive_exceptions_reached(num_consecutive_exceptions, 'likes'):
                raise  # Simply crash so that the navigator can be inspected

            if self.bot_is_blocked(exception_found, exception_cause, 'likes') or \
                    self.enough_run_time(run_time, 'likes') or \
                    self.enough_counts_in_run(likes_counter, 'likes'):
                break

            self.wait_for_max_count_rate(time_start, 'likes')

        return profiles_liked

    def do_follows(self, target_urls, in_jump_wait=False) -> list:
        """
        Sets the bot to do follows

        Params:
            target_urls: list, urls to follow
            in_jump_wait: bool, jump the sleep after max count per hour reached

        Return:
            list, followed urls
        """

        print('({}) follows: START'.format(self.name))

        self.print_last_events('follows')

        if not self.waited_enough_after_last_block('follows'):
            return []

        self.login()

        time_start = datetime.now() if self.time_start is None else self.time_start

        follows_counter = 0
        num_consecutive_exceptions = 0
        profiles_followed = []
        for my_profile in target_urls:

            print(my_profile)

            if self.enough_counts_for_today('follows'):
                break

            # Wait conditions
            if self.wait_for_max_counts_per_hour('follows', in_jump_wait=in_jump_wait) \
                    and in_jump_wait:
                break
            self.sleep_for_next_profile('follows')

            # Follow
            if self.test_on:
                follow_done, exception_cause = mock_follow_profile(self.name)
            else:
                follow_done, exception_cause = \
                    follow_profile(self.bot, my_profile, log_fail_folder=self.log_folder)

            # add profile to already followed list
            profiles_followed.append(my_profile)
            append_profile_as_row(self.excluded_profiles_file, my_profile)

            if follow_done:
                follows_counter += 1
                num_consecutive_exceptions = 0
                self.event_register.add_event(self.name, EVENT_FOLLOW, my_profile)
            else:
                num_consecutive_exceptions += 1
                self.event_register.add_event(self.name, EVENT_EXCEPTION, in_comments='follow')
                if exception_cause == ACTION_BLOCK:
                    self.event_register.add_event(self.name, EVENT_BLOCK, in_comments='follow')
                self.wait_after_exception('follow')

            self.event_register.save()

            run_time = datetime.now() - time_start
            print('({}) follows: Total: {} in {}'
                  .format(self.name,
                          self.event_register.get_number_of_follows_since(self.name,
                                                                          datetime.now() - run_time),
                          run_time))

            # Ending conditions
            if self.max_consecutive_exceptions_reached(num_consecutive_exceptions, 'follows'):
                raise  # Simply crash so that the navigator can be inspected

            if self.bot_is_blocked(not follow_done, exception_cause, 'follows') \
                    or self.enough_run_time(run_time, 'follows') \
                    or self.enough_counts_in_run(follows_counter, 'follows'):
                break

        return profiles_followed

    def do_likes_and_follows(self,
                             likes_targets=None, likes_max_run_hours=None,
                             follow_targets=None, follow_max_run_hours=None) -> tuple:
        # Likes
        liked_profiles = []
        if likes_targets is not None:
            self.max_run_hours = self.max_run_hours if likes_max_run_hours is None \
                else likes_max_run_hours
            liked_profiles = self.do_likes(likes_targets)

        # Follows
        followed_profiles = []
        if follow_targets is not None:
            self.max_run_hours = self.max_run_hours if follow_max_run_hours is None \
                else follow_max_run_hours
            followed_profiles = self.do_follows(follow_targets)

        return liked_profiles, followed_profiles

    def engage(self, likes_targets, follow_targets, run_time_hours=None,
               probabilities=None) -> tuple:
        """
        Runs likes and follows randomly until the profiles are ended or the maximum time is reached.

        Params:
            likes_targets: list, urls to like
            follow_targets: list, urls to follow
            run_time_hours: float, time of max duration in hours
            probabilities: list of floats, probability to do a like or a follow

        Return:
            tuple, liked and followed urls lists
        """

        num_actions = len(ACTION_LIST)

        if probabilities is None:
            probabilities = [1/num_actions for _ in range(num_actions)]
        assert len(probabilities) == num_actions, 'len(probabilities) != {}'.format(num_actions)
        probs_str = ', '.join(['{}: {}'.format(a.upper(), p)
                               for a, p in zip(ACTION_LIST, probabilities)])
        print('({}) Probabilities: {}'.format(self.name, probs_str))

        self.max_run_hours = None

        # Build targets
        targets = {ACTION_LIKE: likes_targets.values,
                   ACTION_FOLLOW: follow_targets.values}
        total_actions = {x: len(targets[x]) for x in ACTION_LIST}
        total_str = ', '.join(['{} ({})'.format(x.upper(), n) for x, n in total_actions.items()])
        print('({}) Targets: {}'.format(self.name, total_str))

        # Loop
        actions_count = {x: 0 for x in ACTION_LIST}
        profiles = {x: [] for x in ACTION_LIST}
        self.time_start = datetime.now()
        run_time = timedelta(0, 3600 * run_time_hours)
        while datetime.now() - self.time_start < run_time:
            sleep(5)  # 0.2 Hz

            if not self.waited_enough_after_last_block('engage', in_with_rnd=True):
                continue

            num_actions = {x: len(profiles[x]) for x in ACTION_LIST}
            actions_completed = [num_actions[x] >= total_actions[x] for x in ACTION_LIST]
            if all(actions_completed):
                break

            # Select action
            probabilities = [(not actions_completed[i]) * p for i, p in enumerate(probabilities)]
            selected_action = ACTION_LIST[select_idx_by_prob(probabilities)]
            selected_action_str = selected_action.upper()
            action_idx = actions_count[selected_action]
            print('({}) RUN **** {}: {} / {} profiles ****'
                  .format(self.name, selected_action_str,
                          action_idx, total_actions[selected_action]))

            # Run action
            new_target = targets[selected_action][action_idx]
            if selected_action == ACTION_LIKE:
                processed_profile = self.do_likes([new_target], in_jump_wait=True)
            elif selected_action == ACTION_FOLLOW:
                processed_profile = self.do_follows([new_target], in_jump_wait=True)

            success = len(processed_profile) > 0
            if success:
                actions_count[selected_action] += 1
                profiles[selected_action].append(processed_profile)
            print('({}) {} **** {} ****'
                  .format(self.name, 'DONE' if success else 'PASS', selected_action_str))

        return tuple(profiles[x] for x in ACTION_LIST)

    def print_last_events(self, in_source):
        past_day = datetime.now() - ONE_DAY
        past_hour = datetime.now() - ONE_HOUR
        if in_source == 'likes':
            last_day = self.event_register.get_number_of_likes_since(self.name, past_day)
            last_hour = self.event_register.get_number_of_likes_since(self.name, past_hour)
        elif in_source == 'follows':
            last_day = self.event_register.get_number_of_follows_since(self.name, past_day)
            last_hour = self.event_register.get_number_of_follows_since(self.name, past_hour)
        print('({}) {}: last day / hour: {} / {}'.format(self.name, in_source,
                                                         int(last_day), int(last_hour)))

    def waited_enough_after_last_block(self, in_source, in_with_rnd=False) -> bool:
        last_block_timestamp = self.event_register.get_last_block_timestamp(self.name)
        if last_block_timestamp is None:
            print('({}) {}: No block in database'.format(self.name, in_source))
            return True
        else:
            print('({}) {}: last block: {}'.format(self.name, in_source, last_block_timestamp))
            duration_last_block = datetime.now() - last_block_timestamp
            if not in_with_rnd:
                min_duration_after_block = timedelta(0, 3600 * self.wait_after_block_hours)
            else:
                duration = uniform(self.wait_after_block_hours, 2 * self.wait_after_block_hours)
                min_duration_after_block = timedelta(0, 3600 * duration)
            if duration_last_block < min_duration_after_block:
                print('({}) {}: not enough time after last block: {} < {}'
                      .format(self.name, in_source, duration_last_block, min_duration_after_block))
                return False
            else:
                return True

    def max_consecutive_exceptions_reached(self, num_consecutive_exceptions, in_source) -> bool:
        if num_consecutive_exceptions >= self.max_consecutive_exceptions:
            print('({}) {}: max number of consecutive exceptions reached: {}'.format(
                in_source, self.name, self.max_consecutive_exceptions))
            return True
        else:
            return False

    def bot_is_blocked(self, exception_found, exception_cause, in_source):
        if exception_found and exception_cause == ACTION_BLOCK:
            print("({}) {}: action has been BLOCKED".format(self.name, in_source))
            return True
        else:
            return False

    def enough_counts_for_today(self, in_source) -> bool:
        past_day = datetime.now() - ONE_DAY
        if in_source == 'likes':
            counts_last_day = self.event_register.get_number_of_likes_since(self.name, past_day)
            max_per_day = self.likes_max_per_day
        elif in_source == 'follows':
            counts_last_day = self.event_register.get_number_of_follows_since(self.name, past_day)
            max_per_day = self.follows_max_per_day

        if counts_last_day > max_per_day:
            print("({}) {}: max per day reached for bot: {} > {}"
                  .format(self.name, in_source, counts_last_day, max_per_day))
            return True
        else:
            return False

    def enough_run_time(self, run_time, in_source) -> bool:
        if self.max_run_hours is None:
            return False

        max_run_duration = timedelta(0, self.max_run_hours * 3600)
        if run_time > max_run_duration:
            print("({}) {}}: max run time reached for bot: {:.1f} > {:.1f} hours"
                  .format(self.name, in_source, run_time.seconds / 3600, self.max_run_hours))
            return True
        else:
            return False

    def enough_counts_in_run(self, counts, in_source) -> bool:
        if in_source == 'likes':
            limit = self.likes_limit
        elif in_source == 'follows':
            limit = self.follows_limit

        if limit is None:
            return False

        if counts > limit:
            print('({}) {}: enough: {} > {}'.format(self.name, in_source, counts, limit))
            return True
        else:
            return False

    def sleep_for_next_profile(self, in_source):
        if in_source == 'likes':
            sleep_time = uniform(self.likes_min_seconds_between_profiles,
                                 self.likes_max_seconds_between_profiles)
        elif in_source == 'follows':
            sleep_time = uniform(self.follows_min_seconds_between_profiles,
                                 self.follows_max_seconds_between_profiles)
        duration = timedelta(0, sleep_time)
        print('({}) {}: wait for next profile: {}'.format(self.name, in_source, duration))
        sleep(sleep_time)

    def wait_for_max_counts_per_hour(self, in_source, in_jump_wait=False) -> bool:
        past_hour = datetime.now() - ONE_HOUR
        if in_source == 'likes':
            counts = self.event_register.get_number_of_likes_since(self.name, past_hour)
            max_counts = self.likes_max_per_hour
        elif in_source == 'follows':
            counts = self.event_register.get_number_of_follows_since(self.name, past_hour)
            max_counts = self.follows_max_per_hour

        if counts > max_counts:
            if in_jump_wait:
                print("({}) {}: max per hour reached for bot: {} > {}"
                      .format(self.name, in_source, counts, max_counts))
            else:
                wait_time_in_s = uniform(15 * 60, 30 * 60)  # Wait between 15 and 30 min
                print("({}) {}: max per hour reached for bot: {} > {}: wait {:.2f} min"
                      .format(self.name, in_source, counts, max_counts, wait_time_in_s / 60))
                sleep(wait_time_in_s)
            return True
        else:
            return False

    def wait_for_max_count_rate(self, time_start, in_source):
        if in_source == 'likes':
            run_time = datetime.now() - time_start
            number_of_likes_in_run = \
                self.event_register.get_number_of_likes_since(self.name, time_start)
            max_likes_per_sec = self.likes_max_per_hour / 3600
            max_likes_in_run = run_time.seconds * max_likes_per_sec
            if number_of_likes_in_run > max_likes_in_run:
                wait_time_in_sec = number_of_likes_in_run / max_likes_per_sec - run_time.seconds
                wait_time_in_sec = uniform(wait_time_in_sec, 2 * wait_time_in_sec)
                print('({}) {}: Too fast: {} > {}: wait {} sec'
                      .format(self.name, in_source,
                              number_of_likes_in_run, max_likes_in_run,
                              wait_time_in_sec))
                sleep(wait_time_in_sec)
                return True
            else:
                rate_counts_per_hour = number_of_likes_in_run / (run_time.seconds / 3600)
                print('({}) {}: rate count / hour: {} < {}'
                      .format(self.name, in_source, rate_counts_per_hour, self.likes_max_per_hour))
        return False

    def wait_after_exception(self, in_source):
        wait_time_in_sec = uniform(SLEEP_AFTER_EXCEPTION_MIN_S, SLEEP_AFTER_EXCEPTION_MAX_S)
        print("({}) {}: wait after exception for {}"
              .format(self.name, in_source, timedelta(0, wait_time_in_sec)))
        sleep(wait_time_in_sec)
