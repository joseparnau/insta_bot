#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
from random import uniform, randint

from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException, \
    WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

from acid_rain.acid_rain_constants import ACTION_BLOCK, WAIT_MINS
from acid_rain.acid_rain_utils import log_page_to_folder, check_action_blocked, \
    check_wait_a_few_minutes


def start_selenium():
    bot = webdriver.Chrome(ChromeDriverManager().install())
    return bot


def close_selenium(bot):
    bot.quit()


def login(bot, username, password):
    bot.get('https://www.instagram.com/accounts/login/?source=auth_switcher')
    sleep(uniform(2.0, 3.0))
    bot.find_element_by_name('username').send_keys(username)
    sleep(uniform(0.5, 1.0))
    bot.find_element_by_name('password').send_keys(password)
    sleep(uniform(0.5, 1.0))
    bot.find_element_by_tag_name('form').submit()
    sleep(uniform(3.0, 4.0))
    bot.find_element_by_xpath("//button[contains(text(),'Not Now')]").click()


def like_photos_profile(bot, profiler_url, min_target_likes, max_target_likes, min_time=20,
                        max_time=60, log_fail_folder=None):
    """ does number of likes given a profileUrl and number of likes.
    
    Args:
        bot: chromedriver bot to use
        profiler_url: full url of the target profile
        min_target_likes: min number of likes that we want to do.
        max_target_likes: max number of likes that we want to do.
        min_time: min sleep time.
        max_time: max sleep time.
        log_fail_folder: str, folder to save data in case of failure.
        
    Returns: 
        n_exit: number of likes done successfully
        bool: if an exception was found
    """

    bot.get(profiler_url)  # go to profile

    # Get num of posts
    try:
        num_posts = bot.find_element_by_xpath(
            '//*[@id="react-root"]/section/main/div/header/section/ul/li[1]/span/span').text
    except NoSuchElementException as e:
        print("like_photo 1: NoSuchElementException")
        return 0, True, ''
    except WebDriverException as e:
        print("like_photo 1: WebDriverException")
        return 0, True, ''
    num_posts = int(num_posts.replace(',', ''))

    # number of likes that we will do
    max_likes = min(num_posts, max_target_likes)
    min_likes = min(min_target_likes, max_likes)
    num_likes = randint(min_likes, max_likes)
    print('Posts / expected likes: {} / {}'.format(num_posts, num_likes))

    sleep(uniform(1, 3))

    success_likes = 0  # variable to store the successful likes
    exception_found = False
    exception_cause = ''
    for i_photo in range(num_likes):
        print('- photo:', i_photo)

        try:
            element = '_9AhH0' if i_photo == 0 else 'coreSpriteRightPaginationArrow'
            bot.find_element_by_class_name(element).click()  # next photo
            print('  - photo found')
            sleep(uniform(min_time, max_time))

            # Fer like
            bot.find_element_by_xpath(
                '/html/body/div[4]/div[2]/div/article/div[2]/section[1]/span[1]/button').click()
            print('  - like done')
            sleep(uniform(min_time, max_time))

            success_likes += 1  # counter of successful likes
        except (NoSuchElementException, ElementClickInterceptedException) as e:
            exception_found = True
            print("Exception when about to like")
            if log_fail_folder is not None:
                log_page_to_folder(log_fail_folder, 'fail_like', bot.page_source)
            if check_action_blocked(bot.page_source):
                exception_cause = ACTION_BLOCK

        if exception_found:
            # Let's just quit if one exception is found
            break

    print('likes done = {} / {} {}'.format(success_likes,
                                           num_likes,
                                           'EXCEPTION FOUND' if exception_found else ''))
    return success_likes, exception_found, exception_cause


def follow_profile(bot, profile_url, log_fail_folder=None):
    """ does a follow to profileUrl

    Args:
        bot: chromedriver bot to use
        profile_url: full url of the target profile
        log_fail_folder: str, folder to save data in case of failure.

    Returns:
        int: success or not
    """

    bot.get(profile_url)  # go to profile
    sleep(uniform(1, 2))

    exception_cause = ''
    try:
        bot.find_element_by_xpath("//*[text()='Follow']").click()
        return 1, exception_cause
    except:
        print("Unable to follow: {}".format(profile_url))
        if log_fail_folder is not None:
            log_page_to_folder(log_fail_folder, 'fail_follow', bot.page_source)
        if check_action_blocked(bot.page_source):
            exception_cause = ACTION_BLOCK
        return 0, exception_cause


def append_profile_as_row(file_name, new_profileUrl):
    # Open file in append mode
    file = open(file_name, 'a')
    file.write(new_profileUrl+'\n')
    file.close()
