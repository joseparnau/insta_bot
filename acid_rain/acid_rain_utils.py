# -*- coding: utf-8 -*-

import ast
import datetime
from pathlib import Path
from random import choice, choices, random
import string


def represents_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def list_to_string(in_list) -> str:
    """
    Converts a list into a literal string. Example: ['a', 'b'] -> "['a', 'b']"

    Params:
        in_list: list, the list

    Returns:
        The string
    """
    return "['" + "', '".join(map(str, in_list)) + "']" if in_list else "[]"


def string_to_list(in_string) -> list:
    """
    Converts a string into a list. Example: "['a', 'b']" -> ['a', 'b']

    Params:
        in_string: str, the string

    Returns:
        The list
    """
    return ast.literal_eval(in_string)


def string_to_date(in_string) -> datetime.datetime:
    """
    Convert a string with format '%Y-%m-%d %H:%M:%S.%f' to datetime

    Params:
        in_string: str, string to convert

    Returns:
        The datetime
    """
    return datetime.datetime.strptime(in_string, '%Y-%m-%d %H:%M:%S.%f')


def is_date(in_string) -> bool:
    """
    Return whether the string can be interpreted as a date with format %Y-%m-%d %H:%M:%S.%f.

    Params:
        in_string: str, string to check for date

    Returns:
        bool, whether the string is a date or not
    """
    try:
        string_to_date(in_string)
        return True
    except ValueError:
        return False


def get_random_bool(in_with_less_than_probability=0.5) -> bool:
    """
    Return a random bool
    """
    return random() < in_with_less_than_probability


def get_random_string(in_string_length=8) -> str:
    """
    Return a random string
    """
    letters = string.ascii_lowercase
    return ''.join(choice(letters) for _ in range(in_string_length))


def select_idx_by_prob(probabilities) -> int:
    """
    Return an index of the the list based on the probabilities specified in each element.
    Probabilities are normalized.

    Params:
        probabilities: list, list of unnormalized probabilities

    Returns:
        bool, the selected index
    """
    num_probs = len(probabilities)
    sum_probs = sum(probabilities)
    if sum_probs == 0.0:
        normalized_probabilities = [1/num_probs for _ in probabilities]
    else:
        normalized_probabilities = [prob/sum_probs for prob in probabilities]
    return choices(range(0, num_probs), weights=normalized_probabilities)[0]


def remove_repeated(in_list) -> list:
    """
    Return the list without repeated elements. Example: [1, 1, 2, 3, 4, 3, 4, 5] -> [2, 5]
    """
    counter_dict = {x: 0 for x in in_list}
    for elem in in_list:
        counter_dict[elem] = counter_dict[elem] + 1
    return [x for x in in_list if counter_dict[x] == 1]


def log_page_to_folder(folder, base_name, page_source):
    """
    Save page to folder

    Params:
        folder: str, folder to save
        base_name: str, base name of the file to save
        page_source: str, the page as txt to save
    """
    now_str = str(datetime.datetime.now()).replace('.', '').replace(' ', '_')
    log_fail_file_path = Path(folder) / (base_name + '_' + now_str + '.html')
    with open(log_fail_file_path, 'w') as f:
        f.write(page_source)


def check_action_blocked(in_page_source):
    """
    Check if an action blocked has been issued

    Params:
        in_page_source: str, the page as txt
    """
    return 'Action Blocked' in in_page_source


def check_wait_a_few_minutes(in_page_source):
    """
    Check if it is required to wait some minutes

    Params:
        in_page_source: str, the page as txt
    """
    return 'Please wait a few minutes before you try again' in in_page_source
