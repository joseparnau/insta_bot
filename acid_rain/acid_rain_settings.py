#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""File to store common / global variables"""

import threading


class CustomRLock(threading._PyRLock):

    @property
    def acquired(self):
        return bool(self._count)

    @property
    def owner(self):
        return self._owner


use_global_database = False
global_events_db = None
global_bot_lock = CustomRLock()
