# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import threading
from collections import OrderedDict


class VoltRefresher(object):
    refreshers = OrderedDict()

    def __init__(self):
        self._refresher_thread = None
        # True if _refresher_thread is running and another refresh() is called
        self._restart_refresh = threading.Event()

    def refresh(self, executor, completer, callbacks):
        """
        Refresh the dbmetadata of completer in a background thread.
        """
        if self.is_refreshing():
            # if _refresher_thread is already running, force the previous one to refresh from beginning
            self._restart_refresh.set()
            return 'Auto-completion refresh restarted.'

        self._refresher_thread = threading.Thread(
            target=self._background_refresh,
            args=(executor, completer, callbacks),
            name='completion_refresh')
        self._refresher_thread.setDaemon(True)
        self._refresher_thread.start()
        return 'Auto-completion refresh started in the background.'

    def is_refreshing(self):
        return self._refresher_thread and self._refresher_thread.is_alive()

    def _background_refresh(self, executor, completer, callbacks):
        # If callbacks is a single function then push it into a list.
        if callable(callbacks):
            callbacks = [callbacks]

        while True:
            for refresher in self.refreshers.values():
                refresher(completer, executor)
                if self._restart_refresh.is_set():
                    # restart if a new refresh() is invoked
                    self._restart_refresh.clear()
                    break
            else:
                # Break out of while loop if the for loop finishes natually
                # without hitting the break statement.
                break

            # Start over the refresh from the beginning if the for loop hit the
            # break statement.
            continue

        for callback in callbacks:
            callback(completer)


def refresher(name, refreshers=VoltRefresher.refreshers):
    """
    Decorator to populate the dictionary of refreshers with the current function.
    """

    def wrapper(wrapped):
        refreshers[name] = wrapped
        return wrapped

    return wrapper


@refresher('tables')
def refresh_tables(completer, executor):
    completer.update_tables(executor.get_table_catalog())


@refresher('views')
def refresh_views(completer, executor):
    completer.update_views(executor.get_view_catalog())


@refresher('procedures')
def refresh_views(completer, executor):
    completer.update_procedures(executor.get_procedure_catalog())


@refresher('functions')
def refresh_views(completer, executor):
    completer.update_functions(executor.get_function_catalog())
