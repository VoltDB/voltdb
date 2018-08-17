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

import time

import pytest
from mock import Mock, patch


@pytest.fixture
def refresher():
    from voltsql.voltrefresher import VoltRefresher
    return VoltRefresher()


def test_constructor(refresher):
    """
    Refresher object should contain a few handlers
    :param refresher:
    :return:
    """
    assert len(refresher.refreshers) > 0
    actual_handlers = list(refresher.refreshers.keys())
    expected_handlers = ['tables', 'views', 'procedures', 'functions']
    assert expected_handlers == actual_handlers


def test_refresh_called_once(refresher):
    """

    :param refresher:
    :return:
    """
    callbacks = Mock()
    executor = Mock()
    completer = Mock()

    with patch.object(refresher, '_background_refresh') as background_refresh:
        actual = refresher.refresh(executor, completer, callbacks)
        time.sleep(1)  # Wait for the thread to work.
        assert actual == 'Auto-completion refresh started in the background.'
        # make sure the "_background_refresh" method is invoked
        background_refresh.assert_called_with(executor, completer, callbacks)


def test_refresh_called_twice(refresher):
    """
    If refresh is called a second time, it should be restarted
    :param refresher:
    :return:
    """
    callbacks = Mock()

    executor = Mock()
    completer = Mock()

    def dummy_background_refresh(*args):
        time.sleep(2)  # seconds

    refresher._background_refresh = dummy_background_refresh

    actual1 = refresher.refresh(executor, completer, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert actual1 == 'Auto-completion refresh started in the background.'

    actual2 = refresher.refresh(executor, completer, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert actual2 == 'Auto-completion refresh restarted.'


def test_refresh_with_callbacks(refresher):
    """
    Callbacks must be called
    :param refresher:
    """
    callbacks = [Mock()]
    executor = Mock()
    completer = Mock()

    # Set refreshers to 0: we're not testing refresh logic here
    refresher.refreshers = {}
    refresher.refresh(executor, completer, callbacks)
    time.sleep(1)  # Wait for the thread to work.
    assert (callbacks[0].call_count == 1)
