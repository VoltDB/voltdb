# This file is part of VoltDB.
# Copyright (C) 2008-2020 VoltDB Inc.
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

# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# * Neither the name of the {organization} nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
