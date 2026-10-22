# Copyright 2014 eNovance
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os

import eventlet

# NOTE(arnaud) set OSLO_MESSAGING_TEST_EVENTLET=0 to run the tests with
# native threads, e.g. the functional tests of a driver whose library runs
# its own threads, which does not work with eventlet monkey patching
if os.environ.get('OSLO_MESSAGING_TEST_EVENTLET', '1') != '0':
    eventlet.monkey_patch()
