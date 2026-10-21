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

# NOTE(arnaud) the options are not in impl_bmq, so they can be listed (e.g.
# by oslo-config-generator) without the blazingmq SDK installed
from oslo_config import cfg

BMQ_OPTS = [
    cfg.StrOpt(
        'ssl_ca_file',
        default='',
        help='Path to the file with the certificates of the authorities '
        'trusted to sign the broker certificate. When set, connect to the '
        'broker with TLS (version 1.3) and verify its certificate; the '
        'broker must listen with TLS. Requires a blazingmq SDK with TLS '
        'support.',
    ),
]
