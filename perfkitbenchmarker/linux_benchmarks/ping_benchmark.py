# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs ping.

This benchmark runs ping using the internal ips of vms in the same zone.
"""

import logging
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import flags
import re


BENCHMARK_NAME = 'ping'
BENCHMARK_CONFIG = """
ping:
  description: Benchmarks ping latency over external IP addresses \
      or internal IP address
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
      vm_count: 1
    vm_2:
      vm_spec: *default_single_core
      vm_count: 1
"""


flags.DEFINE_boolean('ping_use_external_ip', False, 'Runs ping using '
                                                    'external ip or internal ip.')
METRICS = ('Min Latency', 'Average Latency', 'Max Latency', 'Latency Std Dev', 'Packet loss rate')
FLAGS = flags.FLAGS
#ALI_REGIONS = {
#  '10.0.1.1': 'CN-Hangzhou',
#  '192.168.1.1': 'US-West'
#}

def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):  # pylint: disable=unused-argument
  """Install ping on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass


def Run(benchmark_spec):
  """Run ping on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  vms = benchmark_spec.vms
  for vm_idx1 in range(0, len(vms)):
    for vm_idx2 in range(vm_idx1+1, len(vms)):
      # if not vms[vm_idx1].IsPubReachable(vms[vm_idx2]):
      #   logging.warn('%s is not reachable from %s', vms[vm_idx2], vms[vm_idx1])
      #   return []
      vm = vms[vm_idx1]
      logging.info('Ping results:')
      metadata = {}
      if FLAGS.ping_use_external_ip == True:
        ping_cmd = 'ping -c 250 -i 1 %s' % vms[vm_idx2].ip_address
        metadata['ip_type'] = 'external'
#        metadata['from'] = ALI_REGIONS[vms[vm_idx1].ip_address]+'_'+vms[vm_idx1].ip_address
#        metadata['to'] = ALI_REGIONS[vms[vm_idx2].ip_address]+'_'+vms[vm_idx2].ip_address
      else:
        ping_cmd = 'ping -c 250 -i 1 %s' % vms[vm_idx2].internal_ip
        metadata['ip_type'] = 'internal'
#        metadata['from'] = ALI_REGIONS[vms[vm_idx1].internal_ip]+'_'+vms[vm_idx1].internal_ip
#        metadata['to'] = ALI_REGIONS[vms[vm_idx2].internal_ip]+'_'+vms[vm_idx2].internal_ip
      stdout, _ = vm.RemoteCommand(ping_cmd, should_log=True)
      stats = re.findall('([0-9]*\\.[0-9]*)', stdout.splitlines()[-1])
      stats += re.findall('([0-9]*\\.?[0-9]*)%', stdout.splitlines()[-2])
      assert len(stats) == len(METRICS), stats

      for i, metric in enumerate(METRICS[:-1]):
        results.append(sample.Sample(metric, float(stats[i]), 'ms', metadata))
      results.append(sample.Sample(METRICS[-1], float(stats[-1]), '%', metadata))
  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup ping on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
