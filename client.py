#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
sys.path.append('gen-py')

from reloco import ProcStatsService
from reloco.ttypes import *
from reloco.constants import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import psutil
import time
from copy import deepcopy

try:

    # Make socket
    transport = TSocket.TSocket('localhost', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = ProcStatsService.Client(protocol)

    # Connect!
    transport.open()

    client.ping()
    print 'ping()'

    client.authenticate('ow3qn32nroiwe')

    pg_ids = {}
    autogroups = {}
    autogroups_old = None
    authkey = "djabda"

    while True:

        for proc in psutil.process_iter():

            try:
                local_id = (proc.name, proc.username)

                try:
                    pg_id = pg_ids[local_id]

                except KeyError:

                    if proc.create_time > time.time() - PROCESS_MIN_AGE_SECONDS:
                        continue

                    pg_ids[local_id] = pg_id = client.get_process_group(authkey, proc.name, proc.username)

                try:
                    pgs = autogroups[pg_id]
                except KeyError:
                    pgs = autogroups[pg_id] = ProcGroupStats()
                    pgs.pg_id = pg_id
                    pgs.cpu = ProcCpuStats()
                    pgs.mem = ProcMemoryStats()
                else:
                    pgs.processes += 1
            
                try:
                    mem_info = proc.get_memory_info()
                except psutil.error.AccessDenied:
                    print "memory access denied"
                else:
                    pgs.mem.rss += mem_info[0] / (1048576)
                    pgs.mem.vms += mem_info[1] / (1048576)
            
                try:
                    cpu_times = proc.get_cpu_times()
                except psutil.error.AccessDenied:
                    print "cpu access denied"
                else:
                    pgs.cpu.usr += cpu_times[0]
                    pgs.cpu.sys += cpu_times[1]
        
                #try:
                #    print proc.get_io_counters()
                #except psutil.error.AccessDenied:
                #    print "io access denied"

            except psutil.error.AccessDenied:
                continue

            except psutil.error.NoSuchProcess:
                continue

        to_send = filter(lambda pg: pg.cpu.sys != 0 or pg.mem.vms != 0, autogroups.values())

        client.store_bulk(authkey, to_send)

        autogroups_old = autogroups
        autogroups = {}

        time.sleep(2)

    # Close!
    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)

