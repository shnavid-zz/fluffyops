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
import os

from reloco import ProcStatsService
from reloco.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from hashlib import md5

import rrdtool

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class System(Base):
    __tablename__ = "systems"

    id = Column(Integer, primary_key = True)
    hostname = Column(String, unique = True, nullable = False)

class SystemToken(Base):
    __tablename__ = "system_tokens"

    id = Column(Integer, primary_key = True)
    token = Column(String, unique = True, nullable = False)
    system_id = Column(Integer, ForeignKey(System.id))
    system = relationship(System, backref=backref('tokens'))

engine = create_engine('sqlite:///:memory:', echo=True)
Session = sessionmaker(bind=engine)

Base.metadata.create_all(engine)

class ProcStatsServiceHandler:

    def __init__(self):
        self.log = {}

    def ping(self):
        print 'ping()'

    def authenticate(self, token):
        session = Session()
        tok = session.query(SystemToken).filter(SystemToken.token == token).first()
        if not tok and AUTOCREATE_TOKENS:
            tok = SystemToken()
        print tok
        return token

    def validate_authkey(self, authkey):
        return

    def get_process_group(self, authkey, username, procname):
        print "get_process_group()", username, procname
        m = md5()
        m.update(username+procname)
        key = m.hexdigest()
        rrd_fname = "rrd/%s.rrd" % (key)
        if not os.path.exists(rrd_fname):
            ret = rrdtool.create(rrd_fname, "--step", "60", "--start", '0',
                             "DS:processes:GAUGE:300:U:U",
                             "RRA:AVERAGE:0.5:1:600",
                             "RRA:AVERAGE:0.5:6:700",
 "RRA:AVERAGE:0.5:24:775",
 "RRA:AVERAGE:0.5:288:797",
 "RRA:MAX:0.5:1:600",
 "RRA:MAX:0.5:6:700",
 "RRA:MAX:0.5:24:775",
 "RRA:MAX:0.5:444:797")
        return key

    def _store(self, pgs):
        print 'store()', pgs
        rrdtool.update("rrd/%s.rrd" % (pgs.pg_id), "N:%d" % (pgs.processes))

    def store_bulk(self, authkey, pgs_list):
        self.validate_authkey(authkey)
        print "bulk() %d items" % (len(pgs_list))
        for pg in pgs_list:
            self._store(pg)

handler = ProcStatsServiceHandler()
processor = ProcStatsService.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

print 'Starting the server...'
server.serve()
print 'done.'
