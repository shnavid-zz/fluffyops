#
# Autogenerated by Thrift Compiler (0.8.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None



class NotAuthorisedException(TException):
  """
  Attributes:
   - errorMessage
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'errorMessage', None, None, ), # 1
  )

  def __init__(self, errorMessage=None,):
    self.errorMessage = errorMessage

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.errorMessage = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('NotAuthorisedException')
    if self.errorMessage is not None:
      oprot.writeFieldBegin('errorMessage', TType.STRING, 1)
      oprot.writeString(self.errorMessage)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class AuthTimeoutException(TException):
  """
  Attributes:
   - errorMessage
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'errorMessage', None, None, ), # 1
  )

  def __init__(self, errorMessage=None,):
    self.errorMessage = errorMessage

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.errorMessage = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('AuthTimeoutException')
    if self.errorMessage is not None:
      oprot.writeFieldBegin('errorMessage', TType.STRING, 1)
      oprot.writeString(self.errorMessage)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ProcCpuStats:
  """
  Attributes:
   - usr
   - sys
  """

  thrift_spec = (
    None, # 0
    (1, TType.DOUBLE, 'usr', None, 0, ), # 1
    (2, TType.DOUBLE, 'sys', None, 0, ), # 2
  )

  def __init__(self, usr=thrift_spec[1][4], sys=thrift_spec[2][4],):
    self.usr = usr
    self.sys = sys

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.DOUBLE:
          self.usr = iprot.readDouble();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.DOUBLE:
          self.sys = iprot.readDouble();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ProcCpuStats')
    if self.usr is not None:
      oprot.writeFieldBegin('usr', TType.DOUBLE, 1)
      oprot.writeDouble(self.usr)
      oprot.writeFieldEnd()
    if self.sys is not None:
      oprot.writeFieldBegin('sys', TType.DOUBLE, 2)
      oprot.writeDouble(self.sys)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ProcMemoryStats:
  """
  Attributes:
   - vms
   - rss
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'vms', None, 0, ), # 1
    (2, TType.I64, 'rss', None, 0, ), # 2
  )

  def __init__(self, vms=thrift_spec[1][4], rss=thrift_spec[2][4],):
    self.vms = vms
    self.rss = rss

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.vms = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I64:
          self.rss = iprot.readI64();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ProcMemoryStats')
    if self.vms is not None:
      oprot.writeFieldBegin('vms', TType.I64, 1)
      oprot.writeI64(self.vms)
      oprot.writeFieldEnd()
    if self.rss is not None:
      oprot.writeFieldBegin('rss', TType.I64, 2)
      oprot.writeI64(self.rss)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ProcGroupStats:
  """
  Attributes:
   - pg_id
   - processes
   - cpu
   - mem
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'pg_id', None, None, ), # 1
    (2, TType.I32, 'processes', None, 1, ), # 2
    (3, TType.STRUCT, 'cpu', (ProcCpuStats, ProcCpuStats.thrift_spec), None, ), # 3
    (4, TType.STRUCT, 'mem', (ProcMemoryStats, ProcMemoryStats.thrift_spec), None, ), # 4
  )

  def __init__(self, pg_id=None, processes=thrift_spec[2][4], cpu=None, mem=None,):
    self.pg_id = pg_id
    self.processes = processes
    self.cpu = cpu
    self.mem = mem

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.pg_id = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.processes = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRUCT:
          self.cpu = ProcCpuStats()
          self.cpu.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRUCT:
          self.mem = ProcMemoryStats()
          self.mem.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ProcGroupStats')
    if self.pg_id is not None:
      oprot.writeFieldBegin('pg_id', TType.STRING, 1)
      oprot.writeString(self.pg_id)
      oprot.writeFieldEnd()
    if self.processes is not None:
      oprot.writeFieldBegin('processes', TType.I32, 2)
      oprot.writeI32(self.processes)
      oprot.writeFieldEnd()
    if self.cpu is not None:
      oprot.writeFieldBegin('cpu', TType.STRUCT, 3)
      self.cpu.write(oprot)
      oprot.writeFieldEnd()
    if self.mem is not None:
      oprot.writeFieldBegin('mem', TType.STRUCT, 4)
      self.mem.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    if self.pg_id is None:
      raise TProtocol.TProtocolException(message='Required field pg_id is unset!')
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
