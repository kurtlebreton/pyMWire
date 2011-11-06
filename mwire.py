# Copyright 2011 Kurt Le Breton, All Rights Reserved

# NOTE: TODO(follow up with mgateway.com)
#   The following commands don't follow documented M/Wire protocol 
#   - SET;          adds an additional server line, $11, for '{"ok":true}'; not +OK
#   - KILL;         +ok returned not +OK
#   - SETSUBTREE;   inconsistency between commands, SET abc number, SETSUBTREE abc then number on next line
#                   SET sends text without "" but SETSUBTREE sends them with ""
#                   !!!! NOT WORKING AS DESCRIBED IN DOCUMENT !!!!  SEE UNIT TEST FOR OUTPUT
#   - PING;         +PONG can be +OK for consistency?
#   - Inconsistencies with regard to $-1, since it can refer to '' or Null

import csv
import socket
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

CRLF = '\r\n'

class M(object):
    """
    Implementation of the M/Wire protocol.
    """

    def __init__(self, host='localhost', port=6330):
        self._connection = Connection(host, port)

    def connect(self):
        self._connection.connect()

    def disconnect(self):
        self._connection.disconnect()
    
    def read_length(self, length):
        return self._connection.read(length)
    
    def read_line(self):
        return self._connection.read()
    
    def send_line(self, text):
        self._connection._sock.sendall(text + CRLF)
        
    def _subscripts(self, subscripts):
        max = len(subscripts) - 1
        if max > -1:
            text = '['
            for i in range(max + 1):
                # Attempt to concatenate as a string but if this
                # fails then we can conclude it's a number.
                try:
                    text = text + '"' + subscripts[i] + '"'
                except TypeError:
                    text = text + str(subscripts[i])
                if i < max:
                    text = text + ','
            return text + ']'
        else:
            return ''
            
    def decrement(self, node, subscripts):
        return self._increment_decrement(node, subscripts, -1)
        
    def decrement_by(self, node, subscripts, amount):
        return self._increment_decrement(node, subscripts, -amount)
            
    def _increment_decrement(self, node, subscripts, amount):
        self.connect()
        #  INCR test1["a","b"]
        #  INCRBY test1["a","b"] 12
        #  DECR test1["a","b"]
        #  DECRBY test1["a","b"] 12
        if amount >= 2:
            text = 'INCRBY ' + node + self._subscripts(subscripts) + ' ' + str(amount)
        elif amount == 1:
            text = 'INCR ' + node + self._subscripts(subscripts)
        elif amount == -1:
            text = 'DECR ' + node + self._subscripts(subscripts)
        elif amount <= -2:
            text = 'DECRBY ' + node + self._subscripts(subscripts) + ' ' + str(-amount)
        else:
            raise ProtocolError()
        self.send_line(text)
        # :3, :1
        text = self.read_line()
        if text[0] != ':':
            raise ProtocolError()
        return int(text[1:])
        
    def exists(self, node, subscripts):
        self.connect()
        #  EXISTS test1[1,"y"]
        text = 'EXISTS ' + node + self._subscripts(subscripts)
        self.send_line(text)
        text = self.read_line()
        if text[0] != ':':
            raise ProtocolError()
        # :0, :1, :10, :11
        return int(text[1:])
    
    def function(self):
        raise ToDoError()

    def get(self, node, subscripts):
        self.connect()
        # GET test1["a","b"]
        text = 'GET ' + node + self._subscripts(subscripts)
        self.send_line(text)
        # $5
        text = self.read_line()
        if text[0] != '$':
            raise ProtocolError()
        # $-1
        length = int(text[1:])
        if length < 0:
            return None
        # $0
        if length == 0:
            # Read the last empty line
            return self.read_line()
        # Hello World
        return self.read_length(length)
    
    def getallsubs(self, node, subscripts):
        self.connect()
        # GETALLSUBS test1["a","b"]
        text = 'GETALLSUBS ' + node + self._subscripts(subscripts)
        self.send_line(text)
        # *7
        text = self.read_line()
        if text[0] != '*':
            raise ProtocolError()
        # Prepare the returning list
        data = []
        # Iterate the number of results
        for i in range(0, int(text[1:]), 2):
            item = [None, None]
            data.append(item)
            # $1 - subscript
            text = self.read_line()
            if text[0] != '$':
                raise ProtocolError()
            length = int(text[1:])
            if length > -1:
                item[0] = self.read_length(length)
                # $51 - data value
                text = self.read_line()
                if text[0] != '$':
                    raise ProtocolError()
                length = int(text[1:])
                if length > -1:
                    item[1] = self.read_length(length)
        return data
    
    def getsubtree(self, node, subscripts):
        self.connect()
        # GETSUBTREE test1["a","b"]
        text = 'GETSUBTREE ' + node + self._subscripts(subscripts)
        self.send_line(text)
        # *7
        text = self.read_line()
        if text[0] != '*':
            raise ProtocolError()
        # Prepare the returning list
        data = []
        # Iterate the number of results
        for i in range(0, int(text[1:]), 2):
            item = [None, '']
            data.append(item)
            # $1 - subscript
            text = self.read_line()
            if text[0] != '$':
                raise ProtocolError()
            length = int(text[1:])
            if length > -1:
                item[0] = self.read_length(length)
            # $51 - data value
            text = self.read_line()
            if text[0] != '$':
                raise ProtocolError()
            length = int(text[1:])
            if length > -1:
                item[1] = self.read_length(length)
        return data
    
    def halt(self):
        self.connect()
        self.send_line('HALT')
        return True
            
    def increment(self, node, subscripts):
        return self._increment_decrement(node, subscripts, 1)
        
    def increment_by(self, node, subscripts, amount):
        return self._increment_decrement(node, subscripts, amount)
            
    def kill(self, node, subscripts):
        self.connect()
        #  KILL test1["a","b"]
        text = 'KILL ' + node + self._subscripts(subscripts)
        self.send_line(text)
        text = self.read_line()
        # +ok
        if text == '+ok':
            return True
        else:
            return False
    
    def lock(self, node, subscripts):
        raise ToDoError()
    
    def mdate(self):
        raise ToDoError()
    
    def monitor(self):
        raise ToDoError()
    
    def mversion(self):
        raise ToDoError()

    def next(self, node, subscripts):
        text = 'NEXT ' + node + self._subscripts(subscripts)
        return self._next_previous(text, node, subscripts)
        
    def previous(self, node, subscripts):
        text = 'PREVIOUS ' + node + self._subscripts(subscripts)
        return self._next_previous(text, node, subscripts)
            
    def _next_previous(self, command, node, subscripts):
        self.connect()
        # NEXT myArray[1,""]
        # PREVIOUS myArray[1,""]
        self.send_line(command)
        # $12
        text = self.read_line()
        if text[0] != '$':
            raise ProtocolError()
        # $-1, $12
        length = int(text[1:])
        if length < 0:
            return None
        # sub2, 12.98
        return self.read_length(length)
        
    def ping(self):
        self.connect()
        self.send_line('PING')
        if self.read_line() == '+PONG':
            return True
        else:
            return False
            
    def query(self, node, subscripts):
        self.connect()
        # QUERY test1["a","b"]
        text = 'QUERY ' + node + self._subscripts(subscripts)
        self.send_line(text)
        # $5
        text = self.read_line()
        if text[0] != '$':
            raise ProtocolError()
        # $-1
        length = int(text[1:])
        if length < 0:
            return None
        # test1["a","C"]
        return self.read_length(length)
    
    def queryget(self, node, subscripts):
        self.connect()
        data = [None, None]
        # QUERYGET test1["a","b"]
        text = 'QUERYGET ' + node + self._subscripts(subscripts)
        self.send_line(text)
        # *2, $-1
        text = self.read_line()
        if text == '$-1':
            pass
        elif text != '*2':
            raise ProtocolError()
        else:
            # $14 - node and subscript
            text = self.read_line()
            if text[0] != '$':
                raise ProtocolError()
            length = int(text[1:])
            data[0] = self.read_length(length)
            # $51 - data value
            text = self.read_line()
            if text[0] != '$':
                raise ProtocolError()
            length = int(text[1:])
            data[1] = self.read_length(length)
        return data
        
    def set(self, node, subscripts, value):
        self.connect()
        # SET test1["a","b"] 5
        # hello
        text = 'SET ' + node + self._subscripts(subscripts) + ' ' + str(len(value))
        self.send_line(text)
        self.send_line(value)
        # $11
        text = self.read_line()
        if text[0] != "$":
            return False
        text = self.read_line()
        # {"ok":true}
        if text == '{"ok":true}':
            return True
        else:
            return False
        
    def setsubtree(self, node, subscripts, data):
        raise ToDoError()
        self.connect()
        # SETSUBTREE test1["a","b"]
        text = 'SETSUBTREE ' + node + self._subscripts(subscripts)
        self.send_line(text)
        # *4 - record count following
        count = len(data)
        text = '*' + str(2 * count)
        self.send_line(text)
        # Iterate the records
        for i in range(count):
            # $3 - subscript
            text = '$' + str(len(data[i][0]) + 2)
            self.send_line(text)
            # abc - subscript
            text = '"' + data[i][0] + '"'
            self.send_line(text)
            # $4 - data value ($-1, None)
            text = '$' + str(len(data[i][1]) + 2)
            self.send_line(text)
            # wxyz - data value
            text = '"' + data[i][1] + '"'
            self.send_line(text)
        text = self.read_line()
        # {"ok":true}
        if text == '{"ok":true}':
            return True
        else:
            return False
    
    def transaction_start(self):
        raise ToDoError()
    
    def transaction_commit(self):
        raise ToDoError()
    
    def transaction_rollback(self):
        raise ToDoError()

    def unlock(self, node, subscripts):
        raise ToDoError()
    
    def version(self):
        raise ToDoError()

def split_csv(line, *args, **kwargs):
    try:
        buffer = StringIO(line)
        reader = csv.reader(buffer, *args, **kwargs)
        return reader.next()
    finally:
        buffer.close()
        
class SocketLineReader(object):
    MAX_LENGTH = pow(2, 19) # 534,288

    def __init__(self):
        self._file = None

    def __del__(self):
        try:
            self.on_disconnect()
        except:
            pass

    def on_connect(self, connection):
        self._file = connection._sock.makefile('r')

    def on_disconnect(self):
        if self._file is not None:
            self._file.close()
            self._file = None

    def read(self, length=None):
        try:
            if length is None:
                return self._file.readline()[:-2]
            else:
                remaining = length + 2
                if length <= self.MAX_LENGTH:
                    return self._file.read(remaining)[:-2]
                else:
                    try:
                        buffer = StringIO()
                        while remaining > 0:
                            read_length = min(remaining, self.MAX_LENGTH)
                            buffer.write(self._file.read(read_length))
                            remaining -= read_length
                        buffer.seek(0)
                        return buffer.read(length)
                    finally:
                        buffer.close()
            
        except (socket.error, socket.timeout), e:
            raise ConnectionError("Error while reading from socket: %s" % (e.args,))

class Connection(object):
    def __init__(self, host='localhost', port=6330):
        self.host = host
        self.port = port
        self.socket_timeout = 5
        self._sock = None
        self._reader = SocketLineReader()

    def __del__(self):
        try:
            self.disconnect()
        except:
            pass

    def connect(self):
        if self._sock:
            return
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(self.socket_timeout)
            self._sock.connect((self.host, self.port))
        except socket.error, e:
            self._sock = None
            raise ConnectionError(self._error_message(e))
        self.on_connect()

    def _error_message(self, exception):
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

    def on_connect(self):
        self._reader.on_connect(self)

    def disconnect(self):
        self._reader.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def read(self, length=None):
        try:
            response = self._reader.read(length)
        except:
            self.disconnect()
            raise
        if response.__class__ == ResponseError:
            raise response
        return response

class MWireError(Exception):
    pass

class ToDoError(MWireError):
    pass

class ProtocolError(MWireError):
    pass

class ConnectionError(MWireError):
    pass

class ResponseError(MWireError):
    pass
    