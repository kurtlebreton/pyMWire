# Copyright 2011 Kurt Le Breton, All Rights Reserved

import mwire
import unittest

class MWireFunctions(unittest.TestCase):

    def setUp(self):
        self.m = mwire.M('192.168.0.6', 6330)

    def test_exists_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        test1:                        11
        test1[1]:                     10
        test1[1,"x"]:                  1
        test1[1,"y"]:                 11
        test1[1,"y","hello world"]:    1
        test1[1,"z"]:                 10
        test1["x"]:                    0
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual(11, self.m.exists('test1', []))
        
        for i in range(2):
            self.assertEqual(10, self.m.exists('test1', [1]))
        
        for i in range(2):
            self.assertEqual(1, self.m.exists('test1', [1, 'x']))
        
        for i in range(2):
            self.assertEqual(11, self.m.exists('test1', [1, 'y']))
        
        for i in range(2):
            self.assertEqual(1, self.m.exists('test1', [1, 'y', 'hello world']))
        
        for i in range(2):
            self.assertEqual(10, self.m.exists('test1', [1, 'z']))
        
        for i in range(2):
            self.assertEqual(0, self.m.exists('test1', ['x']))
            
        self.assertEqual(False, bool(0))
        self.assertEqual(True, bool(1))
        self.assertEqual(True, bool(10))
        self.assertEqual(True, bool(11))

    def test_decrement_01(self):
        """
        Client: DECR test1["a","b"]
        Server: 2
        """
        
        self.m.kill('test1', [])
        self.m.set('test1', ['a', 'b'], '20')

        for i in range(20):
            self.assertEqual(20 - (i + 1), self.m.decrement('test1', ['a', 'b']))

    def test_get_01(self):
        """
        Client: GET test1["a","b"]
        Server: $5
        Server: Hello
        Client: GET test1
        Server: $22
        Server: This is the top level!
        """
        
        self.m.set('test1', ['a', 'b'], 'Hello')
        for i in range(2):
            self.assertEqual('Hello', self.m.get('test1', ['a', 'b']))
        
        self.m.set('test1', [], 'This is the top level!')
        for i in range(2):
            self.assertEqual('This is the top level!', self.m.get('test1', []))
            
    def test_get_02(self):
        """
        Client: GET test1["xxx"]
        Server: $-1
        """
        
        self.m.kill('xxx', [])
        for i in range(2):
            self.assertEqual(None, self.m.get('xxx', []))
            
    def test_get_03(self):
        """
        Client: GET test1["yyy"]
        Server: $0
        Server:
        """
        
        self.m.set('test1', ['yyy'], '')
        for i in range(2):
            self.assertEqual('', self.m.get('test1', ['yyy']))
    
    def test_getsubtree_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","aa"]=12.34
        test1[1,"y","ab"]=23.45
        test1[1,"y","ab",2,3]=999
        test1[1,"y","ad"]=""        
        test1[1,"y","hello world"]="ok"
        test1[1,"z"]=""
        test1[1,"z","hello world"]="not ok"

        Client: GETSUBTREE myArray[1,"y"]
        Server: *12
        Server: $-1
        Server: $5
        Server: world
        Server: $4
        Server: "aa"
        Server: $5
        Server: 12.34
        Server: $4
        Server: "ab"
        Server: $5
        Server: 23.45
        Server: $8
        Server: "ab",2,3
        Server: $3
        Server: 999
        Server: $4
        Server: "ad"
        Server: $-1
        Server: $13
        Server: "hello world"
        Server: $2
        Server: ok
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'aa'], '12.34')
        self.m.set('test1', [1, 'y', 'ab'], '23.45')
        self.m.set('test1', [1, 'y', 'ab', 2, 3], '999')
        self.m.set('test1', [1, 'y', 'ad'], '')
        self.m.set('test1', [1, 'z'], '')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
    
        for i in range(2):
            data = self.m.getsubtree('test1', [1, 'y'])

            self.assertEqual(data[0][0], None)
            self.assertEqual(data[0][1], 'world')
            
            self.assertEqual(data[1][0], '"aa"')
            self.assertEqual(data[1][1], '12.34')
            
            self.assertEqual(data[2][0], '"ab"')
            self.assertEqual(data[2][1], '23.45')
            
            self.assertEqual(data[3][0], '"ab",2,3')
            self.assertEqual(data[3][1], '999')
            
            self.assertEqual(data[4][0], '"ad"')
            self.assertEqual(data[4][1], '')

    def test_halt_01(self):
        """
        Client: HALT
        """
        
        self.assertEqual(True, self.m.ping())
        self.assertEqual(True, self.m.halt())
        self.assertEqual(False, self.m.ping())
        
        self.setUp()
        
        self.assertEqual(True, self.m.ping())

    def test_increment_01(self):
        """
        Client: INC test1["a","b"]
        Server: 2
        """
        
        self.m.kill('test1', [])

        for i in range(20):
            self.assertEqual(i + 1, self.m.increment('test1', ['a', 'b']))

    def test_kill_01(self):
        """
        Client: KILL test1["a","b"]
        Server: +OK
        """
        
        self.m.set('test1', ['a', 'b'], '')
        for i in range(2):
            self.assertEqual(True, self.m.kill('test1', ['a', 'b']))
        
        self.m.set('test1', ['a', 'b'], '')
        for i in range(2):
            self.assertEqual(True, self.m.kill('test1', []))
            
    def test_next_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        Client: NEXT test1[""]
        Server: $1
        Server: 1
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual('1', self.m.next('test1', ['']))

    def test_next_02(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        Client: NEXT myArray[1]
        Server: $-1
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual(None, self.m.next('test1', [1]))
    
    def test_next_03(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        Client: NEXT myArray[1,""]
        Server: $1
        Server: x
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual('x', self.m.next('test1', [1,'']))

    def test_next_04(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        Client: NEXT myArray[1,"x"]
        Server: $1
        Server: y
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual('y', self.m.next('test1', [1,'x']))

    def test_next_05(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        Client: NEXT myArray[1,"z"]
        Server: $-1
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual(None, self.m.next('test1', [1,'z']))

    def test_previous_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z","hello world"]="not ok"

        Client: GETALLSUBS myArray
        Server: *2
        Server: $1
        Server: 1
        Server: $-1

        Client: GETALLSUBS myArray[1]
        Server: *6
        Server: $1
        Server: x
        Server: $5
        Server: hello
        Server: $1
        Server: y
        Server: $5
        Server: world
        Server: $1
        Server: z
        Server: $0
        Server: 
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z'], '')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            data = self.m.getallsubs('test1', [])
            self.assertEqual(data[0][0], '1')
            self.assertEqual(data[0][1], None)
        
        for i in range(2):
            data = self.m.getallsubs('test1', [1])
            self.assertEqual(data[0][0], 'x')
            self.assertEqual(data[0][1], 'hello')
            self.assertEqual(data[1][0], 'y')
            self.assertEqual(data[1][1], 'world')
            self.assertEqual(data[2][0], 'z')
            self.assertEqual(data[2][1], '')

    def test_ping_01(self):
        m = self.m
        for i in range(2):
            self.assertEqual(True, m.ping())
            
    def test_query_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z"]=""
        test1[1,"z","hello world"]="not ok"

        Client: QUERY test1
        Server: $14
        Server: test1[1,"x"]

        Client: QUERY test1[1,"x"]
        Server: $14
        Server: test1[1,"y"]

        Client: QUERY test1[1,"y"]
        Server: $28
        Server: test1[1,"y","hello world"]

        Client: QUERY test1[1,"z","hello world"]
        Server: $-1

        Client: QUERY test1[1,"a"]
        Server: $14
        Server: test1[1,"x"]
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z'], '')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
        
        for i in range(2):
            self.assertEqual('test1[1,"x"]', self.m.query('test1', []))
        
        for i in range(2):
            self.assertEqual('test1[1,"y"]', self.m.query('test1', [1, 'x']))
        
        for i in range(2):
            self.assertEqual('test1[1,"y","hello world"]', self.m.query('test1', [1, 'y']))
        
        for i in range(2):
            self.assertEqual(None, self.m.query('test1', [1, 'z', 'hello world']))
        
        for i in range(2):
            self.assertEqual('test1[1,"x"]', self.m.query('test1', [1, 'a']))
    
    def test_queryget_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z"]=""
        test1[1,"z","hello world"]="not ok"

        Client: QUERYGET test1
        Server: *2
        Server: $14
        Server: test1[1,"x"]
        Server: $5
        Server: hello

        Client: QUERYGET test1[1,"x"]
        Server: *2
        Server: $14
        Server: test1[1,"y"]
        Server: $5
        Server: world

        Client: QUERYGET test1[1,"y"]
        Server: *2
        Server: $28
        Server: test1[1,"y","hello world"]
        Server: $2
        Server: ok

        Client: QUERYGET test1[1,"z","hello world"]
        Server: $-1

        Client: QUERYGET test1[1,"a"]
        Server: *2
        Server: $14
        Server: test1[1,"x"]
        Server: $5
        Server: hello
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z'], '')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
    
        for i in range(2):
            data = self.m.queryget('test1', [])
            self.assertEqual('test1[1,"x"]', data[0])
            self.assertEqual('hello', data[1])
        
        for i in range(2):
            data = self.m.queryget('test1', [1, 'x'])
            self.assertEqual('test1[1,"y"]', data[0])
            self.assertEqual('world', data[1])
        
        for i in range(2):
            data = self.m.queryget('test1', [1, 'y'])
            self.assertEqual('test1[1,"y","hello world"]', data[0])
            self.assertEqual('ok', data[1])
    
        for i in range(2):
            data = self.m.queryget('test1', [1, 'z', 'hello world'])
            self.assertEqual(None, data[0])
            self.assertEqual(None, data[1])
    
        for i in range(2):
            data = self.m.queryget('test1', [1, 'a'])
            self.assertEqual('test1[1,"x"]', data[0])
            self.assertEqual('hello', data[1])

    def test_set_01_MORE_TO_BE_DONE(self):
        m = self.m
        for i in range(2):
            self.assertEqual(True, m.set('KLB', ['test_set', i, 'b'], 'Kurt Le Breton'))
    
    @unittest.skip("error in protocol needing to be addressed by mgateway.com")
    def test_setsubtree_01(self):
        """
        test1="aaa"
        test1[1,"x"]="hello"
        test1[1,"y"]="world"
        test1[1,"y","hello world"]="ok"
        test1[1,"z"]=""
        test1[1,"z","hello world"]="not ok"

        Client: SETSUBTREE myArray
        Client: *4
        Client: $4
        Client: "aa"
        Client: $5
        Client: 12.34
        Client: $4
        Client: "ab"
        Client: $5
        Client: 23.45
        Server: +OK
        """
        
        self.m.kill('test1', [])
        
        self.m.set('test1', [], 'aaa')
        self.m.set('test1', [1, 'x'], 'hello')
        self.m.set('test1', [1, 'y'], 'world')
        self.m.set('test1', [1, 'y', 'hello world'], 'ok')
        self.m.set('test1', [1, 'z'], '')
        self.m.set('test1', [1, 'z', 'hello world'], 'not ok')
    
        for i in range(2):
            data = [['aa', 'A12.34'], ['ab', 'A23.45']]
            self.assertEqual(True, self.m.setsubtree('test1', [1], data))
            data = self.m.queryget('test1', [1, 'z', 'hello world'])
            self.assertEqual(data[0], 'aa')
            self.assertEqual(data[1], '12.34')
            data = self.m.queryget('test1', ['aa'])
            self.assertEqual(data[0], 'ab')
            self.assertEqual(data[1], '23.45')

    @unittest.skip("todo")
    def test_lock_01(self):
        pass

    @unittest.skip("todo")
    def test_unlock_01(self):
        pass

    @unittest.skip("todo")
    def test_transaction_start_01(self):
        pass

    @unittest.skip("todo")
    def test_transaction_commit_01(self):
        pass

    @unittest.skip("todo")
    def test_transaction_rollback_01(self):
        pass

    @unittest.skip("todo")
    def test_version_01(self):
        pass

    @unittest.skip("todo")
    def test_mversion_01(self):
        pass

    @unittest.skip("todo")
    def test_monitor_01(self):
        pass

    @unittest.skip("todo")
    def test_mdate_01(self):
        pass

    @unittest.skip("todo")
    def test_function_01(self):
        pass

    @unittest.skip("todo")
    def test_processid_01(self):
        pass
        
if __name__ == '__main__':
    unittest.main()