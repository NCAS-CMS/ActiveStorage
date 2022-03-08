#
# Boiler plate code for playing with active storage reads

import pathlib
import struct
import tempfile
import unittest
# from array import array

# FIXME: Add striding examples. We might want to consider
# using numpy mmap at that point.
# FIXME: Add example with missing data
# FIXME: Add other datatypes than 32 bit floats
# FIXME: Add other operations (full set = sum, mean, count, min, max)
# FIXME: Make sure that the byte representation is consistent with
# netcdf byte representations
# nd/or think about what we need to tell active storage about float format.


def raw_write(f, list_of_floats):
    """ Does what it says on the tin, assume f open for binary writing"""
    data = struct.pack('f' * len(list_of_floats), *list_of_floats)
    f.write(data)


def standard_read(f, i, j):
    """
    From open file f, read floats from byte position <i> to byte position <j>.
    """
    f.seek(i, 0)
    n = j - i
    assert n % 4 == 0
    nn = int(n / 4)
    bytes = f.read(n)
    floats = struct.unpack('f' * nn, bytes)
    return list(floats)


def mock_active_read_operation(f, i, n, op='mean'):
    """
    Return <op> applied to <n> floats starting at the <i>th byte of file <f>
    """
    f.seek(i, 0)
    nbytes = n * 4
    bytes = f.read(nbytes)
    floats = struct.unpack('f' * n, bytes)
    print('Mocking active ', op)
    if 'mean':
        return sum(floats) / n
    else:
        raise NotImplementedError


def do_operation(f, i, n, op):
    """
    do an operation on f, and if f is on active storage,
    which needs to be an attribute of f, then use active
    read operations, otherwise do ordinary operations.
    """
    if f.is_active:
        return mock_active_read_operation(f, i, n, op)
    else:
        j = i + 4 * n
        floats = standard_read(f, i, j)
        if op == 'mean':
            mean = sum(floats) / n
            return mean
        else:
            raise NotImplementedError


class TestActive(unittest.TestCase):
    """
    Simple test harness for extension to real active storage
    """

    def setUp(self):
        # deliberately not using numpy
        self.tempdir = tempfile.TemporaryDirectory()
        self.dummydir = pathlib.Path(self.tempdir.name)

        self.dummyfile = self.dummydir / 'dummy.data'
        self.data = [float(i) for i in range(12)]
        # dummydata = array('d', self.data)
        with open(self.dummyfile, 'wb') as f:
            raw_write(f, self.data)

    def test_basic_read(self):
        """ Make sure the data is what we think it is"""
        with open(self.dummyfile, 'rb') as f:
            inputdata = standard_read(f, 0, 48)
        self.assertEqual(self.data, inputdata)

    def test_compare_active(self):
        """
        Test we get the same result with normal active and mocked active.
        Note that the file object (or something) needs to know this
        is an active storage system.
        """
        f = open(self.dummyfile, 'rb')
        f.is_active = False
        ans1 = do_operation(f, 8, 4, 'mean')
        f.close()
        f = open(self.dummyfile, 'rb')
        f.is_active = True
        ans2 = do_operation(f, 8, 4, 'mean')
        f.close()
        self.assertEqual(ans1, ans2)
        print(ans1)

    def tearDown(self):
        # explicitly clean up
        self.tempdir.cleanup()


if __name__ == "__main__":
    unittest.main()
