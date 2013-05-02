import doctest
import unittest
import anygrate
from os.path import dirname, join, abspath

doctests = ['README.rst']

HERE = dirname(anygrate.__file__)

DOCTEST_FLAGS = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS | \
    doctest.REPORT_ONLY_FIRST_FAILURE | doctest.IGNORE_EXCEPTION_DETAIL


def setUp(test):
    pass


def tearDown(test):
    pass


def load_tests():
    # create a test suite
    suite = unittest.TestSuite()
    startDir = abspath(join(HERE, 'test'))

    # unittest2 discovery
    suite = unittest.TestLoader().discover(startDir, pattern='test*.py', top_level_dir=None)

    # add doctests
    for test in doctests:
        suite.addTest(
            doctest.DocFileSuite(
                join(HERE, test),
                module_relative=False,
                optionflags=DOCTEST_FLAGS,
                setUp=setUp,
                tearDown=tearDown)
        )

    return suite
