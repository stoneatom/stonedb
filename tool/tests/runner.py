import unittest
import sys

testmodules = [
#    'basictests',
    'regressions',
#    'updatetests',
    'issue332',
    'issue349',
    'issue356',
]

suite = unittest.TestSuite()
for t in testmodules:
    try:
        mod = __import__(t, globals(), locals(), ['suite'])
        suitefn = getattr(mod, 'suite')
        suite.addTest(suitefn())
    except (ImportError, AttributeError):
        # else, just load all the test cases from the module.
        suite.addTest(unittest.defaultTestLoader.loadTestsFromName(t))

ret = not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
sys.exit(ret)
