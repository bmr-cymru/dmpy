import unittest

class DmpyImportTest(unittest.TestCase):
    # attempt to import dmpy
    try:
        import dmpy as dm
    except Exception, e:
        self.Fail(str(e))

