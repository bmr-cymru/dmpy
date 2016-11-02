import unittest

class DmpyTests(unittest.TestCase):

    def test_import(self):
        # attempt to import dmpy
        try:
            import dmpy as dm
        except Exception as e:
            self.fail(str(e))

