from unittest import TestCase
import sys

sys.path.append('..')
from spymark import Spymark
from support import get_fixture

class TestDatabase(TestCase):
    def testNoDuplicateImports(self):
        spy = Spymark(None)
        c = spy.db.cursor()

        spy.import_ofx(get_fixture('fidelity.ofx'))

        c.execute("SELECT COUNT(*) from stocks")
        (rows1,) = c.fetchone()
        self.assertEqual(rows1, 10)

        spy.import_ofx(get_fixture('fidelity.ofx'))

        c.execute("SELECT COUNT(*) from stocks")
        (rows2,) = c.fetchone()
        self.assertEqual(rows1, rows2)
