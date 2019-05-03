from src.main.python.IpCleaner import ip_cleaner
import unittest

class TestIpCleaner(unittest.TestCase):
    """
    Our basic test class
    """

    def test_ip_cleaner(self):
        """
        The actual test.
        Any method which starts with ``test_`` will considered as a test case.
        """

        res=ip_cleaner("178.23.5")
        self.assertEqual(res,'Format not valid: 178.23.5')
        res = ip_cleaner( "2001.DB9:1" )
        self.assertEqual(res,'Format not valid: 2001.DB9:1')
        res = ip_cleaner( "2001.DB9:1.23.5" )
        self.assertEqual(res,'Format not valid: 2001.DB9:1.23.5')
        res = ip_cleaner( "10.34.76.23" )
        self.assertEqual(res,'10.34.76.23')
        res = ip_cleaner( "178.23.5.34.7" )
        self.assertEqual(res,'Format not valid: 178.23.5.34.7')
        res = ip_cleaner( "holacaracola" )
        self.assertEqual(res,'Format not valid: holacaracola')

if __name__ == '__main__':
    unittest.main()