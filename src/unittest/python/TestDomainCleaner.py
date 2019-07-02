from src.main.python.DomainCleaner import domain_cleaner
import unittest

class TestDomainCleaner(unittest.TestCase):
    """
    Our basic test class
    """

    def test_domain_cleaner(self):
        """
        The actual test.
        Any method which starts with ``test_`` will considered as a test case.
        """
        res = domain_cleaner('com.scan.booster.memory')
        self.assertEqual(res,'scan.booster.memory')
        res = domain_cleaner('com.apalon.myclockfree')
        self.assertEqual(res,'apalon.myclockfree')
        res = domain_cleaner("com.https://www.'\x00\x11Hello'flash.comlight.brightest.BEACON'\x00\x11Hello'conQUESO437364525289.torch.comr")
        self.assertEqual(res,'helloflash.comlight.brightest.beaconhelloconqueso437364525289.torch.comr')
        res = domain_cleaner("com.com.otracoasa.com.helloflash.com/light.brightest/.beaconhelloconqueso437364525289.torch.comr")
        self.assertEqual(res,'helloflash.com')
        res = domain_cleaner("1564646165")
        self.assertEqual(res,'1564646165')
        res = domain_cleaner('498464684')
        self.assertEqual(res,'498464684')
        res = domain_cleaner("46\x01string87167681\x01string6")
        self.assertEqual(res,'46string87167681string6')
        res = domain_cleaner('www.mainDomain111/domain2/domain3.com')
        self.assertEqual(res,'maindomain111')
        res = domain_cleaner("com.com.otracoasa.com.https://www.'\x00\x11Hello'flash.com/light.brightest/.BEACON'\x00\x11Hello'")
        self.assertEqual(res,'helloflash.com')
        res = domain_cleaner("http://hello.domain.com")
        self.assertEqual(res,'domain.com')



if __name__ == '__main__':
    unittest.main()