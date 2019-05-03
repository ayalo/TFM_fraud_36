from src.main.python.prueba import parse_domain
import unittest


def test_prueba(self):
    """
    The actual test.
    Any method which starts with ``test_`` will considered as a test case.
    """
    res = parse_domain('com.scan.booster.memory')
    self.assertEqual(res,'scan.booster.memory')
    res = parse_domain('com.apalon.myclockfree')
    self.assertEqual(res,'apalon.myclockfree')
    res = parse_domain("com.https://www.'\x00\x11Hello'flash.comlight.brightest.BEACON'\x00\x11Hello'conQUESO437364525289.torch.comr")
    self.assertEqual(res,'helloflashlight.brightest.beaconhelloconqueso437364525289.torchr')
    res = parse_domain("1564646165")
    self.assertEqual(res,'1564646165')
    res = parse_domain('498464684')
    self.assertEqual(res,'498464684')
    res = parse_domain("46\x01string87167681\x01string6")
    self.assertEqual(res,'46string87167681string6')
    res = parse_domain('www.mainDomain111/domain2/domain3.com')
    self.assertEqual(res,'maindomain111')

if __name__ == '__main__':
    unittest.main()