from ..fetch_data import get_token, get_credentials
import unittest
import os


class TestFetchData(unittest.TestCase):

    def test_get_credentials(self):
        username, pw = get_credentials(path=f"{os.getcwd()}/login.txt")
        print(f"USERNAME: {username}")
        print(f"PW: {pw}")
        self.assertTrue(username)
        self.assertTrue(pw)
        self.assertEqual(type(username), str)
        self.assertEqual(type(pw), str)

    def test_get_token(self):
        username, pw = get_credentials(path=f"{os.getcwd()}/login.txt")
        token = get_token(username=username, pw=pw)
        self.assertTrue(all(x in token.keys()
                            for x in ['access_token', 'token_type', 'expires_in', 'refresh_token']))





