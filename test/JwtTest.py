import datetime
import unittest
import pytz

from jwt.exceptions import ExpiredSignatureError

from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_web.JwtUtils import creer_token_fichier, get_headers, verify


def get_cle_certificat():
    return CleCertificat.from_files(
        '/var/opt/millegrilles/secrets/pki.core.cle',
        '/var/opt/millegrilles/secrets/pki.core.cert')


class TokensTestCase(unittest.TestCase):
    def test_creer_token(self):

        cle_certificat = get_cle_certificat()

        token = creer_token_fichier(cle_certificat, 'test', 'fuuid_test', 'zUN_USER')
        print('Token cree : %s' % token)
        # self.assertEqual(True, False)  # add assertion here

        # Get headers
        headers = get_headers(token)
        print("Headers : %s" % headers)
        self.assertEqual(headers['kid'], cle_certificat.enveloppe.fingerprint)

        # Verifier
        resultat = verify(cle_certificat.enveloppe, token)
        print("Resultat verification : %s" % resultat)
        self.assertEqual(resultat['sub'], 'fuuid_test')
        self.assertEqual(resultat['user_id'], 'zUN_USER')
        self.assertEqual(resultat['iss'], 'test')

    def test_token_expire(self):
        cle_certificat = get_cle_certificat()

        expiration = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(minutes=1)
        token = creer_token_fichier(cle_certificat, 'test', 'fuuid_test', 'zUN_USER',
                                    expiration=expiration)

        try:
            verify(cle_certificat.enveloppe, token)
            self.fail("Doit lancer expiration")
        except ExpiredSignatureError:
            pass

    def test_info_dechiffrage(self):

        cle_certificat = get_cle_certificat()

        info_dechiffrage = {'ref': 'tata'}

        token = creer_token_fichier(cle_certificat, 'test', 'fuuid_test', 'zUN_USER', dechiffrage=info_dechiffrage)
        print('Token cree : %s' % token)

        # Verifier
        resultat = verify(cle_certificat.enveloppe, token)
        print("Resultat verification : %s" % resultat)
        for key, value in info_dechiffrage.items():
            self.assertEqual(resultat[key], value)


if __name__ == '__main__':
    unittest.main()
