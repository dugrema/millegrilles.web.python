import datetime
import jwt
import pytz

from typing import Optional

from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


def creer_token_fichier(clecert: CleCertificat,
                        issuer: str,
                        fuuid: str,
                        user_id: str,
                        mimetype: Optional[str] = None,
                        dechiffrage: Optional[dict] = None,
                        expiration: Optional[datetime.datetime] = None) -> str:

    headers = {
        'kid': clecert.enveloppe.fingerprint
    }

    now = datetime.datetime.now(tz=pytz.UTC)
    not_before = now - datetime.timedelta(minutes=2)

    claim = {
        'sub': fuuid,
        'user_id': user_id,
        'iss': issuer,
        'nbf': not_before,
    }

    if mimetype:
        claim['mimetype'] = mimetype

    if dechiffrage:
        # Dechiffrage : { ref, header, iv, nonce, tag, format }
        claim.update(dechiffrage)

    if expiration:
        claim['exp'] = expiration

    private_key = clecert.private_key
    encoded_jwt = jwt.encode(
        claim,
        private_key,
        algorithm='EdDSA',
        headers=headers
    )

    return encoded_jwt


def get_headers(token: str) -> dict:
    # return jwt.decode(token, options={"verify_signature": False})
    return jwt.get_unverified_header(token)


def verify(enveloppe: EnveloppeCertificat, token: str) -> dict:
    public_key = enveloppe.get_public_key()
    return jwt.decode(token, public_key, algorithms=['EdDSA'])
