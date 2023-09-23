import base64
import datetime
import json
import logging
import secrets
import socketio

from millegrilles_messages.messages import Constantes

from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web import Constantes as ConstantesWeb


class SocketIoHandler:

    def __init__(self, server, always_connect=False):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._server = server
        self._sio = socketio.AsyncServer(async_mode='aiohttp', always_connect=always_connect)

    @property
    def etat(self) -> EtatWeb:
        return self._server.etat

    async def setup(self):
        self._sio.attach(self._server.app, socketio_path=f'{self._server.get_nom_app()}/socket.io')
        await self._preparer_socketio_events()

    async def _preparer_socketio_events(self):
        self._sio.on('connect', handler=self.connect)
        self._sio.on('disconnect', handler=self.disconnect)

        self._sio.on('unsubscribe', handler=self.unsubscribe)
        self._sio.on('genererChallengeCertificat', handler=self.generer_challenge_certificat)
        self._sio.on('getCertificatsMaitredescles', handler=self.get_certificats_maitredescles)
        self._sio.on('getInfoIdmg', handler=self.get_info_idmg)

        # Options 2.prive - pour usager authentifie  # TODO Ajouter verif authentification "if(authScore > 0)"
        self._sio.on('upgrade', handler=self.upgrade)

        # Obsolete
        self._sio.on('upgradeProtege', handler=self.not_implemented_handler)
        self._sio.on('downgradePrive', handler=self.not_implemented_handler)

    async def _upgrade_socketio_events(self):
        pass

    async def connect(self, sid: str, environ: dict):
        self.__logger.debug("connect %s", sid)
        try:
            request = environ.get('aiohttp.request')
            user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
            user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
        except KeyError:
            self.__logger.error("sio_connect SID:%s sans parametres request user_id/user_name (pas de session)" % sid)
            raise ConnectionRefusedError('authentication failed')

        async with self._sio.session(sid) as session:
            session[ConstantesWeb.SESSION_USER_NAME] = user_name
            session[ConstantesWeb.SESSION_USER_ID] = user_id

        return True

    async def disconnect(self, sid: str):
        self.__logger.debug("disconnect %s", sid)

    async def not_implemented_handler(self, sid: str, environ: dict):
        raise NotImplementedError('not implemented')

    async def upgrade(self, sid: str, message: dict):
        #   const nomUsager = params.nomUsager,
        #         fingerprintPk = params.fingerprintPk,
        #         fingerprintCourant = params.fingerprintCourant,
        #         // hostname = params.hostname,
        #         genererChallenge = params.genererChallenge || false

        contenu = json.loads(message['contenu'])

        # Valider message (params)
        enveloppe = await self.etat.validateur_message.verifier(message, verifier_certificat=True)
        nom_usager = enveloppe.subject_common_name
        user_id = enveloppe.get_user_id

        if not user_id or not nom_usager or Constantes.ROLE_USAGER not in enveloppe.get_roles:
            self.__logger.warning("upgrade Le certificat utilise (%s) n'a pas de nom_usager/user_id - REFUSE" % enveloppe.subject_common_name)
            return {'ok': False, 'err': "Le certificat utilise n'a pas de nomUsager/userId"}

        # Comparer contenu a l'information dans la session
        async with self._sio.session(sid) as session:
            user_name_session = session.get(ConstantesWeb.SESSION_USER_NAME)
            user_id_session = session.get(ConstantesWeb.SESSION_USER_ID)
            challenge = session.get(ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT)

            if user_name_session is None or user_id_session is None or challenge is None:
                self.__logger.warning("upgrade Session ou challenge non initialise pour SID %s - REFUSE" % sid)
                return {'ok': False, 'err': 'Session ou challenge non initialise'}

            if user_name_session != nom_usager or user_id_session != user_id:
                self.__logger.warning("upgrade Mismatch userid/username entre session et certificat pour SID %s - REFUSE" % sid)
                return {'ok': False, 'err': 'Mismatch userid/username entre session et certificat'}

            if challenge['data'] == contenu['data'] and challenge['date'] == contenu['date']:
                # Retirer le challenge pour eviter reutilisation
                session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = None
            else:
                self.__logger.warning("upgrade Mismatch date ou challenge pour SID %s - REFUSE" % sid)
                return {'ok': False, 'err': 'Session ou challenge non initialise'}

        self.__logger.debug("upgrade Authentification reussie, upgrade events")
        await self._upgrade_socketio_events()

        return {'protege': True, 'userName': user_name_session}

    async def unsubscribe(self, sid: str, environ: dict):
        raise NotImplementedError('todo')

    async def generer_challenge_certificat(self, sid: str):
        challenge_secret = base64.b64encode(secrets.token_bytes(32)).decode('utf-8').replace('=', '')
        challenge = {
            'date': int(datetime.datetime.utcnow().timestamp()),
            'data': challenge_secret
        }
        async with self._sio.session(sid) as session:
            session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = challenge
        return {'challengeCertificat': challenge}

    async def get_certificats_maitredescles(self, sid: str, environ: dict):
        raise NotImplementedError('todo')

    async def get_info_idmg(self, sid: str, params: dict):
        async with self._sio.session(sid) as session:
            user_name = session[ConstantesWeb.SESSION_USER_NAME]
            user_id = session[ConstantesWeb.SESSION_USER_ID]

        idmg = self.etat.clecertificat.enveloppe.idmg

        reponse = {
            'idmg': idmg,
            'nomUsager': user_name,
            'userId': user_id,
        }

        return reponse
