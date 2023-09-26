import asyncio
import base64
import datetime
import json
import logging
import secrets
import socketio

from typing import Optional, Union

from certvalidator.errors import PathValidationError
from cryptography.exceptions import InvalidSignature

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat

from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web import Constantes as ConstantesWeb
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_web.SocketIoSubscriptions import SocketIoSubscriptions


class SocketIoHandler:

    def __init__(self, server, stop_event: asyncio.Event, always_connect=False):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._server = server
        self._stop_event = stop_event
        self._sio = socketio.AsyncServer(async_mode='aiohttp', always_connect=always_connect)

        self.__subscription_handler = SocketIoSubscriptions(self, self._stop_event)

        self.__certificats_maitredescles = dict()

    @property
    def etat(self) -> EtatWeb:
        return self._server.etat

    @property
    def subscription_handler(self) -> SocketIoSubscriptions:
        return self.__subscription_handler

    async def setup(self):
        self._sio.attach(self._server.app, socketio_path=f'{self._server.get_nom_app()}/socket.io')
        await self._preparer_socketio_events()

    async def _preparer_socketio_events(self):
        self._sio.on('connect', handler=self.connect)
        self._sio.on('disconnect', handler=self.disconnect)

        # self._sio.on('subscribe', handler=self.subscribe)
        # self._sio.on('unsubscribe', handler=self.unsubscribe)
        self._sio.on('genererChallengeCertificat', handler=self.generer_challenge_certificat)
        self._sio.on('getCertificatsMaitredescles', handler=self.get_certificats_maitredescles)
        self._sio.on('getInfoIdmg', handler=self.get_info_idmg)

        # Options 2.prive - pour usager authentifie  # TODO Ajouter verif authentification "if(authScore > 0)"
        self._sio.on('upgrade', handler=self.upgrade)

        # Obsolete
        self._sio.on('upgradeProtege', handler=self.not_implemented_handler)
        self._sio.on('downgradePrive', handler=self.not_implemented_handler)

    async def run(self):
        await asyncio.gather(
            self.entretien_maitredescles(),
            self.__subscription_handler.run(),
        )

    async def entretien_maitredescles(self):
        while self._stop_event.is_set() is False:

            # Charger le certificat de maitre des cles
            if len(self.__certificats_maitredescles) == 0:
                self.__logger.debug("Tenter de charger au moins un certificat de maitre des cles")
                try:
                    await self.charger_maitredescles()
                except Exception:
                    self.__logger.exception('Erreur chargement certificat de maitre des cles')
            else:
                # Retirer les certificats expires
                expiration = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
                fingerprints_expires = list()
                for key, value in self.__certificats_maitredescles.items():
                    if value['date_reception'] < expiration:
                        fingerprints_expires.append(key)
                for fp in fingerprints_expires:
                    del self.__certificats_maitredescles[fp]

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass  # OK

    async def charger_maitredescles(self):
        try:
            producer = await asyncio.wait_for(self.etat.producer_wait(), timeout=10)
        except asyncio.TimeoutError:
            # MQ non disponible, abort
            return

        requete = dict()
        action = 'certMaitreDesCles'
        domaine = Constantes.DOMAINE_MAITRE_DES_CLES
        reponse = await producer.executer_requete(requete, domaine=domaine, action=action,
                                                  exchange=Constantes.SECURITE_PUBLIC)
        await self.recevoir_certificat_maitredescles(reponse)

    async def recevoir_certificat_maitredescles(self, message: MessageWrapper):
        certificat = message.certificat
        if Constantes.ROLE_MAITRE_DES_CLES in certificat.get_roles:
            fingerprint = certificat.fingerprint
            pem = certificat.chaine_pem()
            self.__certificats_maitredescles[fingerprint] = {
                'pem': pem,
                'date_reception': datetime.datetime.utcnow(),
            }

    async def authentifier_message(self, session: dict, message: dict,
                                   enveloppe: Optional[EnveloppeCertificat]) -> EnveloppeCertificat:

        if enveloppe is None:
            # Valider le message avant de le transmettre
            enveloppe = await self.etat.validateur_message.verifier(message)
        else:
            pass  # OK, assumer que le message a deja ete valide

        if session[ConstantesWeb.SESSION_AUTH_VERIFIE] is not True:
            raise ErreurAuthentificationMessage('Session non authentifiee')
        if enveloppe.get_user_id != session[ConstantesWeb.SESSION_USER_ID]:
            raise ErreurAuthentificationMessage('Mismatch userId')

        return enveloppe

    async def connect(self, sid: str, environ: dict):
        self.__logger.debug("connect %s", sid)

        try:
            request = environ.get('aiohttp.request')

            for k, v in request.headers.items():
                self.__logger.debug("connect HEADER %s = %s" % (k, v))

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
        await self.subscription_handler.disconnect(sid)

    async def not_implemented_handler(self, sid: str, environ: dict):
        raise NotImplementedError('not implemented')

    async def upgrade(self, sid: str, message: dict):
        contenu = json.loads(message['contenu'])

        # Valider message (params)
        try:
            enveloppe = await self.etat.validateur_message.verifier(message, verifier_certificat=True)
        except (PathValidationError, InvalidSignature):
            self.__logger.warning("upgrade Erreur certificat ou signature pour SID %s" % sid)
            return self.etat.formatteur_message.signer_message(
                Constantes.KIND_REPONSE, {'ok': False, 'err': 'Certificat ou signature message invalides'})[0]
        nom_usager = enveloppe.subject_common_name
        user_id = enveloppe.get_user_id

        if not user_id or not nom_usager or Constantes.ROLE_USAGER not in enveloppe.get_roles:
            self.__logger.warning("upgrade Le certificat utilise (%s) n'a pas de nom_usager/user_id - REFUSE" % enveloppe.subject_common_name)
            return self.etat.formatteur_message.signer_message(
                Constantes.KIND_REPONSE, {'ok': False, 'err': "Le certificat utilise n'a pas de nomUsager/userId"})[0]

        # Comparer contenu a l'information dans la session
        async with self._sio.session(sid) as session:
            user_name_session = session.get(ConstantesWeb.SESSION_USER_NAME)
            user_id_session = session.get(ConstantesWeb.SESSION_USER_ID)
            challenge = session.get(ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT)

            if user_name_session is None or user_id_session is None or challenge is None:
                self.__logger.warning("upgrade Session ou challenge non initialise pour SID %s - REFUSE" % sid)
                return self.etat.formatteur_message.signer_message(
                    Constantes.KIND_REPONSE, {'ok': False, 'err': 'Session ou challenge non initialise'})[0]

            if user_name_session != nom_usager or user_id_session != user_id:
                self.__logger.warning("upgrade Mismatch userid/username entre session et certificat pour SID %s - REFUSE" % sid)
                return self.etat.formatteur_message.signer_message(
                    Constantes.KIND_REPONSE, {'ok': False, 'err': 'Mismatch userid/username entre session et certificat'})[0]

            if challenge['data'] == contenu['data'] and challenge['date'] == contenu['date']:
                # Retirer le challenge pour eviter reutilisation
                session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = None
            else:
                self.__logger.warning("upgrade Mismatch date ou challenge pour SID %s - REFUSE" % sid)
                return self.etat.formatteur_message.signer_message(
                    Constantes.KIND_REPONSE, {'ok': False, 'err': 'Session ou challenge non initialise'})[0]

            session[ConstantesWeb.SESSION_AUTH_VERIFIE] = True

        self.__logger.debug("upgrade Authentification reussie, upgrade events")

        return self.etat.formatteur_message.signer_message(
            Constantes.KIND_REPONSE, {'ok': True, 'protege': True, 'userName': user_name_session})[0]

    async def subscribe(self, sid: str, message: dict, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]], enveloppe=None):
        async with self._sio.session(sid) as session:
            try:
                enveloppe = await self.authentifier_message(session, message, enveloppe)
            except ErreurAuthentificationMessage as e:
                return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False, 'err': str(e)})[0]

        try:
            return await self.__subscription_handler.subscribe(sid, routing_keys, exchanges)
        except Exception:
            self.__logger.exception("subscribe Erreur subscribe")
            return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False})[0]

    async def unsubscribe(self, sid: str, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        try:
            await self.__subscription_handler.unsubscribe(sid, routing_keys, exchanges)
        except Exception:
            self.__logger.exception("subscribe Erreur unsubscribe")
            return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False})[0]

        return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': True})[0]

    async def generer_challenge_certificat(self, sid: str):
        challenge_secret = base64.b64encode(secrets.token_bytes(32)).decode('utf-8').replace('=', '')
        challenge = {
            'date': int(datetime.datetime.utcnow().timestamp()),
            'data': challenge_secret
        }
        async with self._sio.session(sid) as session:
            session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = challenge
        return {'challengeCertificat': challenge}

    async def get_certificats_maitredescles(self, sid: str):
        pems = [p['pem'] for p in self.__certificats_maitredescles.values()]
        return pems

    async def get_info_idmg(self, sid: str, params: dict):
        idmg = self.etat.clecertificat.enveloppe.idmg

        reponse = {
            'idmg': idmg,
        }

        async with self._sio.session(sid) as session:
            try:
                reponse['nomUsager'] = session[ConstantesWeb.SESSION_USER_NAME]
                reponse['userId'] = session[ConstantesWeb.SESSION_USER_ID]
            except KeyError:
                pass  # OK

        return reponse

    @property
    def exchange_default(self):
        raise NotImplementedError('must implement')

    async def executer_requete(self, sid: str, requete: dict, domaine: str, action: str, exchange: Optional[str] = None, producer=None, enveloppe=None):
        return await self.__executer_message('requete', sid, requete, domaine, action, exchange, producer, enveloppe)

    async def executer_commande(self, sid: str, commande: dict, domaine: str, action: str, exchange: Optional[str] = None, producer=None, enveloppe=None):
        return await self.__executer_message('commande', sid, commande, domaine, action, exchange, producer, enveloppe)

    async def __executer_message(self, type_message: str, sid: str, message: dict, domaine_verif: str, action_verif: str, exchange: Optional[str] = None,
                                 producer=None, enveloppe=None):
        """

        :param type_message:
        :param sid:
        :param message:
        :param domaine: Domaine du message - utilise pour verifier le routage
        :param action: Action du message - utilise pour verifier le routage
        :param exchange:
        :param producer:
        :param enveloppe:
        :return:
        """

        async with self._sio.session(sid) as session:
            try:
                enveloppe = await self.authentifier_message(session, message, enveloppe)
            except ErreurAuthentificationMessage as e:
                return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False, 'err': str(e)})[0]

        if exchange is None:
            exchange = self.exchange_default

        if producer is None:
            producer = await asyncio.wait_for(self.etat.producer_wait(), timeout=0.5)

        routage = message['routage']
        action = routage['action']
        domaine = routage['domaine']
        partition = routage.get('partition')

        if action != action_verif or domaine != domaine_verif:
            return self.etat.formatteur_message.signer_message(
                Constantes.KIND_REPONSE,
                {'ok': False, 'err': 'Routage mismatch (doit etre domaine %s action %s)' % (domaine_verif, action_verif)}
            )[0]

        if type_message == 'requete':
            reponse = await producer.executer_requete(message, domaine=domaine, action=action, partition=partition,
                                                      exchange=exchange, noformat=True)
        elif type_message == 'commande':
            reponse = await producer.executer_commande(message, domaine=domaine, action=action, partition=partition,
                                                       exchange=exchange, noformat=True)
        else:
            raise ValueError('Type de message non supporte : %s' % type_message)
        # Note - le certificat et la signature du message ont ete verifies. L'autorisation est laissee a l'appeleur

        parsed = reponse.parsed
        return parsed['__original']

    def ajouter_sid_room(self, sid: str, room: str):
        return self._sio.enter_room(sid, room)

    def retirer_sid_room(self, sid: str, room: str):
        return self._sio.leave_room(sid, room)

    async def emettre_message_room(self, event: str, data: dict, room: str):
        return await self._sio.emit(event, data, room=room)

    @property
    def rooms(self):
        return self._sio.manager.rooms


class ErreurAuthentificationMessage(Exception):
    pass
