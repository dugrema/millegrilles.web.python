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
        self._sio = socketio.AsyncServer(async_mode='aiohttp', always_connect=always_connect, cors_allowed_origins='*')

        self.__subscription_handler = SocketIoSubscriptions(self, self._stop_event)

        self.__certificats_maitredescles = dict()

        self._semaphore_requetes = asyncio.BoundedSemaphore(value=10)
        self._semaphore_commandes = asyncio.BoundedSemaphore(value=5)
        self._semaphore_subscriptions = asyncio.BoundedSemaphore(value=3)
        self._semaphore_auth = asyncio.BoundedSemaphore(value=3)

    @property
    def etat(self) -> EtatWeb:
        return self._server.etat

    @property
    def subscription_handler(self) -> SocketIoSubscriptions:
        return self.__subscription_handler

    async def setup(self):
        socketio_path = f'{self._server.get_nom_app()}/socket.io'
        self._sio.attach(self._server.app, socketio_path=socketio_path)
        await self._preparer_socketio_events()

    async def _preparer_socketio_events(self):
        self._sio.on('connect', handler=self.connect)
        self._sio.on('disconnect', handler=self.disconnect)

        # self._sio.on('subscribe', handler=self.subscribe)
        # self._sio.on('unsubscribe', handler=self.unsubscribe)
        self._sio.on('genererChallengeCertificat', handler=self.generer_challenge_certificat)
        self._sio.on('getCertificatsMaitredescles', handler=self.get_certificats_maitredescles)
        self._sio.on('getInfoIdmg', handler=self.get_info_idmg)
        self._sio.on('getEtatAuth', handler=self.get_info_idmg)

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

    async def evict_usager(self, message: MessageWrapper):
        user_id = message.parsed['userId']
        self.__logger.info("evict_usager Expulser usager %s" % user_id)

        # Trouver les sids associes au user_id
        for (sid, _) in self.get_participants('user.%s' % user_id):
            self.__logger.debug("evict_usager %s SID:%s" % (user_id, sid))
            async with self._sio.session(sid) as session:
                session.clear()

            await self._sio.disconnect(sid)

    async def authentifier_message(self, session: dict, message: dict,
                                   enveloppe: Optional[EnveloppeCertificat] = None) -> EnveloppeCertificat:

        if enveloppe is None:
            # Valider le message avant de le transmettre
            enveloppe = await self.etat.validateur_message.verifier(message)
        else:
            pass  # OK, assumer que le message a deja ete valide

        if session.get(ConstantesWeb.SESSION_REQUEST_AUTH) != '1':
            raise ErreurAuthentificationMessage('Session non authentifiee (request)')
        if session.get(ConstantesWeb.SESSION_AUTH_VERIFIE) is not True:
            raise ErreurAuthentificationMessage('Session non authentifiee (upgrade)')
        if enveloppe.get_user_id != session.get(ConstantesWeb.SESSION_USER_ID):
            raise ErreurAuthentificationMessage('Mismatch userId')

        return enveloppe

    async def connect(self, sid: str, environ: dict):
        self.__logger.debug("connect %s", sid)

        async with self._semaphore_auth:
            try:
                request = environ.get('aiohttp.request')

                for k, v in request.headers.items():
                    self.__logger.debug("connect HEADER %s = %s" % (k, v))

                user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
                user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
                auth = request.headers[ConstantesWeb.HEADER_AUTH]

                # Conserver toutes les connexion d'un meme usager dans
                # la meme room. Permet de faire un evict au besoin.
                await self._sio.enter_room(sid, 'user.%s' % user_id)
            except KeyError:
                self.__logger.debug("sio_connect SID:%s sans parametres request user_id/user_name (non authentifie)" % sid)
                # raise ConnectionRefusedError('authentication failed')
                return True

            async with self._sio.session(sid) as session:
                session[ConstantesWeb.SESSION_USER_NAME] = user_name
                session[ConstantesWeb.SESSION_USER_ID] = user_id
                session[ConstantesWeb.SESSION_REQUEST_AUTH] = auth

        return True

    async def disconnect(self, sid: str):
        self.__logger.debug("disconnect %s", sid)
        async with self._semaphore_auth:
            await self.subscription_handler.disconnect(sid)

    async def not_implemented_handler(self, sid: str, environ: dict):
        raise NotImplementedError('not implemented')

    async def upgrade(self, sid: str, message: dict):
        async with self._semaphore_auth:
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

            # Associer toutes les connexions d'un meme usager a un room. Permet de faire un evict.
            await self._sio.enter_room(sid, 'user.%s' % user_id)

            if not user_id or not nom_usager or Constantes.ROLE_USAGER not in enveloppe.get_roles:
                self.__logger.warning("upgrade Le certificat utilise (%s) n'a pas de nom_usager/user_id - REFUSE" % enveloppe.subject_common_name)
                return self.etat.formatteur_message.signer_message(
                    Constantes.KIND_REPONSE, {'ok': False, 'err': "Le certificat utilise n'a pas de nomUsager/userId"})[0]

            # Comparer contenu a l'information dans la session
            async with self._sio.session(sid) as session:
                if session.get(ConstantesWeb.SESSION_REQUEST_AUTH) != '1':
                    return self.etat.formatteur_message.signer_message(
                        Constantes.KIND_REPONSE, {'ok': False, 'err': "Session non autorisee via param request X-Auth"})[0]

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

    async def subscribe(self, sid: str, message: dict, routing_keys: Union[str, list[str]],
                        exchanges: Union[str, list[str]], enveloppe=None, session_requise=True, user_id: Optional[str] = None):
        async with self._semaphore_subscriptions:
            if session_requise is True:
                async with self._sio.session(sid) as session:
                    if session.get(ConstantesWeb.SESSION_REQUEST_AUTH) != '1':
                        return self.etat.formatteur_message.signer_message(
                            Constantes.KIND_REPONSE, {'ok': False, 'err': "Session non autorisee via param request X-Auth"})[0]

            if user_id is None:
                if enveloppe is not False:
                    async with self._sio.session(sid) as session:
                        try:
                            enveloppe = await self.authentifier_message(session, message, enveloppe)
                            user_id = enveloppe.get_user_id
                        except ErreurAuthentificationMessage as e:
                            return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False, 'err': str(e)})[0]
                else:
                    user_id = None

            try:
                return await self.__subscription_handler.subscribe(sid, user_id, routing_keys, exchanges)
                # return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, reponse)[0]
            except Exception:
                self.__logger.exception("subscribe Erreur subscribe")
                # return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False})[0]
                return {'ok': False}

    async def unsubscribe(self, sid: str, message: dict, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]], session_requise=True):
        async with self._semaphore_subscriptions:
            if session_requise is True:
                async with self._sio.session(sid) as session:
                    try:
                        enveloppe = await self.authentifier_message(session, message)
                        user_id = enveloppe.get_user_id
                    except (KeyError, ErreurAuthentificationMessage):
                        user_id = None
            else:
                user_id = None

            try:
                return await self.__subscription_handler.unsubscribe(sid, user_id, routing_keys, exchanges)
                # return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, reponse)[0]
            except Exception:
                self.__logger.exception("subscribe Erreur unsubscribe")
                # return self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, {'ok': False})[0]
                return {'ok': False}

    async def generer_challenge_certificat(self, sid: str):
        async with self._semaphore_requetes:
            challenge_secret = base64.b64encode(secrets.token_bytes(32)).decode('utf-8').replace('=', '')
            challenge = {
                'date': int(datetime.datetime.utcnow().timestamp()),
                'data': challenge_secret
            }
            async with self._sio.session(sid) as session:
                session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = challenge
            return {'challengeCertificat': challenge}

    async def get_certificats_maitredescles(self, sid: str):
        async with self._semaphore_requetes:
            pems = [p['pem'] for p in self.__certificats_maitredescles.values()]
            return pems

    async def get_info_idmg(self, sid: str, params: dict):
        async with self._semaphore_requetes:
            idmg = self.etat.clecertificat.enveloppe.idmg

            reponse = {
                'idmg': idmg,
            }

            async with self._sio.session(sid) as session:
                try:
                    reponse['nomUsager'] = session[ConstantesWeb.SESSION_USER_NAME]
                    reponse['userId'] = session[ConstantesWeb.SESSION_USER_ID]
                    reponse['auth'] = True
                except KeyError:
                    reponse['auth'] = False

            return reponse

    @property
    def exchange_default(self):
        return self.etat.configuration.exchange_default

    async def executer_requete(self, sid: str, requete: dict, domaine: str, action: str, exchange: Optional[str] = None, producer=None, enveloppe=None):
        async with self._semaphore_requetes:
            return await self.__executer_message('requete', sid, requete, domaine, action, exchange, producer, enveloppe)

    async def executer_commande(self, sid: str, commande: dict, domaine: str, action: str, exchange: Optional[str] = None, producer=None, enveloppe=None, nowait=False):
        async with self._semaphore_commandes:
            return await self.__executer_message('commande', sid, commande, domaine, action, exchange, producer, enveloppe, nowait=nowait)

    async def __executer_message(self, type_message: str, sid: str, message: dict, domaine_verif: str, action_verif: str, exchange: Optional[str] = None,
                                 producer=None, enveloppe=None, nowait=False):
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
                {'ok': False, 'err': 'Routage mismatch domaine: %s, action: %s (doit etre domaine %s action %s)' % (domaine, action, domaine_verif, action_verif)}
            )[0]

        if type_message == 'requete':
            reponse = await producer.executer_requete(message, domaine=domaine, action=action, partition=partition,
                                                      exchange=exchange, noformat=True)
        elif type_message == 'commande':
            reponse = await producer.executer_commande(message, domaine=domaine, action=action, partition=partition,
                                                       exchange=exchange, noformat=True, nowait=nowait)
        else:
            raise ValueError('Type de message non supporte : %s' % type_message)
        # Note - le certificat et la signature du message ont ete verifies. L'autorisation est laissee a l'appeleur

        try:
            # parsed = reponse.parsed
            # return parsed['__original']
            return reponse.original
        except AttributeError:
            return None

    async def ajouter_sid_room(self, sid: str, room: str):
        self.__logger.debug("Ajout sid %s a room %s" % (sid, room))
        return await self._sio.enter_room(sid, room)

    async def retirer_sid_room(self, sid: str, room: str):
        self.__logger.debug("Retrait sid %s de room %s" % (sid, room))
        return await self._sio.leave_room(sid, room)

    async def emettre_message_room(self, event: str, data: dict, room: str):
        return await self._sio.emit(event, data, room=room)

    @property
    def rooms(self):
        # return self._sio.manager.rooms
        return self._sio.manager.rooms

    def get_participants(self, room: str):
        return self._sio.manager.get_participants('/', room)

    # async def traiter_message_userid(self, message: MessageWrapper) -> Union[bool, dict]:
    #     self.__logger.warning("traiter_message_userid Message user sans handler, DROPPED %s", message.routage)
    #     return False


class ErreurAuthentificationMessage(Exception):
    pass
