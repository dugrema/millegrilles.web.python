import asyncio
import base64
import datetime
import json
import logging
import secrets
import socketio

from aiohttp import web
from asyncio import TaskGroup
from typing import Optional, Union
from certvalidator.errors import PathValidationError
from cryptography.exceptions import InvalidSignature

from millegrilles_messages.bus.BusContext import ForceTerminateExecution
from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_web import Constantes as ConstantesWeb
from millegrilles_web.SocketIoSubscriptions import SocketIoSubscriptions
from millegrilles_web.WebAppManager import WebAppManager


class SocketIoHandler:

    def __init__(self, manager: WebAppManager, subscription_handler: SocketIoSubscriptions, always_connect=False):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._manager: WebAppManager = manager
        self._subscription_handler = subscription_handler

        self._sio = socketio.AsyncServer(async_mode='aiohttp', always_connect=always_connect, cors_allowed_origins='*')
        self._certificats_maitredescles = dict()

        self._semaphore_requetes = asyncio.BoundedSemaphore(value=10)
        self._semaphore_commandes = asyncio.BoundedSemaphore(value=5)
        self._semaphore_subscriptions = asyncio.BoundedSemaphore(value=3)
        self._semaphore_auth = asyncio.BoundedSemaphore(value=3)

    @property
    def sio(self) -> socketio.AsyncServer:
        return self._sio

    async def setup(self, web_app: web.Application):
        # Wire socketio server in subscriptions handler
        self._subscription_handler.sio = self._sio

        application_path = self._manager.application_path
        socketio_path = f'{application_path[1:]}/socket.io'
        self._sio.attach(web_app, socketio_path=socketio_path)
        await self._preparer_socketio_events()

    async def _preparer_socketio_events(self):
        self._sio.on('connect', handler=self.connect)
        self._sio.on('disconnect', handler=self.disconnect)

        self._sio.on('genererChallengeCertificat', handler=self.generer_challenge_certificat)
        self._sio.on('getCertificatsMaitredescles', handler=self.get_certificats_maitredescles)
        self._sio.on('getInfoIdmg', handler=self.get_info_idmg)
        self._sio.on('getEtatAuth', handler=self.get_info_idmg)
        self._sio.on('getInfoFilehost', handler=self.get_info_filehost)

        # Options 2.prive - pour usager authentifie
        self._sio.on('upgrade', handler=self.upgrade)

        # Obsolete
        self._sio.on('upgradeProtege', handler=self.not_implemented_handler)
        self._sio.on('downgradePrive', handler=self.not_implemented_handler)

    async def run(self):
        try:
            async with TaskGroup() as group:
                group.create_task(self.__initial_keymaster_load())
                group.create_task(self.__stop_thread())
        except* ForceTerminateExecution:
            pass  # Ok

    async def __stop_thread(self):
        await self._manager.context.wait()
        await self._sio.shutdown()
        raise ForceTerminateExecution()

    async def __initial_keymaster_load(self):
        while self._manager.context.stopping is False:

            # Charger le certificat de maitre des cles
            if len(self._certificats_maitredescles) == 0:
                self.__logger.debug("Tenter de charger au moins un certificat de maitre des cles")
                try:
                    await self.charger_maitredescles()
                    return  # Done
                except Exception:
                    self.__logger.exception('Erreur chargement certificat de maitre des cles')

            await self._manager.context.wait(15)

    async def charger_maitredescles(self):
        try:
            producer = await self._manager.context.get_producer()
        except asyncio.TimeoutError:
            # MQ not available, abort
            return

        response = await producer.request(
            dict(), domain=Constantes.DOMAINE_MAITRE_DES_CLES, action='certMaitreDesCles', exchange=Constantes.SECURITE_PUBLIC)
        await self._manager.update_keymaster_certificate(response)

    async def authentifier_message(self, session: dict, message: dict,
                                   enveloppe: Optional[EnveloppeCertificat] = None) -> EnveloppeCertificat:

        if enveloppe is None:
            # Valider le message avant de le transmettre
            enveloppe = await self._manager.context.validateur_message.verifier(message)
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
            await self._subscription_handler.disconnect(sid)

    async def not_implemented_handler(self, sid: str, environ: dict):
        raise NotImplementedError('not implemented')

    async def upgrade(self, sid: str, message: dict):
        async with self._semaphore_auth:
            contenu = json.loads(message['contenu'])

            # Valider message (params)
            try:
                enveloppe = await self._manager.context.validateur_message.verifier(message, verifier_certificat=True)
            except (PathValidationError, InvalidSignature):
                self.__logger.warning("upgrade Erreur certificat ou signature pour SID %s" % sid)
                return self._manager.context.formatteur.signer_message(
                    Constantes.KIND_REPONSE, {'ok': False, 'err': 'Certificat ou signature message invalides'})[0]
            nom_usager = enveloppe.subject_common_name
            user_id = enveloppe.get_user_id

            # Associer toutes les connexions d'un meme usager a un room. Permet de faire un evict.
            await self._sio.enter_room(sid, 'user.%s' % user_id)

            if not user_id or not nom_usager or Constantes.ROLE_USAGER not in enveloppe.get_roles:
                self.__logger.warning("upgrade Le certificat utilise (%s) n'a pas de nom_usager/user_id - REFUSE" % enveloppe.subject_common_name)
                return self._manager.context.formatteur.signer_message(
                    Constantes.KIND_REPONSE, {'ok': False, 'err': "Le certificat utilise n'a pas de nomUsager/userId"})[0]

            # Comparer contenu a l'information dans la session
            async with self._sio.session(sid) as session:
                if session.get(ConstantesWeb.SESSION_REQUEST_AUTH) != '1':
                    return self._manager.context.formatteur.signer_message(
                        Constantes.KIND_REPONSE, {'ok': False, 'err': "Session non autorisee via param request X-Auth"})[0]

                user_name_session = session.get(ConstantesWeb.SESSION_USER_NAME)
                user_id_session = session.get(ConstantesWeb.SESSION_USER_ID)
                challenge = session.get(ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT)

                if user_name_session is None or user_id_session is None or challenge is None:
                    self.__logger.warning("upgrade Session ou challenge non initialise pour SID %s - REFUSE" % sid)
                    return self._manager.context.formatteur.signer_message(
                        Constantes.KIND_REPONSE, {'ok': False, 'err': 'Session ou challenge non initialise'})[0]

                if user_name_session != nom_usager or user_id_session != user_id:
                    self.__logger.warning("upgrade Mismatch userid/username entre session et certificat pour SID %s - REFUSE" % sid)
                    return self._manager.context.formatteur.signer_message(
                        Constantes.KIND_REPONSE, {'ok': False, 'err': 'Mismatch userid/username entre session et certificat'})[0]

                if challenge['data'] == contenu['data'] and challenge['date'] == contenu['date']:
                    # Retirer le challenge pour eviter reutilisation
                    session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = None
                else:
                    self.__logger.warning("upgrade Mismatch date ou challenge pour SID %s - REFUSE" % sid)
                    return self._manager.context.formatteur.signer_message(
                        Constantes.KIND_REPONSE, {'ok': False, 'err': 'Session ou challenge non initialise'})[0]

                session[ConstantesWeb.SESSION_AUTH_VERIFIE] = True

            self.__logger.debug("upgrade Authentification reussie, upgrade events")

            return self._manager.context.formatteur.signer_message(
                Constantes.KIND_REPONSE, {'ok': True, 'protege': True, 'userName': user_name_session})[0]

    async def subscribe(self, sid: str, message: dict, routing_keys: Union[str, list[str]],
                        exchanges: Union[str, list[str]], enveloppe=None, session_requise=True, user_id: Optional[str] = None):
        async with self._semaphore_subscriptions:
            if session_requise is True:
                async with self._sio.session(sid) as session:
                    if session.get(ConstantesWeb.SESSION_REQUEST_AUTH) != '1':
                        return self._manager.context.formatteur.signer_message(
                            Constantes.KIND_REPONSE, {'ok': False, 'err': "Session non autorisee via param request X-Auth"})[0]

            if user_id is None:
                if enveloppe is not False:
                    async with self._sio.session(sid) as session:
                        try:
                            enveloppe = await self.authentifier_message(session, message, enveloppe)
                            user_id = enveloppe.get_user_id
                        except ErreurAuthentificationMessage as e:
                            return self._manager.context.formatteur.signer_message(Constantes.KIND_REPONSE, {'ok': False, 'err': str(e)})[0]
                else:
                    user_id = None

            try:
                raise NotImplementedError('todo')
                # return await self._subscription_handler.subscribe(sid, user_id, routing_keys, exchanges)
            except Exception:
                self.__logger.exception("subscribe Erreur subscribe")
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
                raise NotImplementedError('todo')
                # return await self._subscription_handler.unsubscribe(sid, user_id, routing_keys, exchanges)
            except Exception:
                self.__logger.exception("subscribe Erreur unsubscribe")
                return {'ok': False}

    async def generer_challenge_certificat(self, sid: str):
        async with self._semaphore_requetes:
            challenge_secret = base64.b64encode(secrets.token_bytes(32)).decode('utf-8').replace('=', '')
            challenge = {
                'date': int(datetime.datetime.now().timestamp()),
                'data': challenge_secret
            }
            async with self._sio.session(sid) as session:
                session[ConstantesWeb.SESSION_CHALLENGE_CERTIFICAT] = challenge
            return {'challengeCertificat': challenge}

    async def get_certificats_maitredescles(self, sid: str):
        async with self._semaphore_requetes:
            pems = [p['pem'] for p in self._certificats_maitredescles.values()]
            return pems

    async def get_info_idmg(self, sid: str, params: dict):
        async with self._semaphore_requetes:
            idmg = self._manager.context.signing_key.enveloppe.idmg

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

    async def get_info_filehost(self, sid: str, params: dict):
        async with self._semaphore_requetes:
            async with self._sio.session(sid) as session:
                if session.get('auth_verifie'):
                    filehost_info = self._manager.context.filehost
                    reponse = {'ok': True, 'filehost': filehost_info.to_dict()}
                else:
                    reponse = {'ok': False, 'err': 'Not authenticated'}

            reponse_signee, message_id = self._manager.context.formatteur.signer_message(Constantes.KIND_REPONSE, reponse)
            return reponse_signee

    async def executer_requete(self, sid: str, requete: dict, domaine: str, action: str, exchange: Optional[str] = None, producer=None, enveloppe=None, stream=False):
        async with self._semaphore_requetes:
            if stream is True:
                if producer is None:
                    producer = await asyncio.wait_for(self._manager.context.get_producer(), timeout=0.5)

                # Enregistrer une correlation streaming
                correlation_id = requete['id']

                async def callback(cb_correlation_id: str, message):
                    message_parsed = message.original
                    await self._sio.emit(f'stream_{cb_correlation_id}', message_parsed)

                self.__logger.info("Stream to correlation_id %s", correlation_id)
                raise NotImplementedError('todo')
                # await producer.ajouter_correlation_callback(correlation_id, callback)

                # Emettre la commande nowait - la reponse va etre acheminee via correlation
                # return await self.__executer_message('requete', sid, requete, domaine, action, exchange, producer, enveloppe, nowait=True)
            else:
                return await self.__executer_message('requete', sid, requete, domaine, action, exchange, producer, enveloppe)

    async def executer_commande(self, sid: str, commande: dict, domaine: str, action: str, exchange: Optional[str] = None, producer=None, enveloppe=None, nowait=False, stream=False):
        async with self._semaphore_commandes:
            if stream is True:
                if producer is None:
                    producer = await asyncio.wait_for(self._manager.context.get_producer(), timeout=0.5)

                # Enregistrer une correlation streaming
                correlation_id = commande['id']

                async def callback(cb_correlation_id: str, message):
                    message_parsed = message.original
                    await self._sio.emit(f'stream_{cb_correlation_id}', message_parsed)

                raise NotImplementedError('todo')
                # await producer.ajouter_correlation_callback(correlation_id, callback)

                # Emettre la commande nowait - la reponse va etre acheminee via correlation
                #return await self.__executer_message('commande', sid, commande, domaine, action, exchange, producer, enveloppe, nowait=True)
            else:
                return await self.__executer_message('commande', sid, commande, domaine, action, exchange, producer, enveloppe, nowait=nowait)

    async def __executer_message(self, type_message: str, sid: str, message: dict, domaine_verif: str, action_verif: str,
                                 exchange: str, producer=None, enveloppe=None, nowait=False):
        async with self._sio.session(sid) as session:
            try:
                enveloppe = await self.authentifier_message(session, message, enveloppe)
                # Note - le certificat et la signature du message ont ete verifies. L'autorisation est laissee a l'appeleur.
            except ErreurAuthentificationMessage as e:
                return self._manager.context.formatteur.signer_message(Constantes.KIND_REPONSE, {'ok': False, 'err': str(e)})[0]

        if producer is None:
            producer = await asyncio.wait_for(self._manager.context.get_producer(), timeout=0.5)

        routage = message['routage']
        action = routage['action']
        domaine = routage['domaine']
        partition = routage.get('partition')

        if action != action_verif or domaine != domaine_verif:
            return self._manager.context.formatteur.signer_message(
                Constantes.KIND_REPONSE,
                {'ok': False, 'err': 'Routage mismatch domaine: %s, action: %s (doit etre domaine %s action %s)' % (domaine, action, domaine_verif, action_verif)}
            )[0]

        if type_message == 'requete':
            reponse = await producer.request(
                message, domain=domaine, action=action, partition=partition, exchange=exchange, noformat=True)
        elif type_message == 'commande':
            reponse = await producer.command(
                message, domain=domaine, action=action, partition=partition, exchange=exchange, noformat=True, nowait=nowait)
        else:
            raise ValueError('Type de message non supporte : %s' % type_message)

        try:
            return reponse.original
        except AttributeError:
            return None

    # async def ajouter_sid_room(self, sid: str, room: str):
    #     self.__logger.debug("Ajout sid %s a room %s" % (sid, room))
    #     return await self._sio.enter_room(sid, room)
    #
    # async def retirer_sid_room(self, sid: str, room: str):
    #     self.__logger.debug("Retrait sid %s de room %s" % (sid, room))
    #     return await self._sio.leave_room(sid, room)
    #
    # async def emettre_message_room(self, event: str, data: dict, room: str):
    #     return await self._sio.emit(event, data, room=room)
    #
    # @property
    # def rooms(self):
    #     # return self._sio.manager.rooms
    #     return self._sio.manager.rooms
    #
    # def get_participants(self, room: str):
    #     return self._sio.manager.get_participants('/', room)


class ErreurAuthentificationMessage(Exception):
    pass
