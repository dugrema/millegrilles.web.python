import asyncio
import json
import logging

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.Hachage import hacher
from millegrilles_messages.certificats.Generes import EnveloppeCsr
from millegrilles_web.SocketIoHandler import SocketIoHandler

REQUESTS_DICT = 'requests_dict'
COMMANDS_DICT = 'commands_dict'

AUTHORIZED_WEBAPI_IDMG = ['zeYncRqEqZ6eTEmUZ8whJFuHG796eSvCTWE4M432izXrp22bAtwGm7Jf']


class MappedSocketIoHandler(SocketIoHandler):

    def __init__(self, app, stop_event: asyncio.Event):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(app, stop_event)
        self.__exchange_default = app.etat.configuration.exchange_default

        # Mapping files for all connected sessions. Indexed by SID.
        self.__mapping_files: dict[str, dict] = {}

        self._semaphore_routing = asyncio.BoundedSemaphore(value=10)

    async def _preparer_socketio_events(self):
        await super()._preparer_socketio_events()
        # Add public handlers for logging into the application.
        self._sio.on('authentication_register', handler=self.register)
        self._sio.on('authentication_authenticate', handler=self.authenticate)
        self._sio.on('authentication_challenge_webauthn', handler=self.generate_challenge_webauthn)
        self._sio.on('authentication_addrecoverycsr', handler=self.add_recovery_csr)
        self._sio.on('authentication_subscribe_activation', handler=self.subscribe_activation)
        self._sio.on('authentication_unsubscribe_activation', handler=self.unsubscribe_activation)

        # Add private handler for routed messages with the provided api configuration.
        self._sio.on('request_application_list', handler=self.request_application_list)
        self._sio.on('route_message', handler=self.handle_routed_message)

        # Private listeners
        self._sio.on('subscribe', handler=self.handle_subscribe)
        self._sio.on('unsubscribe', handler=self.handle_unsubscribe)

    async def map_connection(self, sid: str, message: dict):
        try:
            mapping = message['attachements']['apiMapping']
        except KeyError:
            return  # No mapping

        if mapping.get('sig'):
            # Verify signed api mapping file
            cert = await self.etat.validateur_message.verifier(mapping, utiliser_date_message=True, utiliser_idmg_message=True)
            roles = cert.get_roles
            if 'webapi' not in roles or 'signature' not in roles:
                raise Exception('The api mapping signature is invalid')

            # Check that the IDMG of the message is in the allowed lists for webapi signature
            idmg = cert.idmg
            if idmg not in AUTHORIZED_WEBAPI_IDMG:
                raise Exception('Unauthorized signature for webapi')

            api_map = json.loads(mapping['contenu'])
        else:
            if not self.etat.configuration.dev_mode:
                raise Exception('The api mapping is not signed')
            # Dev mode - accept unsigned mapping files
            api_map = mapping

        # Extract mapping keys for requests and commands
        defaults = api_map['defaults']
        default_domain = defaults['domain']

        requests = {}
        for r in api_map.get('requests'):
            req_domain = r.get('domain') or default_domain
            action = r['action']
            requests['/'.join((req_domain, action))] = r

        commands = {}
        for c in api_map.get('commands'):
            cmd_domain = c.get('domain') or default_domain
            action = c['action']
            commands['/'.join((cmd_domain, action))] = c

        api_map[REQUESTS_DICT] = requests
        api_map[COMMANDS_DICT] = commands

        # Save the api map file for this sid
        self.__mapping_files[sid] = api_map

    async def handle_routed_message(self, sid, message: dict):
        try:
            mapping = self.__mapping_files[sid]
        except KeyError:
            return self.etat.formatteur_message.signer_message(
                Constantes.KIND_REPONSE, {'ok': False, 'err': 'Session action mapping not initialized'})[0]

        defaults = mapping['defaults']
        default_domain = defaults['domain']
        default_exchange = defaults['exchange']

        routage = message['routage']
        action = routage['action']
        domain = routage.get('domaine') or default_domain

        kind = message['kind']
        if kind == Constantes.KIND_REQUETE:
            request_mapping = mapping[REQUESTS_DICT]['/'.join((domain, action))]
            exchange = request_mapping.get('exchange') or default_exchange
            return await self.executer_requete(sid, message, domain, action, exchange)
        elif kind == Constantes.KIND_COMMANDE:
            command_mapping = mapping[COMMANDS_DICT]['/'.join((domain, action))]
            exchange = command_mapping.get('exchange') or default_exchange
            nowait = command_mapping.get('nowait')
            return await self.executer_commande(sid, message, domain, action, exchange, nowait=nowait)
        else:
            raise Exception('Unsupported message kind')

    async def subscribe_activation(self, sid: str, request: dict):
        """
        Subscribes without authentication to listen for the registration of a user certificate.
        :param sid:
        :param request:
        :return:
        """
        exchanges = [Constantes.SECURITE_PRIVE]
        public_key = request['publicKey']
        routing_keys = [f'evenement.CoreMaitreDesComptes.{public_key}.activationFingerprintPk']
        # Note : message non authentifie (sans signature). Flag enveloppe=False empeche validation.
        reponse = await self.subscribe(sid, request, routing_keys, exchanges, enveloppe=False, session_requise=False)
        reponse_signee, correlation_id = self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, reponse)
        return reponse_signee

    async def unsubscribe_activation(self, sid: str, request: dict):
        # Note : message non authentifie (sans signature)
        exchanges = [Constantes.SECURITE_PRIVE]
        public_key = request['publicKey']
        routing_keys = [f'evenement.CoreMaitreDesComptes.{public_key}.activationFingerprintPk']
        reponse = await self.unsubscribe(sid, request, routing_keys, exchanges, session_requise=False)
        reponse_signee, correlation_id = self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, reponse)
        return reponse_signee

    async def handle_subscribe(self, sid: str, request: dict):
        routing_keys, exchanges, enveloppe = await self.map_subscription(sid, request)
        response = await self.subscribe(sid, request, routing_keys, exchanges, enveloppe=enveloppe)
        signed_response, correlation_id = self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, response)
        return signed_response

    async def handle_unsubscribe(self, sid: str, request: dict):
        routing_keys, exchanges, _enveloppe = await self.map_subscription(sid, request)
        response = await self.unsubscribe(sid, request, routing_keys, exchanges)
        signed_response, correlation_id = self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, response)
        return signed_response

    async def map_subscription(self, sid: str, request: dict) -> (list[str], list[str], EnveloppeCertificat):
        try:
            mapping = self.__mapping_files[sid]
        except KeyError:
            return self.etat.formatteur_message.signer_message(
                Constantes.KIND_REPONSE, {'ok': False, 'err': 'Session action mapping not initialized'})[0]

        event_name = request['routage']['action']
        event_mapping = mapping['subscriptions'][event_name]

        enveloppe = await self.etat.validateur_message.verifier(request)
        user_id = enveloppe.get_user_id

        parametres = json.loads(request['contenu'])

        if user_id is None:
            raise Exception('Access denied: no user_id in the certificate')

        exchanges = event_mapping['exchanges']

        routing_keys: list[str] = list()
        for rk in event_mapping['routingKeys']:
            rk = rk.replace('{USER_ID}', user_id)

            if len(parametres) > 0:
                rk = rk.format(**parametres)

            # Check to ensure no unmapped values remain
            try:
                rk.index('{')
            except ValueError:
                pass  # Ok
            else:
                raise Exception('Routing key not mapped completely: %s', rk)

            routing_keys.append(rk)

        return routing_keys, exchanges, enveloppe

    async def authenticate(self, sid: str, message: dict):
        response = await super().upgrade(sid, message)
        response_content = json.loads(response['contenu'])
        if response_content.get('ok') is not True:
            return response

        # Map the api
        try:
            await self.map_connection(sid, message)
        except Exception:
            self.__logger.exception('Mapping error')
            # Override the response
            return self.etat.formatteur_message.signer_message(
                Constantes.KIND_REPONSE, {'ok': False, 'err': 'Invalid mapping file'})[0]

        return response

    async def register(self, _sid: str, message: dict):

        nom_usager = message['nomUsager']
        idmg = self.etat.clecertificat.enveloppe.idmg

        # Verifier CSR
        try:
            csr = EnveloppeCsr.from_str(message['csr'])  # Note : valide le CSR, lance exception si erreur
        except Exception:
            reponse = {'ok': False, 'err': 'Signature CSR invalide'}
            reponse, correlation_id = self.etat.formatteur_message.signer_message(Constantes.KIND_REPONSE, reponse)
            return reponse

        # Calculer fingerprintPk
        fingperint_pk = csr.get_fingerprint_pk()  # Le fingerprint de la cle publique == la cle (32 bytes)

        # Generer nouveau user_id
        params_user_id = ':'.join([nom_usager, idmg, fingperint_pk])
        user_id = hacher(params_user_id, hashing_code='blake2s-256', encoding='base58btc')

        commande = {
            'csr': message['csr'],
            'nomUsager': nom_usager,
            'userId': user_id,
            'securite': '1.public',
            'fingerprint_pk': fingperint_pk
        }

        producer = await asyncio.wait_for(self.etat.producer_wait(), timeout=0.5)
        resultat = await producer.executer_commande(
            commande,
            domaine=Constantes.DOMAINE_CORE_MAITREDESCOMPTES, action='inscrireUsager',
            exchange=Constantes.SECURITE_PRIVE)

        reponse_parsed = resultat.parsed
        reponse = reponse_parsed['__original']
        return reponse

    async def generate_challenge_webauthn(self, sid: str, message: dict):
        reponse_challenge = await self.executer_commande(
            sid, message,
            domaine=Constantes.DOMAINE_CORE_MAITREDESCOMPTES,
            action='genererChallenge',
            exchange=Constantes.SECURITE_PRIVE
        )

        # Intercepter la reponse - on ne veut pas transmettre l'information passkey, juste le challenge
        reponse_contenu = json.loads(reponse_challenge['contenu'])

        reponse_usager = dict()

        try:
            authentication_challenge = reponse_contenu['authentication_challenge']
            passkey_authentication = reponse_contenu['passkey_authentication']

            # Conserver la passkey dans la session
            async with self._sio.session(sid) as session:
                session['authentication_challenge'] = authentication_challenge
                session['passkey_authentication'] = passkey_authentication

            reponse_usager['authentication_challenge'] = authentication_challenge
        except KeyError:
            pass  # Pas de challenge d'authentification

        try:
            reponse_usager['registration_challenge'] = reponse_contenu['registration_challenge']
        except KeyError:
            pass  # Pas de challenge de registration

        try:
            # Conserver le challenge de delegation
            session['delegation_challenge'] = reponse_contenu['challenge']
            reponse_usager['delegation_challenge'] = reponse_contenu['challenge']
        except KeyError:
            pass  # Pas de challenge de delegation

        reponse_usager, correlation = self.etat.formatteur_message.signer_message(
            Constantes.KIND_REPONSE, reponse_usager)

        return reponse_usager

    async def request_application_list(self, sid: str, message: dict):
        reponse = await self.executer_requete(sid, message, Constantes.DOMAINE_CORE_TOPOLOGIE, 'listeApplicationsDeployees')

        # Ajouter un message signe localement pour prouver l'identite du serveur (instance_id)
        info_serveur = self.etat.formatteur_message.signer_message(
            Constantes.KIND_REPONSE,
            dict(),
            domaine='maitredescomptes',
            action='identite',
            ajouter_chaine_certs=True
        )[0]

        reponse['attachements'] = {'serveur': info_serveur}

        return reponse

    async def add_recovery_csr(self, _sid: str, message: dict):
        command = {
            'nomUsager': message['nomUsager'],
            'csr': message['csr'],
        }

        producer = await asyncio.wait_for(self.etat.producer_wait(), timeout=0.5)
        result = await producer.executer_commande(
            command,
            domaine=Constantes.DOMAINE_CORE_MAITREDESCOMPTES,
            action='ajouterCsrRecovery',
            exchange=Constantes.SECURITE_PRIVE)

        parsed_response = result.parsed
        response = parsed_response['__original']
        return response
