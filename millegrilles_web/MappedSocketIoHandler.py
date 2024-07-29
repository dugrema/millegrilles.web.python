import asyncio
import logging

from millegrilles_messages.messages import Constantes
from millegrilles_web.SocketIoHandler import SocketIoHandler
from millegrilles_messages.messages.Hachage import hacher
from millegrilles_messages.certificats.Generes import EnveloppeCsr


class MappedSocketIoHandler(SocketIoHandler):

    def __init__(self, app, stop_event: asyncio.Event):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(app, stop_event)
        self.__exchange_default = app.etat.configuration.exchange_default

        # Mapping files for all connected sessions. Indexed by SID.
        self.__mapping_files: dict[str, dict] = {}

        self._semaphore_mapping = asyncio.BoundedSemaphore(value=10)

    async def _preparer_socketio_events(self):
        await super()._preparer_socketio_events()
        # Only initial handler, allows receiving the mapping configuration after authentication.
        self._sio.on('_map', handler=self.map_connection)

        # Add public handlers for logging into the application.
        self._sio.on('inscrireUsager', handler=self.inscrire_usager)
        self._sio.on('login_upgrade', handler=self.upgrade)
        # self._sio.on('ajouterCsrRecovery', handler=self.ajouter_csr_recovery)

    async def map_connection(self, sid: str, message: dict):
        async with self._semaphore_mapping:
            # Validate configuration

            # Save the configuration file for this sid
            self.__mapping_files[sid] = message

    def map_message(self, sid, message: dict):
        mapping = self.__mapping_files[sid]
        defaults = mapping['defaults']
        default_domain = defaults['domain']
        default_exchange = defaults['exchange']

        routage = message['routage']
        action = routage['action']
        domain = routage.get('domaine') or default_domain
        exchange = default_exchange

        nowait = False

        return domain, action, exchange, nowait

    async def handle_request(self, sid: str, request: dict):
        domain, action, exchange, nowait = self.map_message(sid, request)
        return await self.executer_requete(sid, request, domain, action, exchange)

    async def handle_command(self, sid: str, request: dict):
        domain, action, exchange, nowait = self.map_message(sid, request)
        return await self.executer_commande(sid, request, domain, action, exchange, nowait=nowait)

    async def handle_subscribe(self, sid: str, request: dict):
        raise NotImplementedError('todo')

    async def handle_unsubscribe(self, sid: str, request: dict):
        raise NotImplementedError('todo')

    async def inscrire_usager(self, _sid: str, message: dict):

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