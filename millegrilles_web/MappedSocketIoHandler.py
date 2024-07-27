import asyncio
import logging

from millegrilles_web.SocketIoHandler import SocketIoHandler


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
