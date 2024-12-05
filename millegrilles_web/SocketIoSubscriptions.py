import asyncio
import logging
import json
import socketio

from typing import Optional, Union

from millegrilles_messages.bus.PikaQueue import RoutingKey
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages.MessagesThread import MessagesThread, RessourcesConsommation
from millegrilles_web.WebAppManager import WebAppManager


class SocketIoSubscriptions:
    """
    Gere les subscriptions a des "chat rooms" pour recevoir des evenements MQ via Socket.IO
    """

    def __init__(self, manager: WebAppManager):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__manager = manager
        self.__messages_thread: Optional[MessagesThread] = None
        self.__ressources_consommation: Optional[RessourcesConsommation] = None
        self.__rooms = dict()  # { [room_handle]: {exchange, routing_key} }
        self.__semaphore_rooms = asyncio.BoundedSemaphore(value=1)
        self.__sio: Optional[socketio.AsyncServer] = None

    @property
    def messages_thread(self) -> Optional[MessagesThread]:
        return self.__messages_thread

    @messages_thread.setter
    def messages_thread(self, m):
        self.__messages_thread = m

    @property
    def ressources_consommation(self) -> Optional[RessourcesConsommation]:
        return self.__ressources_consommation

    @ressources_consommation.setter
    def ressources_consommation(self, r):
        self.__ressources_consommation = r

    @property
    def sio(self) -> socketio.AsyncServer:
        return self.__sio

    @sio.setter
    def sio(self, value: socketio.AsyncServer):
        self.__sio = value

    async def run(self):
        self.__logger.info("run Debut")
        await asyncio.gather(
            self.entretien_rooms()
        )
        self.__logger.info("run Fin")

    async def entretien_rooms(self):
        while self.__manager.context.stopping is False:

            # Faire entretien des rooms
            async with self.__semaphore_rooms:
                for room_handle, value in self.__rooms.copy().items():
                    # if namespace_root is None or namespace_root.get(room_handle) is None:
                    participants = 0
                    try:
                        participants_list = self.get_participants(room_handle)
                        for p in participants_list:
                            participants = participants + 1
                    except TypeError:
                        participants = 0  # Room n'existe pas
                    if participants == 0:
                        # Retirer la routing key
                        self.__logger.info('entretien_rooms Retrait room %s : absente de namespace_root. Params %s' % (room_handle, value))
                        await self.retirer_rk(**value)
                        del self.__rooms[room_handle]

            await self.__manager.context.wait(20)

    async def disconnect(self, sid: str):
        pass

    async def subscribe(self, sid: str, user_id: str, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        self.__logger.debug('subscribe sid: %s (user_id %s) de %s/%s' % (sid, user_id, exchanges, routing_keys))
        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]
        if isinstance(exchanges, str):
            exchanges = [exchanges]

        for exchange in exchanges:
            for rk in routing_keys:
                await self.ajouter_sid_a_room(sid, exchange, rk)

        # Filter routing_keys pour retirer les partitions
        rks_filtrees = set()
        for rk in routing_keys:
            split_rk = rk.split('.')
            rk_sans_partition = '.'.join([split_rk[0], split_rk[1], split_rk[-1]])
            rks_filtrees.add(rk_sans_partition)
        rks_filtrees = list(rks_filtrees)

        # {ok: true, routingKeys: rkEvents, exchanges, roomParam}
        return {
            'ok': True,
            'routingKeys': rks_filtrees, 'exchanges': exchanges,
            'roomParam': None
        }

    async def unsubscribe(self, sid: str, user_id: Optional[str], routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        self.__logger.debug('unsubscribe sid: %s (user_id %s) de %s/%s' % (sid, user_id, exchanges, routing_keys))
        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]
        if isinstance(exchanges, str):
            exchanges = [exchanges]

        for exchange in exchanges:
            for rk in routing_keys:
                await self.retirer_sid_de_room(sid, exchange, rk)

        # Filter routing_keys pour retirer les partitions
        rks_filtrees = set()
        for rk in routing_keys:
            split_rk = rk.split('.')
            rk_sans_partition = '.'.join([split_rk[0], split_rk[1], split_rk[-1]])
            rks_filtrees.add(rk_sans_partition)
        rks_filtrees = list(rks_filtrees)

        # {ok: true, routingKeys: rkEvents, exchanges}
        return {'ok': True, 'routingKeys': rks_filtrees, 'exchanges': exchanges}

    async def ajouter_sid_a_room(self, sid: str, exchange: str, routing_key: str):
        room_handle = get_room_handle(exchange, routing_key)
        await self.ajouter_rk(routing_key, exchange)
        await self.add_sid_room(sid, room_handle)

        self.__logger.debug("ajouter_sid_a_room Rooms : %s" % self.rooms)

        # Conserver une reference pour cleanup de la routing key dans MQ si la room est fermee
        async with self.__semaphore_rooms:
            self.__rooms[room_handle] = {'exchange': exchange, 'routing_key': routing_key}

    async def retirer_sid_de_room(self, sid: str, exchange: str, routing_key: str):
        room_handle = get_room_handle(exchange, routing_key)
        await self.remove_sid_room(sid, room_handle)

        self.__logger.debug("retirer_sid_de_room Rooms : %s", self.rooms)

    async def handle_subscription_message(self, message: MessageWrapper):
        self.__logger.debug("handle_subscription_message Mgbus subscriptions message received : %s", message)

        type_evenement, domaine, partition, action = get_key_parts(message.routing_key)
        try:
            user_id = message.routage.get('user_id')
        except AttributeError:
            user_id = None  # Default to None - use case of encrypted response as event

        try:
            original = message.parsed['__original']
        except TypeError:
            # Encrypted response
            original = json.loads(message.contenu)

        message_room = {
            'exchange': message.exchange,
            'routingKey': message.routing_key,
            'message': original
        }
        exchange = message.exchange

        nom_evenement = f'{type_evenement}.{domaine}.{action}'
        nom_evenement_all_domaines = f'{type_evenement}.*.{action}'

        # Toujours emettre pour listeners partition __NONE__
        rooms = [
            f'{exchange}/{type_evenement}.{domaine}.__NONE__.{action}',
            f'{exchange}/{type_evenement}.__ALL__.__NONE__.{action}',
        ]

        if user_id:
            rooms.append(f'{user_id}/{type_evenement}.{domaine}.__NONE__.{action}', )
            rooms.append(f'{user_id}/{type_evenement}.__ALL__.__NONE__.{action}', )

        if partition != '__NONE__':
            # Aussi emettre sur la room de la partition et combinaisons (* domaines et partitions)
            rooms.append(f'{exchange}/{type_evenement}.{domaine}.{partition}.{action}')
            rooms.append(f'{exchange}/{type_evenement}.__ALL__.{partition}.{action}')
            rooms.append(f'{exchange}/{type_evenement}.{domaine}.__ALL__.{action}')

            if user_id:
                rooms.append(f'{user_id}/{type_evenement}.{domaine}.{partition}.{action}')
                rooms.append(f'{user_id}/{type_evenement}.__ALL__.{partition}.{action}')
                rooms.append(f'{user_id}/{type_evenement}.{domaine}.__ALL__.{action}')

        for nom_room in rooms:
            exchange_room, routing_room = nom_room.split('/')
            type_message_room, domaine_room, partition_room, action_room = routing_room.split('.')
            if domaine_room == '__ALL__':
                nom_evenement_room = nom_evenement_all_domaines
            else:
                nom_evenement_room = nom_evenement
            await self.emit_message_room(nom_evenement_room, message_room, nom_room)

    async def ajouter_rk(self, routing_key: str, exchange: str):
        """ Ajouter une routing_key sur la Q du consumer dans MQ """
        subscription_queue = self.__manager.get_subcription_queue()
        await subscription_queue.add_bind_routing_key(RoutingKey(exchange, routing_key))

    async def retirer_rk(self, routing_key: str, exchange: str):
        """ Retirer une routing_key de la Q du consumer dans MQ """
        subscription_queue = self.__manager.get_subcription_queue()
        await subscription_queue.remove_unbind_routing_key(RoutingKey(exchange, routing_key))

    async def add_sid_room(self, sid: str, room: str):
        self.__logger.debug("Ajout sid %s a room %s" % (sid, room))
        return await self.__sio.enter_room(sid, room)

    async def remove_sid_room(self, sid: str, room: str):
        self.__logger.debug("Retrait sid %s de room %s" % (sid, room))
        return await self.__sio.leave_room(sid, room)

    async def emit_message_room(self, event: str, data: dict, room: str):
        return await self.__sio.emit(event, data, room=room)

    @property
    def rooms(self):
        # return self._sio.manager.rooms
        return self.__sio.manager.rooms

    def get_participants(self, room: str):
        return self.__sio.manager.get_participants('/', room)

    async def evict_usager(self, message: MessageWrapper):
        user_id = message.parsed['userId']
        self.__logger.info("evict_usager Evist user %s", user_id)

        # Find SIDs associated to the user_id
        for (sid, _) in self.get_participants('user.%s' % user_id):
            self.__logger.debug("evict_usager %s SID:%s", user_id, sid)
            async with self.__sio.session(sid) as session:
                session.clear()

            await self.__sio.disconnect(sid)


def get_key_parts(routing_key: str):
    parts = routing_key.split('.')
    if len(parts) == 3:
        type_message, domaine, action = parts
        partition = '__NONE__'
    elif len(parts) == 4:
        type_message, domaine, partition, action = parts
    else:
        raise ValueError("Type de routing key non supporte : %s" % routing_key)

    if domaine == '*':
        domaine = '__ALL__'

    if partition == '*':
        partition = '__ALL__'

    return type_message, domaine, partition, action


def get_room_handle(exchange: str, routing_key: str):
    type_message, domaine, partition, action = get_key_parts(routing_key)
    return f'{exchange}/{type_message}.{domaine}.{partition}.{action}'
