import asyncio
import logging
import json

from typing import Optional, Union

from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages.MessagesThread import MessagesThread, RessourcesConsommation


class SocketIoSubscriptions:
    """
    Gere les subscriptions a des "chat rooms" pour recevoir des evenements MQ via Socket.IO
    """

    def __init__(self, sio_handler, stop_event: asyncio.Event):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__sio_handler = sio_handler
        self.__stop_event = stop_event
        self.__messages_thread: Optional[MessagesThread] = None
        self.__ressources_consommation: Optional[RessourcesConsommation] = None

        self.__rooms = dict()  # { [room_handle]: {exchange, routing_key} }
        self.__semaphore_rooms = asyncio.BoundedSemaphore(value=1)

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

    async def setup(self):
        pass

    async def run(self):
        self.__logger.info("run Debut")
        await asyncio.gather(
            self.entretien_rooms()
        )
        self.__logger.info("run Fin")

    async def entretien_rooms(self):
        while self.__stop_event.is_set() is False:

            # rooms = self.__sio_handler.rooms
            # namespace_root = rooms.get('/')
            #
            # if namespace_root is None:
            #     self.__logger.debug("entretien_rooms namespace root (/) est None")
            # else:
            #     self.__logger.debug("entretien_rooms namespace root : %s" % namespace_root)

            # Faire entretien des rooms
            async with self.__semaphore_rooms:
                for room_handle, value in self.__rooms.copy().items():
                    # if namespace_root is None or namespace_root.get(room_handle) is None:
                    participants = 0
                    try:
                        participants_list = self.__sio_handler.get_participants(room_handle)
                        for p in participants_list:
                            participants = participants + 1
                    except TypeError:
                        participants = 0  # Room n'existe pas
                    if participants == 0:
                        # Retirer la routing key
                        self.__logger.info('entretien_rooms Retrait room %s : absente de namespace_root. Params %s' % (room_handle, value))
                        await self.retirer_rk(**value)
                        del self.__rooms[room_handle]

            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=20)
            except asyncio.TimeoutError:
                pass  # OK

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
        await self.__sio_handler.ajouter_sid_room(sid, room_handle)

        self.__logger.debug("ajouter_sid_a_room Rooms : %s" % self.__sio_handler.rooms)

        # Conserver une reference pour cleanup de la routing key dans MQ si la room est fermee
        async with self.__semaphore_rooms:
            self.__rooms[room_handle] = {'exchange': exchange, 'routing_key': routing_key}

    async def retirer_sid_de_room(self, sid: str, exchange: str, routing_key: str):
        room_handle = get_room_handle(exchange, routing_key)
        await self.__sio_handler.retirer_sid_room(sid, room_handle)

        self.__logger.debug("retirer_sid_de_room Rooms : %s" % self.__sio_handler.rooms)

    async def callback_reply_q(self, message: MessageWrapper, module_messages: MessagesThread):
        self.__logger.debug("callback_reply_q RabbitMQ nessage recu : %s" % message)

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
            await self.__sio_handler.emettre_message_room(nom_evenement_room, message_room, nom_room)

    async def ajouter_rk(self, routing_key: str, exchange: str):
        """ Ajouter une routing_key sur la Q du consumer dans MQ """
        messages_module = self.__messages_thread.messages_module
        consumers = messages_module.get_consumers()

        for consumer in consumers:
            if consumer.q == self.ressources_consommation.q:
                self.__logger.debug("Queue %s : Ajouter rks %s sur exchanges %s" % (consumer.q, routing_key, exchange))
                consumer.ajouter_routing_key(exchange, routing_key)

    async def retirer_rk(self, routing_key: str, exchange: str):
        """ Retirer une routing_key de la Q du consumer dans MQ """
        messages_module = self.__messages_thread.messages_module
        consumers = messages_module.get_consumers()

        for consumer in consumers:
            if consumer.q == self.ressources_consommation.q:
                self.__logger.debug("Queue %s : Retirer rks %s sur exchanges %s" % (consumer.q, routing_key, exchange))
                consumer.retirer_routing_key(exchange, routing_key)


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
