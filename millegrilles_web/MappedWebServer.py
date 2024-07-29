import logging

from millegrilles_web.WebServer import WebServer, EtatWeb
from millegrilles_web.MappedCommandHandler import MappedCommandHandler
from millegrilles_web.MappedSocketIoHandler import MappedSocketIoHandler

APP_NAME = 'millegrilles'


class MappedWebServer(WebServer):

    def __init__(self, app_path: str, etat: EtatWeb, commandes: MappedCommandHandler, duree_session=1800):
        super().__init__(app_path, etat, commandes, duree_session)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def get_nom_app(self) -> str:
        return APP_NAME

    async def setup_socketio(self):
        """ Wiring socket.io """
        # Utiliser la bonne instance de SocketIoHandler dans une sous-classe
        self._socket_io_handler = MappedSocketIoHandler(self, self._stop_event)
        await self._socket_io_handler.setup()
