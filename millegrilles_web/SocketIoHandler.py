import logging
import socketio

from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web import Constantes as ConstantesWeb


class SocketIoHandler:

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._server = server
        self._sio = socketio.AsyncServer(async_mode='aiohttp')

    @property
    def etat(self):
        return self._server.etat

    async def setup(self):
        self._sio.attach(self._server.app, socketio_path=f'{self._server.get_nom_app()}/socket.io')
        await self._preparer_socketio_events()

    async def _preparer_socketio_events(self):
        self._sio.on("connect", handler=self.sio_connect)
        self._sio.on("disconnect", handler=self.sio_disconnect)

        #       if(authScore > 0) {
        #         // On peut activer options privees, l'usager est authentifie
        #         debugConnexions("Configurer evenements prives : %O", configurationEvenements.listenersPrives)
        #         socket.on('upgradeProtege', (params, cb)=>upgradeConnexion(socket, params, cb))
        #         socket.on('upgrade', (params, cb)=>upgradeConnexion(socket, params, cb))
        #       }
        #
        #       socket.on('unsubscribe', (params, cb) => unsubscribe(socket, params, cb))
        #       socket.on('downgradePrive', (params, cb) => downgradePrive(socket, params, cb))
        #       socket.on('genererChallengeCertificat', async cb => {cb(await genererChallengeCertificat(socket))})
        #       socket.on('getCertificatsMaitredescles', async cb => {cb(await getCertificatsMaitredescles(socket))})
        #
        #       socket.subscribe =   (params, cb) => { subscribe(socket, params, cb) }
        #       socket.unsubscribe = (params, cb) => { unsubscribe(socket, params, cb) }
        #       socket.modeProtege = false
        #
        #       socket.on('getInfoIdmg', (params, cb) => getInfoIdmg(socket, params, cb, opts))

        pass

    async def sio_connect(self, sid, environ):
        self.__logger.debug("connect %s", sid)
        try:
            request = environ.get('aiohttp.request')
            user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
            user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
        except KeyError:
            self.__logger.error("sio_connect SID:%s sans parametres request user_id/user_name (pas de session)" % sid)
            raise ConnectionRefusedError('authentication failed')

        async with self._sio.session(sid) as session:
            session['user_name'] = user_name
            session['user_id'] = user_id

        return True

    async def sio_disconnect(self, sid):
        self.__logger.debug("disconnect %s", sid)
