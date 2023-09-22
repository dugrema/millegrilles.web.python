import argparse
import asyncio
import logging
import os
import signal

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur

from millegrilles_web import Constantes
from millegrilles_web.Configuration import ConfigurationApplicationWeb
from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web.Commandes import CommandHandler
from millegrilles_web.Intake import IntakeFichiers
from millegrilles_web.WebServer import WebServer

logger = logging.getLogger(__name__)


class WebMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationApplicationWeb()
        self._etat = EtatWeb(self.__config)

        self.__rabbitmq_dao: Optional[MilleGrillesConnecteur] = None
        self.__web_server: Optional[WebServer] = None

        self.__commandes_handler: Optional[CommandHandler] = None
        self.__intake: Optional[IntakeFichiers] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat.reload_configuration()
        self.__intake = IntakeFichiers(self._stop_event, self._etat)

        self.__commandes_handler = CommandHandler(self._etat, self.__intake)
        self.__rabbitmq_dao = MilleGrillesConnecteur(self._stop_event, self._etat, self.__commandes_handler)

        await self.__intake.configurer()

        self.__web_server = WebServer(self._etat, self.__commandes_handler)
        self.__web_server.setup()

    async def run(self):

        threads = [
            self.__rabbitmq_dao.run(),
            self.__intake.run(),
            self._etat.run(self._stop_event, self.__rabbitmq_dao),
            self.__web_server.run(self._stop_event),
        ]

        await asyncio.gather(*threads)

        logger.info("run() stopping")

    def exit_gracefully(self, signum=None, frame=None):
        logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)
