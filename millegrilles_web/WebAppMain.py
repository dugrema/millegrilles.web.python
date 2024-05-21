import argparse
import asyncio
import datetime
import logging

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur
from millegrilles_messages.docker.Entretien import TacheEntretien

from millegrilles_web.Configuration import ConfigurationApplicationWeb
from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web.Commandes import CommandHandler
from millegrilles_web.Intake import IntakeFichiers
from millegrilles_web.WebServer import WebServer

logger = logging.getLogger(__name__)

LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_web']


class WebAppMain:

    def __init__(self):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__args = self.parse()
        self.__config = ConfigurationApplicationWeb()
        self.__etat = self.init_etat()

        self._rabbitmq_dao: Optional[MilleGrillesConnecteur] = None
        self._web_server: Optional[WebServer] = None

        self._commandes_handler: Optional[CommandHandler] = None
        self.__intake_fichiers: Optional[IntakeFichiers] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    def init_etat(self):
        return EtatWeb(self.__config)

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        # Utiliser override pour la configuration du nombre de correlations en attente
        self.__config.nb_reply_correlation_max = self.nb_reply_correlation_max

        await self.__etat.reload_configuration()

        self.__etat.ajouter_tache_entretien(
            TacheEntretien(datetime.timedelta(minutes=10), self.__etat.charger_consignation))

        if self.args.fichiers:
            self.__logger.info("Activation de la reception de fichiers")
            self.__intake_fichiers = IntakeFichiers(self._stop_event, self.__etat)

        self._commandes_handler = self.init_command_handler()
        self._rabbitmq_dao = MilleGrillesConnecteur(self._stop_event, self.__etat, self._commandes_handler)
        self._rabbitmq_dao.nb_reply_correlation_max = self.__config.nb_reply_correlation_max

        if self.args.fichiers:
            await self.__intake_fichiers.configurer()

        await self.configurer_web_server()

    def init_command_handler(self) -> CommandHandler:
        return CommandHandler(self)

    async def configurer_web_server(self):
        raise NotImplementedError('must implement')

    async def run(self):

        threads = [
            self._rabbitmq_dao.run(),
            self.__etat.run(self._stop_event, self._rabbitmq_dao),
            self._web_server.run(),
        ]

        if self.__intake_fichiers:
            threads.append(self.__intake_fichiers.run())

        await asyncio.gather(*threads)

        logger.info("run() stopping")

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.info("Fermer application, signal: %d" % signum)
        self._stop_event.set()

    @property
    def nb_reply_correlation_max(self):
        return 20

    @property
    def config(self):
        return self.__config

    @property
    def args(self) -> argparse.Namespace:
        return self.__args

    @property
    def etat(self):
        return self.__etat

    @property
    def intake_fichiers(self):
        return self.__intake_fichiers

    @property
    def socket_io_handler(self):
        return self._web_server.socket_io_handler

    @property
    def app(self):
        return self._web_server.app

    @property
    def app_path(self):
        return self._web_server.app_path

    def parse(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Demarrer le serveur d'applications web pour MilleGrilles")
        parser.add_argument(
            '--fichiers', action="store_true", required=False,
            help="Active le path /WEBAPP/fichiers pour l'upload et transfert de fichiers vers la consignation"
        )
        parser.add_argument(
            '--verbose', action="store_true", required=False,
            help="Active le logging maximal"
        )

        args = parser.parse_args()
        adjust_logging(LOGGING_NAMES, args)

        return args


def adjust_logging(loggers: list[str], args: argparse.Namespace):
    if args.verbose is True:
        for log in loggers:
            logging.getLogger(log).setLevel(logging.DEBUG)
