import argparse
import asyncio
import logging
import signal

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur

from millegrilles_web.WebAppMain import WebAppMain

from millegrilles_web.Commandes import CommandHandler
from millegrilles_web.Intake import IntakeFichiers
from millegrilles_web.WebServer import WebServer
from millegrilles_web.WebAppMain import LOGGING_NAMES as LOGGING_NAMES_WEB, adjust_logging
from coupdoeil.WebServerCoupdoeil import WebServerCoupdoeil

logger = logging.getLogger(__name__)

LOGGING_NAMES = ['coupdoeil']
LOGGING_NAMES.extend(LOGGING_NAMES_WEB)


class CoupdoeilAppMain(WebAppMain):

    def __init__(self):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__()

    async def configurer(self):
        await super().configurer()

    async def configurer_web_server(self):
        self._web_server = WebServerCoupdoeil(self.etat, self._commandes_handler)
        self._web_server.setup()

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.info("Fermer application, signal: %d" % signum)
        self._stop_event.set()

    def parse(self) -> argparse.Namespace:
        args = super().parse()
        adjust_logging(LOGGING_NAMES_WEB, args)
        return args


async def demarrer():
    main_inst = CoupdoeilAppMain()

    signal.signal(signal.SIGINT, main_inst.exit_gracefully)
    signal.signal(signal.SIGTERM, main_inst.exit_gracefully)

    await main_inst.configurer()
    logger.info("Run main coupdoeil")
    await main_inst.run()
    logger.info("Fin main coupdoeil")


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    logging.basicConfig()
    for log in LOGGING_NAMES:
        logging.getLogger(log).setLevel(logging.INFO)
    asyncio.run(demarrer())


if __name__ == '__main__':
    main()
