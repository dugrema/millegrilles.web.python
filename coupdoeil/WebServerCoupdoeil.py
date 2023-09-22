import logging

from millegrilles_web.WebServer import WebServer


class WebServerCoupdoeil(WebServer):

    def __init__(self, etat, commandes):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(etat, commandes)

    def _preparer_routes(self):
        self.__logger.info("Preparer routes WebServerCoupdoeil sous /coupdoeil")
        pass
