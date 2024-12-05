from millegrilles_web.WebServer import WebServer
from millegrilles_web.mapped.MappedWebAppManager import MappedWebAppManager


class MappedWebAppServer(WebServer):

    def __init__(self, manager: MappedWebAppManager):
        super().__init__(manager)
