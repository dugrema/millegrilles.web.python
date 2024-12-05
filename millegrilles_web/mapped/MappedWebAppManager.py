from millegrilles_web.WebAppManager import WebAppManager
from millegrilles_web.mapped.MappedContext import MappedWebAppContext


class MappedWebAppManager(WebAppManager):

    def __init__(self, context: MappedWebAppContext):
        super().__init__(context)

    @property
    def app_name(self) -> str:
        return 'millegrilles'

    @property
    def application_path(self):
        return '/millegrilles'
