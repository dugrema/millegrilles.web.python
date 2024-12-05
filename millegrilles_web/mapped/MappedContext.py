from millegrilles_web.Configuration import WebAppConfiguration
from millegrilles_web.Context import WebAppContext


class MappedWebAppContext(WebAppContext):

    def __init__(self, configuration: WebAppConfiguration):
        super().__init__(configuration)
