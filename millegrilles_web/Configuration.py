import argparse
import logging

from os import environ
from typing import Optional

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration
from millegrilles_web import Constantes

LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_web']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


def _parse_command_line():
    parser = argparse.ArgumentParser(description="Web Application for MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args


class WebAppConfiguration(MilleGrillesBusConfiguration):

    def __init__(self):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        super().__init__()
        self.redis_username = Constantes.REDIS_USERNAME_DEFAULT
        self.redis_session_db = Constantes.REDIS_SESSION_DATABASE
        # self.nb_reply_correlation_max = 20
        self.port = 1443
        self.application_path: Optional[str] = None
        self.session_duration = Constantes.DEFAULT_SESSION_DURATION
        self.web_cert_path: Optional[str] = None
        self.web_key_path: Optional[str] = None
        self.web_client_ssl_verification = False
        self.dev_mode = False

    def parse_config(self):
        """
        Conserver l'information de configuration
        :return:
        """
        super().parse_config()

        # Params optionnels
        self.redis_username = environ.get(Constantes.PARAM_REDIS_USERNAME) or self.redis_username
        self.redis_session_db = int(environ.get(Constantes.PARAM_REDIS_SESSION_DATABASE) or self.redis_session_db)
        self.application_path = environ.get(Constantes.ENV_APPLICATION_PATH)
        self.port = int(environ.get(Constantes.ENV_WEB_PORT) or self.port)
        self.web_cert_path = environ.get(Constantes.PARAM_WEB_CERT_PATH)
        self.web_key_path = environ.get(Constantes.PARAM_WEB_KEY_PATH)
        if self.web_key_path is None or self.web_cert_path is None:
            self.__logger.info("Using millegrille cert/key for web ssl")
            self.web_key_path = self.key_path
            self.web_cert_path = self.cert_path
            self.web_client_ssl_verification = True

        if environ.get(Constantes.PARAM_DEV_MODE) in ('1', 'true'):
            self.dev_mode = True

    @staticmethod
    def load():
        # Override
        config = WebAppConfiguration()
        _parse_command_line()
        config.parse_config()
        return config
