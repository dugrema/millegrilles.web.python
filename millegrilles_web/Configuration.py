import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_web import Constantes
from millegrilles_messages.MilleGrillesConnecteur import Configuration as ConfigurationAbstract

CONST_WEBAPP_PARAMS = [
    Constantes.ENV_DIR_STAGING,
    Constantes.PARAM_REDIS_HOSTNAME,
    Constantes.PARAM_REDIS_PORT,
    Constantes.PARAM_REDIS_USERNAME,
    Constantes.PARAM_REDIS_PASSWORD_PATH,
    Constantes.PARAM_REDIS_SESSION_DATABASE,
    Constantes.PARAM_EXCHANGE_DEFAULT,
    Constantes.PARAM_DEV_MODE,
]

CONST_WEB_PARAMS = [
    Constantes.ENV_WEB_PORT,
    ConstantesMessages.ENV_CA_PEM,
    Constantes.PARAM_CERT_PATH,
    Constantes.PARAM_KEY_PATH,
]


class ConfigurationApplicationWeb(ConfigurationAbstract):

    def __init__(self):
        super().__init__()
        self.__dir_staging: Optional[str] = None
        self.redis_hostname = 'redis'
        self.redis_port = 6379
        self.redis_username = Constantes.REDIS_USERNAME_DEFAULT
        self.redis_password: Optional[str] = None
        self.redis_session_db = Constantes.REDIS_SESSION_DATABASE
        self.nb_reply_correlation_max = 20
        self.exchange_default = '2.prive'
        self.dev_mode = False

    def get_params_list(self) -> list:
        params = super().get_params_list()
        params.extend(CONST_WEBAPP_PARAMS)
        return params

    def parse_config(self, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param configuration:
        :return:
        """
        dict_params = super().parse_config(configuration)

        # Params optionnels
        self.__dir_staging = dict_params.get(Constantes.ENV_DIR_STAGING)

        self.redis_hostname = dict_params.get(Constantes.PARAM_REDIS_HOSTNAME) or self.redis_hostname
        self.redis_port = int(dict_params.get(Constantes.PARAM_REDIS_PORT) or self.redis_port)
        self.redis_username = dict_params.get(Constantes.PARAM_REDIS_USERNAME) or self.redis_username
        redis_password_path = dict_params.get(Constantes.PARAM_REDIS_PASSWORD_PATH)
        if redis_password_path:
            with open(redis_password_path, 'r') as fichier:
                for line in fichier:
                    self.redis_password = line
                    break
        self.redis_session_db = int(dict_params.get(Constantes.PARAM_REDIS_SESSION_DATABASE) or self.redis_session_db)
        self.exchange_default = dict_params.get(Constantes.PARAM_EXCHANGE_DEFAULT) or self.exchange_default

        if dict_params.get(Constantes.PARAM_DEV_MODE) in ('1', 'true'):
            self.dev_mode = True

    @property
    def dir_staging(self):
        if self.__dir_staging is None:
            raise TypeError('param env DIR_STAGING non configure')
        return self.__dir_staging


class ConfigurationWeb:

    def __init__(self):
        self.ca_pem_path = '/run/secrets/pki.millegrille.pem'
        self.web_cert_pem_path = '/run/secrets/cert.pem'
        self.web_key_pem_path = '/run/secrets/key.pem'
        self.port = 1443

    def get_env(self) -> dict:
        """
        Extrait l'information pertinente pour pika de os.environ
        :return: Configuration dict
        """
        config = dict()
        for opt_param in CONST_WEB_PARAMS:
            value = os.environ.get(opt_param)
            if value is not None:
                config[opt_param] = value

        return config

    def parse_config(self, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param configuration:
        :return:
        """
        dict_params = self.get_env()
        if configuration is not None:
            dict_params.update(configuration)

        self.ca_pem_path = dict_params.get(ConstantesMessages.ENV_CA_PEM) or self.ca_pem_path
        self.web_cert_pem_path = dict_params.get(ConstantesMessages.ENV_CERT_PEM) or self.web_cert_pem_path
        self.web_key_pem_path = dict_params.get(ConstantesMessages.ENV_KEY_PEM) or self.web_key_pem_path
        self.port = int(dict_params.get(Constantes.ENV_WEB_PORT) or self.port)
