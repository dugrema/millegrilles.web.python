import aiohttp
import asyncio
import logging

from typing import Optional

from millegrilles_web.EtatWeb import EtatWeb


class ConsignationHandler:
    """
    Download et dechiffre les fichiers de media a partir d'un serveur de consignation
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatWeb):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__url_consignation: Optional[str] = None

        self.__session_http_requests: Optional[aiohttp.ClientSession] = None
