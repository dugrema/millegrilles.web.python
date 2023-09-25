import datetime
import logging
import pytz

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur, MessageWrapper, RessourcesConsommation
from millegrilles_messages.MilleGrillesConnecteur import CommandHandler as CommandesAbstract

from millegrilles_web.Intake import IntakeFichiers
from millegrilles_web import Constantes as  ConstantesWeb
from millegrilles_web.SocketIoHandler import SocketIoHandler
from millegrilles_web.EtatWeb import EtatWeb


class CommandHandler(CommandesAbstract):

    def __init__(self, web_app):
        super().__init__()
        self.__web_app = web_app
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__messages_thread = None

    @property
    def etat(self) -> EtatWeb:
        return self.__web_app.etat

    @property
    def intake_fichiers(self) -> IntakeFichiers:
        return self.__web_app.intake_fichiers

    @property
    def socket_io_handler(self) -> SocketIoHandler:
        return self.__web_app.socket_io_handler

    def get_routing_keys(self):
        return [
            # f'evenement.{Constantes.DOMAINE_GROSFICHIERS}.{Constantes.EVENEMENT_GROSFICHIERS_CHANGEMENT_CONSIGNATION_PRIMAIRE}',
            # 'evenement.CoreTopologie.changementConsignation',
        ]

    def configurer_consumers(self, messages_thread: MessagesThread):
        self.__messages_thread = messages_thread

        # Creer une Q pour les messages volatils. Utilise le nom du OU dans le certificat.
        nom_ou = self.etat.clecertificat.enveloppe.subject_organizational_unit_name
        if nom_ou is not None:
            nom_queue_ou = f'{nom_ou}/volatil'
            res_volatil = RessourcesConsommation(self.callback_reply_q, nom_queue=nom_queue_ou, channel_separe=True,
                                                 est_asyncio=True, durable=True)
            res_volatil.set_ttl(300000)  # millisecs
            res_volatil.ajouter_rk(
                Constantes.SECURITE_PUBLIC,
                f'evenement.{Constantes.DOMAINE_GLOBAL}.{Constantes.EVENEMENT_CEDULE}', )
        else:
            res_volatil = None

        res_evenements = RessourcesConsommation(self.callback_reply_q, channel_separe=True, est_asyncio=True)
        res_evenements.ajouter_rk(
            Constantes.SECURITE_PUBLIC,
            f'evenement.{Constantes.DOMAINE_MAITRE_DES_CLES}.{Constantes.EVENEMENT_MAITREDESCLES_CERTIFICAT}', )

        if self.socket_io_handler:
            # Listener pour les subscriptions. Les routing keys sont gerees par subsbscription_handler dynamiquement.
            res_subscriptions = RessourcesConsommation(
                self.socket_io_handler.subscription_handler.callback_reply_q,
                channel_separe=True, est_asyncio=True)
            self.socket_io_handler.subscription_handler.messages_thread = messages_thread
            self.socket_io_handler.subscription_handler.ressources_consommation = res_subscriptions

            messages_thread.ajouter_consumer(res_subscriptions)

        messages_thread.ajouter_consumer(res_evenements)
        if res_volatil:
            messages_thread.ajouter_consumer(res_volatil)

    async def traiter_commande(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        routing_key = message.routing_key
        exchange = message.exchange
        action = routing_key.split('.').pop()
        type_message = routing_key.split('.')[0]
        enveloppe = message.certificat

        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        try:
            roles = enveloppe.get_roles
        except ExtensionNotFound:
            roles = list()

        try:
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = list()

        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        if type_message == 'evenement':
            if exchange == Constantes.SECURITE_PUBLIC:
                if action == Constantes.EVENEMENT_CEDULE:
                    await self.traiter_cedule(producer, message)
                    return False
                if self.socket_io_handler:
                    if action == Constantes.EVENEMENT_MAITREDESCLES_CERTIFICAT:
                        await self.socket_io_handler.recevoir_certificat_maitredescles(message)
                        return False

        self.__logger.warning("Message non gere : %s sur exchange %s " % (routing_key, exchange))

        return False  # Empeche de transmettre un message de reponse

    async def traiter_cedule(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        contenu = message.parsed
        date_cedule = datetime.datetime.fromtimestamp(contenu['estampille'], tz=pytz.UTC)

        now = datetime.datetime.now(tz=pytz.UTC)
        if now - datetime.timedelta(minutes=2) > date_cedule:
            return  # Vieux message de cedule

        weekday = date_cedule.weekday()
        hour = date_cedule.hour
        minute = date_cedule.minute

        if weekday == 0 and hour == 4:
            pass
        elif minute % 20 == 0:
            pass

