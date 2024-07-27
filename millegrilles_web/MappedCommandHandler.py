from millegrilles_web.Commandes import CommandHandler
from millegrilles_messages.messages.MessagesModule import RessourcesConsommation
from millegrilles_messages.messages.MessagesThread import MessagesThread


class MappedCommandHandler(CommandHandler):

    def __init__(self, web_app):
        super().__init__(web_app)

    def configurer_consumers(self, messages_thread: MessagesThread):
        super().configurer_consumers(messages_thread)

        # Queue dynamique selon subscriptions (rooms) dans socketio
        res_subscriptions = RessourcesConsommation(
            self.socket_io_handler.subscription_handler.callback_reply_q,
            channel_separe=True, est_asyncio=True)
        self.socket_io_handler.subscription_handler.messages_thread = messages_thread
        self.socket_io_handler.subscription_handler.ressources_consommation = res_subscriptions

        messages_thread.ajouter_consumer(res_subscriptions)
