import asyncio
import logging
from asyncio import TaskGroup
from concurrent.futures.thread import ThreadPoolExecutor

from typing import Awaitable

from millegrilles_messages.bus.BusContext import ForceTerminateExecution, StopListener
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_web.Configuration import WebAppConfiguration
from millegrilles_web.MgbusHandler import MgbusHandler
from millegrilles_web.SocketIoSubscriptions import SocketIoSubscriptions
from millegrilles_web.mapped.MappedContext import MappedWebAppContext
from millegrilles_web.mapped.MappedSocketioHandler import MappedSocketIoHandler
from millegrilles_web.mapped.MappedWebAppManager import MappedWebAppManager
from millegrilles_web.mapped.MappedWebAppServer import MappedWebAppServer

LOGGER = logging.getLogger(__name__)


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = WebAppConfiguration.load()
    context = MappedWebAppContext(config)

    LOGGER.setLevel(logging.INFO)
    LOGGER.info("Starting")

    # Wire classes together, gets awaitables to run
    coros = await wiring(context)

    try:
        # Use taskgroup to run all threads
        async with TaskGroup() as group:
            for coro in coros:
                group.create_task(coro)

            # Create a listener that fires a task to cancel all other tasks
            async def stop_group():
                group.create_task(force_terminate_task_group())
            stop_listener = StopListener(stop_group)
            context.register_stop_listener(stop_listener)

    except* (ForceTerminateExecution, asyncio.CancelledError):
        pass  # Result of the termination task


async def wiring(context: MappedWebAppContext) -> list[Awaitable]:
    # Some threads get used to handle sync events for the duration of the execution. Ensure there are enough.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

    # Service instances
    bus_connector = MilleGrillesPikaConnector(context)

    # Facade
    manager = MappedWebAppManager(context)

    # Access modules
    socketio_subscriptions_handler = SocketIoSubscriptions(manager)
    socketio_handler = MappedSocketIoHandler(manager, socketio_subscriptions_handler)
    web_server = MappedWebAppServer(manager)
    bus_handler = MgbusHandler(manager, socketio_subscriptions_handler.handle_subscription_message)

    # Setup, injecting additional dependencies
    context.bus_connector = bus_connector
    manager.setup(
        # socketio_subscriptions_handler.handle_subscription_message,
        bus_handler.get_subscription_queue,
        socketio_subscriptions_handler.evict_user,
    )
    await web_server.setup()
    await socketio_handler.setup(web_server.web_app)

    # Create tasks
    coros = [
        context.run(),
        web_server.run(),
        bus_handler.run(),
        manager.run(),
        socketio_handler.run(),
        socketio_subscriptions_handler.run(),
    ]

    return coros


if __name__ == '__main__':
    asyncio.run(main())
    LOGGER.info("Stopped")
