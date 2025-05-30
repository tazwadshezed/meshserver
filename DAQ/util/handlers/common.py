import asyncio
import bz2
from bson import BSON, InvalidBSON
import multiprocessing
import os
import queue
import signal
import time
import uuid
from multiprocessing import util
from multiprocessing.managers import SyncManager
from DAQ.util.hex import _h
from DAQ.util.logger import make_logger
from DAQ.util.utctime import utcepochnow
from DAQ.util.config import load_config, get_topic
from DAQ.util.brokers.broker import local_nats_broker

# ---------------------
# Signal-Ignoring Manager
# ---------------------

def handler_entrypoint(target, handler, data_queue, processed_queue):
    import signal
    from DAQ.util.logger import make_logger

    handler.logger = make_logger(f"{handler.name}:{os.getpid()}")
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    try:
        target(data_queue, processed_queue)
    except Exception as e:
        handler.logger.error(f"Handler error: {e}")
        if not handler.clean_stop:
            raise

class IgnoreSignalManager(SyncManager):
    @classmethod
    def _run_server(cls, registry, address, authkey, serializer, writer,
                    initializer=None, initargs=()):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)

        if initializer:
            initializer(*initargs)

        server = cls._Server(registry, address, authkey, serializer)
        writer.send(server.address)
        writer.close()

        util.info('manager serving at %r', server.address)
        server.serve_forever()

# ---------------------
# HandlerManager
# ---------------------

class HandlerManager:
    def __init__(self):
        self.manager = IgnoreSignalManager()
        self.manager.start()
        self.state = self.manager.dict()
        self.handlers = set()

    def add_handler(self, handler):
        if handler not in self.handlers:
            self.handlers.add(handler)
            handler.state = self.state
            if hasattr(handler, "configure"):
                handler.configure()
            for sub in handler.subhandlers:
                self.add_handler(sub)

    def set_input_queue(self, queue):
        for handler in self.handlers:
            handler.data_queue = queue

    def start(self):
        for handler in self.handlers:
            handler.start()

    def stop(self):
        for handler in self.handlers:
            handler.stop()

        for handler in self.handlers:
            proc = handler.process
            if proc and proc.is_alive():
                proc.join(IHandler.JOIN_TIMEOUT)
                if proc.is_alive():
                    os.kill(proc.pid, signal.SIGKILL)

# ---------------------
# IHandler
# ---------------------

class IHandler:
    GENERIC = 0
    COMPILER = 1
    DECOMPILER = 2
    JOIN_TIMEOUT = 30

    def __init__(self, handler_type=GENERIC, clean_stop=True, **kwargs):
        self.data_queue = multiprocessing.Queue()
        self.processed_queue = multiprocessing.Queue()
        self.process = None
        self.subhandlers = []
        self.kwargs = kwargs
        self.handler_type = handler_type
        self.clean_stop = clean_stop
        self._id = _h(str(uuid.uuid4()).encode())[:4]
        self.name = self.__class__.__name__
        self.ppid = os.getpid()
        self.logger = make_logger(f"{self.name}:{self.ppid}:{self._id}")
        self._living = multiprocessing.Event()
        self.state = {}  # placeholder — overwritten by HandlerManager

    def _mkprocess(self):
        if self.handler_type == IHandler.GENERIC:
            target = self.worker
        elif self.handler_type == IHandler.COMPILER:
            target = self.compile
        elif self.handler_type == IHandler.DECOMPILER:
            target = self.decompile

        self._living = multiprocessing.Event()
        self.process = multiprocessing.Process(
            target=handler_entrypoint,
            args=(target, self, self.data_queue, self.processed_queue),
            name=self.name
        )

    def _check_living(self):
        return self._living.is_set() and os.getppid() == self.ppid

    def start(self, subhandlers=True):
        if not self._living.is_set():
            self.logger.debug("Starting handler...")
            self._mkprocess()
            self._living.set()
            self.process.start()
            self.logger.info(f"PID: {self.process.pid}; TYPE: {self.handler_type}")
            if subhandlers:
                for handler in self.subhandlers:
                    handler.start()

    def stop(self, subhandlers=True, terminate=False, join=True):
        if self.process is None:
            return

        self._living.clear()

        if join:
            self.process.join(self.JOIN_TIMEOUT)

        if subhandlers:
            for handler in self.subhandlers:
                handler.stop()

        if join and self.process.is_alive():
            os.kill(self.process.pid, signal.SIGKILL)

        self.process = None
        self.logger.debug("Handler stopped")

    def is_alive(self):
        return self.process and self.process.is_alive()

    def is_stack_alive(self):
        return self.is_alive() and all(h.is_stack_alive() for h in self.subhandlers)

    def get_dead_handlers(self):
        dead = []
        if not self.is_alive():
            dead.append(self)
        for h in self.subhandlers:
            dead.extend(h.get_dead_handlers())
        return dead

    def loop(self, *_):
        self.set('heartbeat', utcepochnow())

    def add_subhandler(self, subhandler):
        if subhandler not in self.subhandlers:
            self.subhandlers.append(subhandler)
        if not isinstance(subhandler, IStateHandler):
            self.processed_queue = subhandler.data_queue

    def __call__(self, subhandler):
        self.add_subhandler(subhandler)
        return self

    def _kw(self, key):
        return f"{self.name}.{self._id}.{key}"

    def set(self, key, value):
        assert hasattr(self, 'state'), "Handler must be connected to a HandlerManager"
        self.state[self._kw(key)] = value

    def get(self, key, default=None):
        return self.state.get(self._kw(key), default)

    def worker(self, data_queue, processed_queue):
        raise NotImplementedError

    def compile(self, data_queue, processed_queue):
        return self.worker(data_queue, processed_queue)

    def decompile(self, data_queue, processed_queue):
        return self.worker(data_queue, processed_queue)

# ---------------------
# IStateHandler
# ---------------------

class IStateHandler(IHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_queue = None
        self.processed_queue = None

    def _mkprocess(self):
        if not hasattr(self, 'state_modifier'):
            raise NotImplementedError(f"{self.__class__.__name__} must implement a 'state_modifier' method.")
        self.process = multiprocessing.Process(
            target=self.state_modifier,
            name=self.name
        )

# ---------------------
# BSON Handler
# ---------------------

class BSONHandler(IHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = make_logger("BSONHandler")

    def encode(self, payload: dict) -> bytes:
        if not isinstance(payload, dict):
            self.logger.warning(f"[BSON] Invalid payload type: {type(payload)}")
            return b''
        try:
            encoded = BSON.encode(payload)
            BSON(encoded).decode()  # self-test
            self.logger.info(f"[BSON] Encoding payload: {payload}")
            return encoded
        except Exception as e:
            self.logger.error(f"[BSON] Encoding failed: {e}")
            return b''

    def worker(self, data_queue, processed_queue):
        while self._check_living():
            try:
                payload = data_queue.get(timeout=1)
                if not isinstance(payload, dict):
                    self.logger.warning(f"[BSON] Skipping non-dict payload: {type(payload)}")
                    continue
                encoded = self.encode(payload)
                processed_queue.put(encoded)
            except queue.Empty:
                time.sleep(0.1)

# ---------------------
# Compression Handler
# ---------------------

class CompressionHandler(IHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = make_logger(self.__class__.__name__)

    def configure(self):
        cfg = load_config().get("daq", {}).get("compression", {})
        self.set("batch_on", cfg.get("batch_on", 500))
        self.set("batch_at", cfg.get("batch_at", 60))

    def compile(self, data_queue, processed_queue):
        cache = {'cache': [], 'last_processed': time.time()}
        self.state['num_records'] = 0

        while self._check_living():
            try:
                data = data_queue.get(timeout=5)
                cache['cache'].append(data)
            except queue.Empty:
                pass

            batch_on = self.get('batch_on', 500)
            batch_at = self.get('batch_at', 60)

            if cache['cache'] and (
                len(cache['cache']) >= batch_on or
                time.time() - cache['last_processed'] >= batch_at
            ):
                self.logger.info(
                    f"[COMPRESS] Compressing {len(cache['cache'])} records due to "
                    f"{'size' if len(cache['cache']) >= batch_on else 'time'}"
                )
                self.state['num_records'] = max(self.state['num_records'], len(cache['cache']))
                processed_queue.put(bz2.compress(BSON.encode(cache)))
                cache = {'cache': [], 'last_processed': time.time()}

            self.loop(data_queue, processed_queue)
