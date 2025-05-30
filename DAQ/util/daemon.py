import signal
from DAQ.util.logger import make_logger

class BaseDaemon:
    """
    Base class for daemon-style processes.
    Subclasses should implement the `run()` method.
    """

    def __init__(self):
        self.running = True
        self.logger = make_logger(self.__class__.__name__)
        signal.signal(signal.SIGINT, self.stop_signal)
        signal.signal(signal.SIGTERM, self.stop_signal)

    def stop_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal: {signum}")
        self.running = False

    def start(self):
        self.logger.info("Starting daemon process...")
        try:
            self.run()
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user.")
        except Exception as e:
            self.logger.exception(f"Exception in daemon: {e}")
        finally:
            self.logger.info("Daemon process stopped.")

    def run(self):
        """
        Override this method in subclass.
        """
        raise NotImplementedError("Subclasses must implement run()")
