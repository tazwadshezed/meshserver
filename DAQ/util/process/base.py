
import multiprocessing
import os
import pprint
import signal
import subprocess
import sys
import time

from DAQ.util.process.attribute import AttributeBase


class ProcessBase(AttributeBase):
    """
    Base class for long running objects.

    Use ProcessAttributes in conjunction with ProcessBase to extend functionality.

    Handles SIGTERM and SIGINT to nicely clean up a process by call self.kill()

    Handles the main long-running loop and execution times. It will
    also call self.check() every ``self.CHECK_EVERY`` seconds, the default is 5.
    """

    CHECK_EVERY = 5

    def __init__(self, **kwargs):
        """
        Binds sys exit handlers
        """
        super(ProcessBase, self).__init__()



        self._living = multiprocessing.Event()

        # self.logger.debug("MAIN PID %s" % os.getpid())
        self._sigint_count = 0

        #: SIGTERM is the signal sent to a process to request its termination.
        signal.signal(signal.SIGTERM, self.kill_self)
        #: SIGINT is the signal sent to a process by its controlling terminal when
        #: a user wishes to interrupt the process.
        signal.signal(signal.SIGINT, self.kill_self)

    def kill_self(self, signum=None, stack_frame=None):
        """
        Attempts to cleanly shut everything down.

        In the end uses os._exit since this function
        **SHOULD** be responsible for cleaning up and saving
        state.
        """

        # if signum is not None:
        #     self.logger.info("Detected signal #%d at %s" % (signum,
        #                                                     repr(stack_frame)))
        # else:
        #     self.logger.info("Kill self called")
        #
        # if stack_frame:
        #     self.logger.info("Frame Info [f_code]: %s -- %s" % (stack_frame.f_code.co_filename,
        #                                                         stack_frame.f_lineno))

        if self._living.is_set():
            # self.logger.info("Clearing 'living' boolean")
            self._living.clear()
        elif signum == signal.SIGINT and self._sigint_count >= 2:
            # self.logger.info("Process already slated to stop... forcing termination now")
            sys.exit(1)

        self._sigint_count += signum == signal.SIGINT

    def stop(self):
        self._living.clear()

        super(ProcessBase, self).stop()

    def wake(self): pass
    def sleep(self): pass
    def pre_run(self): pass
    def post_run(self): pass
    def loop_run(self): pass
    def pre_loop(self): pass
    def post_loop(self, loop_time): pass

    def run(self):
        self._living.set()

        self.pre_run()

        self.start()

        check_clock = 0

        while self._living.is_set():
            if check_clock >= self.CHECK_EVERY:
                self.check()
                check_clock = 0

            self.pre_loop()

            loop_start = time.time()

            self.loop_run()

            loop_finished = time.time()
            loop_time = loop_finished - loop_start

            check_clock += loop_time

            self.post_loop(loop_time)

        self.stop()

        self.post_run()


def mafia():

    target_name = sys.argv[0]

    ps_aux = subprocess.Popen(['ps', 'aux'], stdout=subprocess.PIPE)
    output = subprocess.check_output(['grep', target_name],
                                     stdin=ps_aux.stdout)

    ps_aux.wait()

    lines = [x for x in output.decode('utf-8').split('\n') if x]

    processes = []

    #: USER PID %CPU %MEM VSZ RSS TTY STAT START TIME COMMAND
    for line in lines:
        cols = [x for x in line.split(' ') if x]
        stat = cols[:10]
        command = ' '.join(cols[10:])

        if 'grep' in command:
            continue

        processes.append(dict(user=stat[0],
                              pid=stat[1],
                              cpu=stat[2],
                              mem=stat[3],
                              vsz=stat[4],
                              rss=stat[5],
                              tty=stat[6],
                              stat=stat[7],
                              start=stat[8],
                              time=stat[9],
                              cmd=command))

    my_pid = os.getpid()

    for process in processes:
        if int(process['pid']) != my_pid:
            print( "Killing other process %d, cmd %s" % (int(process['pid']),  process['cmd']))
            print(pprint.pformat(process))

            os.kill(int(process['pid']), signal.SIGKILL)
