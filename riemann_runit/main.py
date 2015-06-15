from collections import defaultdict
import threading
import subprocess
import socket
import glob
import shlex
import re

import click
from bernhard import Client

TIME_ALIVE_RE = re.compile('([0-9]+)s')

class ParseToRiemann(object):
    def __init__(self, timeout, interval, procs, directory, host, port, limit, service_name):
        self.timeout = timeout
        self.interval = int(interval)
        self.procs = str(procs).split(',')
        self.directory = directory
        self.host = host
        self.port = port
        self.limit = limit
        self.syshost = socket.gethostname()
        self.client = Client(self.host, int(self.port))

        self.metric = defaultdict(lambda: 0)
        self.status = dict()
        self.current_status = dict()
        self.data = defaultdict(list)
        self.max_time_running = defaultdict(lambda: 0)
        self.service_name = service_name
        self.delimiter = '.' if '.' in self.service_name else '/'

    def grab(self):
        if not self.directory.endswith('/*'):
            self.directory += '*'

        cmd = 'sv status {}'.format(self.directory)
        arg = shlex.split(cmd)
        arg = arg[:-1] + glob.glob(arg[-1])

        return subprocess.check_output(arg).split('\n')

    def parse_and_update(self):
        output = self.grab()

        for line in output:
            try:
                split_by_space = line.split()
                if not split_by_space:
                    continue

                service_name = split_by_space[1].split('/')[-1]
                time_running = TIME_ALIVE_RE.findall(line)[0]
                self.current_status[service_name] = split_by_space[0].startswith("run")

                if service_name in self.procs or len(self.procs) <= 1:
                    self.data[service_name].append(int(time_running))

                    if time_running > self.max_time_running[service_name]:
                        self.max_time_running[service_name] = time_running

                    if len(self.data[service_name]) > self.limit:
                        self.data[service_name].pop()

            except (KeyError, TypeError) as e:
                continue

        new_times_running = {k: v[-1] for k, v in self.data.iteritems()}

        return new_times_running

    def alive_or_dead(self):
        status = dict()
        for k, v in self.data.iteritems():
            if not self.current_status[k]:
                status[k] = False
            else:
                status[k] = True if len(self.data[k]) <= 1 else (self.data[k][-1] - self.data[k][-2] > self.interval - 1)

        return status

    def run(self):
        new_times_running = self.parse_and_update()
        self.status = self.alive_or_dead()

        if self.status:
            for k, v in self.status.iteritems():
                send_service = "{service_name}{delimiter}{proc_name}".format(service_name=self.service_name,
                                                                            delimiter=self.delimiter, proc_name=k)
                self.client.send(
                    dict(service=send_service, state="ok" if v else "alert",
                        metric=new_times_running[k], host=self.syshost,
                        ttl=600))
        threading.Timer(int(self.interval), self.run).start()


@click.command()
@click.option('--host', '-h', default='127.0.0.1', help='The riemann host')
@click.option('--timeout', '-m', default='5', help='The timeout time')
@click.option('--port', '-p', default='5555', help='The riemann port')
@click.option('--interval', '-i', default='5', help='Seconds between updates')
@click.option('--procs', default='', help='Filters services')
@click.option('--directory', '-d', default='/etc/service/', help='Directory')
@click.option('--limit', '-l', default='100', help='Max number of historical data to store')
@click.option('--service_name', '-s', default='sv.proc', help = 'Service name')
def main_cli(timeout, interval, procs, directory, host, port, limit, service_name):
    parser = ParseToRiemann(timeout, interval, procs, directory, host, port, limit, service_name)
    parser.run()


if __name__ == '__main__':
    main_cli()
