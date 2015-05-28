from collections import defaultdict
import time
import threading
import subprocess
import socket


import click

class ParseToRiemann(object):

	def __init__(self, timeout, interval, procs, directory, host, port, limit):
		self.timeout = timeout
		self.interval = interval
		self.procs = procs.split(',')
		self.directory = directory
		self.host = host
		self.port = port
		self.limit = limit
		self.syshost = socket.gethostname()

		self.metric = parse(self.procs, self.directory)
		self.status = dict()
		self.data   = defaultdict(list) 
		self.max_time_running = defaultdict(lambda: 0)

	def grab():
		if not self.directory.endswith('/*')
			self.directory = self.directory + '/*'
		args = ['sv', 'status', self.directory]

		return subprocess.check_output(args).split('\n')

	def parse_and_update():
		output = self.grab()
		
		for line in output:
			split_by_space = line.split()
			if len(split_by_space) == 0:
				continue

			if service_name in self.procs or len(self.procs) == 0:
				service_name = split_by_space[1].split('/')[-1]
				time_running = int(split_by_space[-1].replace('s', ''))

				data[service_name].append(time_running)
				if time_running > max_time_running[service_name]:
					max_time_running[service_name] = time_running

				if len(data[service_name]) > self.limit:
					data[service_name].pop()
		

		return data

	def alive_or_dead():
		status = dict()
		for k, v, in data.iteritems():
			if len(data[k]) > 1:
				status[k] = data[k][-1] - data[k][-2] > self.interval
			else:
				status[k] = True
			
	def collect_and_emit(self):
		self.parse_and_update
		self.status = self.alive_or_dead()

		from riemann_client.transport import TCPTransport
		from riemann_client.client import QueuedClient

		with QueuedClient(TCPTransport(self.host, int(self.port))) as client:
			for k, v in self.status.iteritems():
				client.event(service = k, status = v, metric_f = new_metric[k], host = self.syshost)
			client.flush()

		threading.Timer(self.interval, self.main).start()

@click.command()
@click.option('--host' ,'-h',       default ='127.0.0.1',       help = 'The riemann host')
@click.option('--timeout', '-m',    default = '5',              help = 'The timeout time')
@click.option('--port', '-p',       default = '5555',           help = 'The riemann port')
@click.option('--event-host', '-e', default = '',               help = 'Event hostname')
@click.option('--interval', '-i',   default ='5',               help = 'Seconds between updates')
@click.option('--procs',            default = '',               help = 'Filters services')
@click.option('--directory', '-d',  default = '/etc/service/',  help = 'Directory')
@click.option('--limit', '-l',      default = '100',            help = 'Max number of historical data to store')
def main_cli(timeout, interval, procs, directory, host, port, limit):
    parser = ParseToRiemann(timeout, interval, procs, directory, host, port, limit)
    parser.collect_and_emit()

if __name__ == '__main__':
    main_cli()
