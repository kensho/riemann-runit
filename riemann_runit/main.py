from collections import defaultdict
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

		self.metric = self.parse(self.procs, self.directory)
		self.status = dict()
		self.data   = defaultdict(list) 
		self.max_time_running = defaultdict(lambda: 0)

	def grab(self):
		if not self.directory.endswith('/*'):
			self.directory.append('/*')
		args = ['sv', 'status', self.directory]

		return subprocess.check_output(args).split('\n')

	def parse_and_update(self):
		output = self.grab()
		
		for line in output:
			split_by_space = line.split()
			if len(split_by_space) == 0:
				continue

			service_name = split_by_space[1].split('/')[-1]
			time_running = int(split_by_space[-1].replace('s', ''))

			if service_name in self.procs or len(self.procs) == 0:				
				self.data[service_name].append(time_running)

				if time_running > self.max_time_running[service_name]:
					self.max_time_running[service_name] = time_running

				if len(self.data[service_name]) > self.limit:
					self.data[service_name].pop()

		new_times_running = dict()
		for k, v in self.data:
			new_times_running[k] = v[-1]

		return new_times_running
		
	def alive_or_dead(self):
		status = dict()
		for k, v in self.data.iteritems():
			if len(self.data[k]) > 1:
				status[k] = self.data[k][-1] - self.data[k][-2] > self.interval + 1
			else:
				status[k] = True
			
	def collect_and_emit(self):
		new_times_running = self.parse_and_update()
		self.status = self.alive_or_dead()

		from riemann_client.transport import TCPTransport
		from riemann_client.client import QueuedClient

		with QueuedClient(TCPTransport(self.host, int(self.port))) as client:
			for k, v in self.status.iteritems():
				client.event(service = k, state = v, metric_f = new_times_running[k], host = self.syshost)
			client.flush()

		threading.Timer(self.interval, self.main).start()

@click.command()
@click.option('--host' ,'-h', default ='127.0.0.1', help = 'The riemann host')
@click.option('--timeout', '-m', default = '5', help = 'The timeout time')
@click.option('--port', '-p', default = '5555', help = 'The riemann port')
@click.option('--event-host','-e', default = '', help = 'Event hostname')
@click.option('--interval', '-i', default ='5', help = 'Seconds between updates')
@click.option('--procs', default = '', help = 'Filters services')
@click.option('--directory', '-d', default = '/etc/service/', help = 'Directory')
@click.option('--limit', '-l', default = '100', help = 'Max number of historical data to store')
def main_cli(timeout, interval, procs, directory, host, port, limit):
    parser = ParseToRiemann(timeout, interval, procs, directory, host, port, limit)
    parser.collect_and_emit()

if __name__ == '__main__':
    main_cli()
