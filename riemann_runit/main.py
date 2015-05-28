import time
import threading
import subprocess
import socket


import click

def grab(directory):
	if not directory.endswith('/*')
		directory = directory + '/*'
	args = ['sv', 'status', directory]
	return subprocess.check_output(args).split('\n')

def parse(procs, directory, host, port):
	data = dict()

	output = grab(directory)
	
	for line in output:
		split_by_space = line.split()
		if len(split_by_space) == 0:
			continue

		if service_name in procs or len(procs) == 0:
			service_name = split_by_space[1].split('/')[-1]
			time_running = split_by_space[-1].replace('s', '')

			data[service_name] = time_running
	


	return data

class ParseToRiemann(object):

	def __init__(self, timeout, interval, procs, directory, host, port, syshost):
		self.timeout = timeout
		self.interval = interval
		self.procs = procs.split(',')
		self.directory = directory
		self.host = host
		self.port = port
		self.syshost = socket.gethostname()

		self.metric = parse(self.procs, self.directory)
		self.status = dict()
		

	def main(self):
		new_metric = parse(self.procs, self.directory, self.host, self.port)
		for key in self.metric:
			self.status[key] = math.copysign(1, int(new_metric[key]) - int(self.metric[key]))

		self.metric = new_metric

		from riemann_client.transport import TCPTransport
		from riemann_client.client import QueuedClient

		with QueuedClient(TCPTransport(self.host, int(self.port))) as client:
			for k, v in self.status.iteritems():
				client.event(service = k, status = v, metric_f = new_metric[k], host = self.syshost)
			client.flush()

		threading.Timer(self.interval, self.main).start()

@click.command()
@click.option('--host' ,'-h',       default='127.0.0.1',        help = 'The riemann host')
@click.option('--timeout', '-m',    default = '5',              help = 'The timeout time')
@click.option('--port', '-p',       default = '5555',           help = 'The riemann port')
@click.option('--event-host', '-e', default = '',               help = 'Event hostname')
@click.option('--interval', '-i',   default='5',                help = 'Seconds between updates')
@click.option('--procs',            default = '',               help = 'Filters services')
@click.option('--directory', 'd',   default = '/etc/service/',  help = 'Directory')
def main_cli(timeout, interval, procs):
    parser = ParseToRiemann(timeout, interval, procs, directory, host, port)
    parser.main()

if __name__ == '__main__':
    main_cli()
