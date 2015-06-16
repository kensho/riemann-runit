DESCRIPTION
-----------
Riemann-runit is a library that will monitor the current status of a service,
and log its stability to a provided riemann host (local as default). It then
effectively monitors and reports whether a service has been killed, revived,
or still running.

REQUIREMENTS
------------
The following modules are necessary for this package to function:
* bernhard 0.2.3 (https://pypi.python.org/pypi/bernhard/0.2.3)
* click 3.3 (https://pypi.python.org/pypi/click/3.3)
* protobuf 2.6.1 (https://pypi.python.org/pypi/protobuf/2.6.1)
* riemann-client 6.0.2 (https://pypi.python.org/pypi/riemann-client)