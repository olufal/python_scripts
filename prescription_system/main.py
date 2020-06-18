from alto_pharmacy.services import Service
from alto_pharmacy.employee import Employee

A = Employee('1')
B = Employee('2', 'Max')
C = Employee('3', 'Jess')

service = Service()
service.build_action(action=service.troubleshoot_insurance)
service.pull_next_action(employee=A)
service.pull_next_action(employee=B)