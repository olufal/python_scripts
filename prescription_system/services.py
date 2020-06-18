from alto_pharmacy.employee import Employee as e
from alto_pharmacy.action import Action
from datetime import datetime, timedelta


class Service:
    """
        Service class
    """
    def __init__(
            self,
    ):
        """
            Constructor for a Service Class
        """
        self.respond_action = Action(name='respond_action', description='responding to a patient message')
        self.request_renewal = Action(name='request_renewal', description='requesting a renewal from a doctor')
        self.conduct_assessment = Action(name='conduct_assessment', description='conducting a clinical assessment')
        self.troubleshoot_insurance = Action(name='troubleshoot_insurance',
                                             description='troubleshooting an insurance rejection')
        self.actions = [self.respond_action, self.request_renewal, self.conduct_assessment, self.troubleshoot_insurance]
        self.generated_actions = []


    def build_action(self, action: Action):
        action.create_datetime = datetime.now()
        if action.name == 'respond_action':
            action.expire_datetime = action.create_datetime + timedelta(minutes=30)
        elif action.name == 'troubleshoot_insurance':
            action.expire_datetime = action.create_datetime + timedelta(minutes=180)
        else:
            action.expire_datetime = action.create_datetime + timedelta(minutes=60)

        self.generated_actions.append(action)


    def pull_next_action(self, employee: e):
        if len(self.generated_actions) > 0:
            now = datetime.now()
            expiration_times = [action.expire_datetime for action in self.generated_actions]
            closest_time = min(expiration_times, key=lambda x: abs(x - now))
            for action in self.generated_actions:
                if action.expire_datetime == closest_time:
                    try:
                        index = self.generated_actions.index(action)
                        action = self.generated_actions.pop(index)
                        employee.assignAction(action)
                        action.show()
                    except ValueError as v:
                        print(f'Action {action.name} has already been selected')
                        print(v)
        else:
            print('No Actions currently Exist')