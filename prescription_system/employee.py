from datetime import date, datetime, timedelta
from alto_pharmacy.action import Action


class Employee:
    """
        Employee class
    """
    def __init__(self, name: str):
        """
            Constructor for a Employee Class
        """
        self.name = name
        self.create_datetime = datetime.now()
        self.role = None
        self.current_action = None

    def assignAction(self, action: Action):
        self.current_action = action
        print(f"Employee {self.name} has selected action: {action.name}")
