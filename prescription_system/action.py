from datetime import datetime, timedelta


class Action:
    """
        Action class
    """
    def __init__(
            self,
    ):
        """
            Constructor for a Charge back event
            :param name:
            :param description:
        """
        self.name = None
        self.description = None
        self.create_datetime = None
        self.effective_datetime = None
        self.expire_datetime = None

    def show(self):
        print(f"Action: {self.name}\n"
              f"Description: {self.description}\n"
              f"Effective at: {self.effective_datetime}\n"
              f"Expires at: {self.expire_datetime}\n")
        print()

