class Controller:
    """ Manage deployment running instances.  """

    def start(self, deployment):
        raise NotImplemented

    def stop(self, deployment_id):
        raise NotImplemented
