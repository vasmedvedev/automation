class Proxy(object):
    """
    Wrapper aroung xmlrpclib.ServerProxy with platform API methods

    """
    def __init__(self, server_proxy):
        """
        :param server_proxy: XMLRPC server proxy
        :type server_proxy: xmlrpclib.ServerProxy
        """
        self.proxy = server_proxy
