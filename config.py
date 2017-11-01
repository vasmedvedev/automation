import ConfigParser
from collections import namedtuple
from functools import partial


Config = namedtuple('Config', [
    'database',
    'user',
    'password',
    'db_host',
    'port',
    'host_ip'
])


def config_from_file(path):
    """
    Parses config file and returns Config instance

    :param path: full path to config file
    :type path: str
    :return: Config instance
    :rtype: config.Config
    """
    with open(path, 'r') as f:
        config = ConfigParser.ConfigParser()
        config.readfp(f)

        get_env_var = partial(config.get, 'environment')

        database = get_env_var('DB_NAME')
        user = get_env_var('DB_USER')
        password = get_env_var('DB_PASSWD')
        db_host = get_env_var('DB_HOST')
        port = get_env_var('DB_PORT')
        host_ip = get_env_var('HOST_IP')

        return Config(database, user, password, db_host, port, host_ip)
