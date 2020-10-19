"""
Formats:
    -   schemed str: scheme://ip:port or scheme://user:pass@user:pswd. Example: socks5://localhost:5556.
    -   dict: {'ip', 'port', 'scheme'} or {'ip', 'port', 'user', 'pswd', 'scheme'}. Scheme is 'http' by default.
    -   None
    Default format: dict.
"""

import re
from utils_ak.re import re as re_tools

# from here: https://stackoverflow.com/questions/106179/regular-expression-to-match-dns-hostname-or-ip-address
IP_PAT = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])"
HOST_PAT = "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])"
PORT_PAT = '(\d+)'
SCHEME_PAT = 'http|https|socks5'
USER_PAT = '[a-zA-Z0-9]+'
PSWD_PAT = '[^\s@]+'

PROXY_PAT = f'%SCHEME://(%USER:%PSWD@)?%HOST:%PORT'
PROXY_PAT = re_tools.replace_with_pattern(PROXY_PAT, '%SCHEME', 'scheme', SCHEME_PAT)
PROXY_PAT = re_tools.replace_with_pattern(PROXY_PAT, '%USER', 'user', USER_PAT)
PROXY_PAT = re_tools.replace_with_pattern(PROXY_PAT, '%PSWD', 'pswd', PSWD_PAT)
PROXY_PAT = re_tools.replace_with_pattern(PROXY_PAT, '%HOST', 'host', HOST_PAT)
PROXY_PAT = re_tools.replace_with_pattern(PROXY_PAT, '%PORT', 'port', PORT_PAT)


def cast_dict(proxy_obj):
    if proxy_obj is None:
        return None
    elif isinstance(proxy_obj, dict):
        if any(x not in proxy_obj for x in ['scheme', 'host', 'port']):
            raise ValueError('Missing some proxy keys')
        if ('user' in proxy_obj or 'pswd' in proxy_obj) and any(x not in proxy_obj for x in ['user', 'pswd']):
            raise ValueError('Missing some proxy keys')
        return proxy_obj
    elif isinstance(proxy_obj, str):
        search = re.search(PROXY_PAT, proxy_obj)
        if not search:
            raise ValueError(f'Non-valid proxy {proxy_obj}')
        return search.groupdict()
    else:
        raise ValueError('Unknown proxy type')


def cast_str(proxy_obj):
    proxy = cast_dict(proxy_obj)
    if not proxy:
        raise Exception('Cannot cast proxy string from null proxy')
    elif proxy.get('user'):
        scheme, user, pswd, host, port = proxy['scheme'], proxy['user'], proxy['pswd'], proxy['host'], proxy['port']
        return f'{scheme}://{user}:{pswd}@{host}:{port}'
    else:
        scheme, host, port = proxy['scheme'], proxy['host'], proxy['port']
        return f'{scheme}://{host}:{port}'


def cast_requests_proxies(proxy_obj):
    """
    :return:
       proxies = {
        'http': 'http://10.10.1.10:3128',
        'https': 'http://10.10.1.10:1080',
    }
    """
    if not proxy_obj:
        return None
    proxy_str = cast_str(proxy_obj)
    return {'http': proxy_str, 'https': proxy_str}


if __name__ == '__main__':
    print(cast_dict('http://ip:1234'))
    print(cast_dict({'scheme': 'http', 'host': 'host', 'port': 9999}))
    print(cast_str({'scheme': 'http', 'host': 'host', 'port': 9999, 'user': 'user', 'pswd': 'pswd'}))
    print(cast_requests_proxies('http://user:pswd@host:9999'))
