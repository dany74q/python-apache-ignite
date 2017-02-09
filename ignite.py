import requests
import urllib
import wrapt


class IgniteClient(object):
    """
        Apache Ignite Rest Client.
        For documentation please consult:
        http://apacheignite.gridgain.org/docs/rest-api
    """

    ENDPOINT = '{scheme}://{host}:{port}/{api_endpoint}'

    def __init__(self, host='localhost', port=8080, scheme='http', api_endpoint='ignite'):
        """
            host - Ignite's server hostname or IP address
            port - Ignite's listening port
            scheme - One of http, https
            api_endpoint - Ignite, unless configured otherwise
        """
        self._host = host
        self._port = port
        self._endpoint = self.ENDPOINT.format(
            scheme=scheme, host=host, port=port, api_endpoint=api_endpoint)

    @staticmethod
    def _normalize_resp_dict(resp_dict):
        if 'cache_name' in resp_dict:
            resp_dict['cacheName'] = resp_dict.pop('cache_name')
        if 'dest_id' in resp_dict:
            resp_dict['destId'] = resp_dict.pop('dest_id')

    def make_command(self, cmd, params=None):
        params = {} if params is None else params
        params = urllib.urlencode(
            {k: v for k, v in params.iteritems() if k and v})
        return requests.get('{endpoint}?cmd={command}&{params}'.format(endpoint=self._endpoint, command=cmd, params=params)).json()

    def log(self, from_=None, path=None, to=None):
        return IgniteResponse(self.make_command('log', {'from': from_, 'path': path, 'to': to}))

    def version(self):
        return IgniteResponse(self.make_command('version'))

    def decrement(self, key, delta, cache_name=None, init=None):
        return IgniteResponse(self.make_command('decr', dict(key=key, delta=delta, cache_name=cache_name, init=init)))

    def decr(self, key, delta, cache_name=None, init=None):
        return self.decrement(key, delta, cache_name, init)

    def increment(self, key, delta, cache_name=None, init=None):
        return IgniteResponse(self.make_command('incr', dict(key=key, delta=delta, cache_name=cache_name, init=init)))

    def incr(self, key, delta, cache_name=None, init=None):
        return self.increment(key, delta, cache_name, init)

    def cache(self, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('cache', dict(cacheName=cache_name, destId=dest_id)))

    def cache_metrics(self, cache_name=None, dest_id=None):
        return self.cache(cache_name, dest_id)

    def cas(self, key, val, val2, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('cas', dict(key=key, val=val, val2=val2, cacheName=cache_name, destId=dest_id)))

    def compare_and_swap(self, key, val, val2, cache_name=None, dest_id=None):
        return self.cas(key, val, val2, cache_name, dest_id)

    def prepend(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('prepend', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def append(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('append', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def replace(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('rep', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def rep(self, key, val, cache_name=None, dest_id=None):
        return self.replace(key, val, cache_name, dest_id)

    def getrep(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('getrep', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def get_and_replace(self, key, val, cache_name=None, dest_id=None):
        return self.getrep(key, val, cache_name, dest_id)

    def repval(self, key, val, val2, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('repval', dict(key=key, val=val, val2=val2, cacheName=cache_name, destId=dest_id)))

    def replace_value(self, key, val, val2, cache_name=None, dest_id=None):
        return self.repval(key, val, val2, cache_name, dest_id)

    def rmvall(self, **kwargs):
        self._normalize_resp_dict(kwargs)
        return IgniteResponse(self.make_command('rmvall', kwargs))

    def remove_all(self, **kwargs):
        return self.rmvall(**kwargs)

    def rmvval(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('rmvval', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def remove_value(self, key, val, cache_name=None, dest_id=None):
        return self.rmvval(key, val, cache_name, dest_id)

    def rmv(self, key, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('rmv', dict(key=key, cacheName=cache_name, destId=dest_id)))

    def remove(self, key, cache_name=None, dest_id=None):
        return self.rmv(key, cache_name, dest_id)

    def getrmv(self, key, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('getrmv', dict(key=key, cacheName=cache_name, destId=dest_id)))

    def get_and_remove(self, key, cache_name=None, dest_id=None):
        return self.getrmv(key, cache_name, dest_id)

    def add(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('add', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def putall(self, **kwargs):
        self._normalize_resp_dict(kwargs)
        return IgniteResponse(self.make_command('putall', kwargs))

    def put(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('put', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def getall(self, **kwargs):
        self._normalize_resp_dict(kwargs)
        return IgniteResponse(self.make_command('getall', kwargs))

    def get(self, key, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('get', dict(key=key, cacheName=cache_name, destId=dest_id)))

    def conkey(self, key, cache_name=None, dest_id=None):
        return IgniteClient(self.make_command('conkey', dict(key=key, cacheName=cache_name, destId=dest_id)))

    def contains_key(self, key, cache_name=None, dest_id=None):
        return self.conkey(key, cache_name, dest_id)

    def conkeys(self, **kwargs):
        self._normalize_resp_dict(kwargs)
        return IgniteResponse(self.make_command('conkeys', kwargs))

    def contains_keys(self, **kwargs):
        return self.conkeys(**kwargs)

    def getput(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('getput', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def get_and_put(self, key, val, cache_name=None, dest_id=None):
        return self.getput(key, val, cache_name, dest_id)

    def putifabs(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('putifabs', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def put_if_absent(self, key, val, cache_name=None, dest_id=None):
        return self.putifabs(key, val, cache_name, dest_id)

    def getputifabs(self, key, val, cache_name=None, dest_id=None):
        return IgniteResponse(self.make_command('getputifabs', dict(key=key, val=val, cacheName=cache_name, destId=dest_id)))

    def get_and_put_if_absent(self, key, val, cache_name=None, dest_id=None):
        return self.getputifabs(key, val, cache_name, dest_id)

    def size(self, cache_name=None):
        return IgniteResponse(self.make_command('size', dict(cacheName=cache_name)))

    def cache_size(self, cache_name=None):
        return self.size(cache_name)

    def metadata(self, cache_name=None):
        return IgniteResponse(self.make_command('metadata', dict(cacheName=cache_name)))

    def cache_metadata(self, cache_name=None):
        return self.metadata(cache_name)

    def getorcreate(self, cache_name=None):
        return IgniteResponse(self.make_command('getorcreate', dict(cacheName=cache_name)))

    def get_or_create_cache(self, cache_name=None):
        return self.getorcreate(cache_name)

    def destcache(self, cache_name=None):
        return IgniteResponse(self.make_command('destcache', dict(caceName=cache_name)))

    def destroy_cache(self, cache_name=None):
        return self.destcache(cache_name)

    def node(self, ip, id, mtr=False, attr=False):
        return IgniteResponse(self.make_command('node', dict(ip=ip, id=id, mtr=mtr, attr=attr)))

    def top(self, ip=None, id=None, mtr=False, attr=False):
        return IgniteResponse(self.make_command('top', dict(ip=ip, id=id, mtr=mtr, attr=attr)))

    def topology(self, ip=None, id=None, mtr=False, attr=False):
        return self.top(ip, id, mtr, attr)

    def exe(self, name, **kwargs):
        return IgniteResponse(self.make_command('exe', kwargs))

    def execute(self, name, **kwargs):
        return self.exe(name, **kwargs)

    def res(self, id):
        return IgniteResponse(self.make_command('res', dict(id=id)))

    def result(self, id):
        return self.res(id)

    def qryexe(self, qry, type, page_size, **kwargs):
        self._normalize_resp_dict(kwargs)
        kwargs.update(dict(qry=qry, type=type, pageSize=page_size))
        return IgniteResponse(self.make_command('qryexec', kwargs))

    def sql_query_execute(self, qry, type, page_size, **kwargs):
        return self.qryexe(qry, type, page_size, **kwargs)

    def qryfldexe(self, qry, page_size, **kwargs):
        self._normalize_resp_dict(kwargs)
        kwargs.update(dict(qry=qry, pageSize=page_size))
        return IgniteResponse(self.make_command('qryfldexe', kwargs))

    def sql_fields_query_execute(self, qry, page_size, **kwargs):
        return self.qryfldexe(qry, page_size, **kwargs)

    def qryscanexe(self, page_size, cache_name, class_name=None):
        return IgniteResponse(self.make_command('qryscanexe', dict(pageSize=page_size, cacheName=cache_name, className=class_name)))

    def sql_scan_query_execute(self, page_size, cache_name, class_name=None):
        return self.qryscanexe(page_size, cache_name, class_name)

    def qryfetch(self, page_size, qry_id):
        return IgniteResponse(self.make_command('qryfetch', dict(pageSize=page_size, qryId=qry_id)))

    def sql_query_fetch(self, page_size, qry_id):
        return self.qryfetch(page_size, qry_id)

    def qrycls(self, qry_id):
        return IgniteResponse(self.make_command('qrycls', dict(qryId=qry_id)))

    def sql_query_close(self, qry_id):
        return self.qrycls(qry_id)


def IgniteResponse(resp_dict):
    resp = resp_dict.get('response', u'') or ''
    affinity_node_id = resp_dict.get('affinityNodeId', u'')
    session_token = resp_dict.get('sessionToken', u'')
    error = resp_dict.get('error', u'')
    success_status = resp_dict.get('successStatus', u'')

    class IgniteResponseWrapper(wrapt.ObjectProxy):

        @property
        def affinity_node_id(self):
            return affinity_node_id

        @property
        def session_token(self):
            return session_token

        @property
        def success_status(self):
            return success_status

        def __repr__(self):
            return repr(resp)

    response = IgniteResponseWrapper(resp)

    if success_status == 2:
        raise IgniteAuthorizationFailed(error)
    elif success_status == 3:
        raise IgniteSecurityCheckFailed(error)
    elif success_status == 1 or error != u'':
        raise IgniteFailed(error)

    return response


class IgniteFailed(Exception):

    def __init__(self, msg):
        super(IgniteFailed, self).__init__(msg)


class IgniteAuthorizationFailed(IgniteFailed):

    def __init__(self, msg=''):
        super(IgniteAuthorizationFailed, self).__init__(
            'Authorization Failed - {}'.format(msg))


class IgniteSecurityCheckFailed(IgniteFailed):

    def __init__(self, msg=''):
        super(IgniteSecurityCheckFailed, self).__init__(
            'Security Check Failed - {}'.format(msg))
