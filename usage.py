from ignite import IgniteClient, IgniteFailed


""" Default endpoint string is 'http://localhost:8080/ignite' """
client = IgniteClient()
# Remote example
# client = IgniteClient('172.168.13.5', 8443, 'https')

""" Create Cache """
cache_name = 'my_cache'
# also, client.getorcreate
client.get_or_create_cache(cache_name)

""" Put + Get value """
client.put('my_key', 'my_val', cache_name=cache_name)
# Will print 'my_val'
print client.get('my_key', cache_name=cache_name)

""" Put if absent + Increment """
client.put_if_absent('my_integer', 5, cache_name=cache_name)
# also, client.incr
client.increment('my_integer', 5, cache_name=cache_name)
# Will print '10'
client.get('my_integer', cache_name=cache_name)

""" Accessing nonexistent cache throws IgniteFailed """
try:
    client.get('my_key', cache_name='no_such_cache')
except IgniteFailed as e:
    # Failed to find cache for given cache name (null for default cache):
    # no_such_cache
    print '{}'.format(e)


""" Response object is a transparent proxy """
key = 'another_key'
client.put(key, 'another_value', cache_name=cache_name)
resp = client.get(key, cache_name=cache_name)
# Will print 'another_value'
print resp
# Will print 'True'
print resp == 'another_value'
# The response object contains 3 properties - affinity_id, session_token, success_status
# Will print '0'
print resp.success_status
