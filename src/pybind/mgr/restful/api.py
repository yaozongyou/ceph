from flask import request
from flask_restful import Resource

import json
import traceback

import common

from functools import wraps
from collections import defaultdict

## We need this to access the instance of the module
#
# We can't use 'from module import instance' because
# the instance is not ready, yet (would be None)
import module


# Helper function to catch and log the exceptions
def catch(f):
    @wraps(f)
    def catcher(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            module.instance.log.error(str(traceback.format_exc()))
            return {'message': str(traceback.format_exc()).split('\n')}, 500
    return catcher


# Handle authorization
def auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth:
            return (
                {'message': 'auth: No HTTP username/password'},
                401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            )

        # Lookup the password-less tokens first
        if auth.username not in module.instance.tokens.values():
            # Check the ceph auth db
            msg = module.instance.verify_user(auth.username, auth.password)
            if msg:
                return (
                    {'message': 'auth: ' + msg},
                    401,
                    {'WWW-Authenticate': 'Basic realm="Login Required"'}
                )

        return f(*args, **kwargs)
    return decorated


# Helper function to lock the function
def lock(f):
    @wraps(f)
    def locker(*args, **kwargs):
        with module.instance.requests_lock:
            return f(*args, **kwargs)
    return locker



class Index(Resource):
    _endpoint = '/'

    @catch
    def get(self):
        """
        Show the basic information for the REST API
        This includes values like api version or auth method
        """
        return {
            'api_version': 1,
            'auth':
                'Use ceph auth key pair as HTTP Basic user/password '
                '(requires caps mon allow * to function properly)',
            'doc': 'See /doc endpoint',
            'info': "Ceph Manager RESTful API server",
        }



class Auth(Resource):
    _endpoint = '/auth'

    @catch
    def get(self):
        """
        Generate a brand new password-less login token for the user
        Uses HTTP Basic Auth for authentication
        """
        auth = request.authorization
        if not auth:
            return (
                {'message': 'auth: No HTTP username/password'},
                401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            )

        # Do not create a new token for a username that is already a token
        if auth.username in module.instance.tokens.values():
            return {
                'token': auth.username
            }

        # Check the ceph auth db
        msg = module.instance.verify_user(auth.username, auth.password)
        if msg:
            return (
                {'message': 'auth: ' + msg},
                401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            )

        # Create a password-less login token for the user
        # This overwrites any old user tokens
        return {
            'token': module.instance.set_token(auth.username)
        }


    @catch
    @auth
    def delete(self):
        """
        Delete the password-less login token for the user
        """

        if module.instance.unset_token(request.authorization.username):
            return {'success': 'auth: Token removed'}

        return {'message': 'auth: No token for the user'}, 500



class ConfigCluster(Resource):
    _endpoint = '/config/cluster'

    @catch
    @auth
    def get(self):
        """
        Show all cluster configuration options
        """
        return module.instance.get("config")



class ConfigClusterKey(Resource):
    _endpoint = '/config/cluster/<string:key>'

    @catch
    @auth
    def get(self, key):
        """
        Show specific configuration option
        """
        return module.instance.get("config").get(key, None)



class ConfigOsd(Resource):
    _endpoint = '/config/osd'

    @catch
    @auth
    def get(self):
        """
        Show OSD configuration options
        """
        flags = module.instance.get("osd_map")['flags']

        # pause is a valid osd config command that sets pauserd,pausewr
        flags = flags.replace('pauserd,pausewr', 'pause')

        return flags.split(',')


    @catch
    @auth
    def patch(self):
        """
        Modify OSD configration options
        """

        args = json.loads(request.data)

        commands = []

        valid_flags = set(args.keys()) & set(common.OSD_FLAGS)
        invalid_flags = list(set(args.keys()) - valid_flags)
        if invalid_flags:
            module.instance.log.warn("%s not valid to set/unset" % invalid_flags)

        for flag in list(valid_flags):
            if args[flag]:
                mode = 'set'
            else:
                mode = 'unset'

            commands.append({
                'prefix': 'osd ' + mode,
                'key': flag,
            })

        return module.instance.submit_request([commands])



class CrushRule(Resource):
    _endpoint = '/crush/rule'

    @catch
    @auth
    def get(self):
        """
        Show crush rules
        """
        rules = module.instance.get('osd_map_crush')['rules']
        nodes = module.instance.get('osd_map_tree')['nodes']

        for rule in rules:
            rule['osd_count'] = len(common.crush_rule_osds(nodes, rule))

        return rules



class CrushRuleset(Resource):
    _endpoint = '/crush/ruleset'

    @catch
    @auth
    def get(self):
        """
        Show crush rulesets
        """
        rules = module.instance.get('osd_map_crush')['rules']
        nodes = module.instance.get('osd_map_tree')['nodes']

        ruleset = defaultdict(list)
        for rule in rules:
            rule['osd_count'] = len(common.crush_rule_osds(nodes, rule))
            ruleset[rule['ruleset']].append(rule)

        return ruleset



class Doc(Resource):
    _endpoint = '/doc'

    @catch
    def get(self):
        """
        Show documentation information
        """
        return module.instance.get_doc_api()



class Mon(Resource):
    _endpoint = '/mon'

    @catch
    @auth
    def get(self):
        """
        Show the information for all the monitors
        """
        return module.instance.get_mons()



class MonName(Resource):
    _endpoint = '/mon/<string:name>'

    @catch
    @auth
    def get(self, name):
        """
        Show the information for the monitor name
        """
        mon = filter(
            lambda x: x['name'] == name,
            module.instance.get_mons()
        )

        if len(mon) != 1:
                return {'message': 'Failed to identify the monitor node "%s"' % name}, 500

        return mon[0]



class Osd(Resource):
    _endpoint = '/osd'

    @catch
    @auth
    def get(self):
        """
        Show the information for all the OSDs
        """
        # Parse request args
        ids = request.args.getlist('id[]')
        pool_id = request.args.get('pool', None)

        return module.instance.get_osds(ids, pool_id)



class OsdId(Resource):
    _endpoint = '/osd/<int:osd_id>'

    @catch
    @auth
    def get(self, osd_id):
        """
        Show the information for the OSD id
        """
        osd = module.instance.get_osds([str(osd_id)])
        if len(osd) != 1:
            return {'message': 'Failed to identify the OSD id "%d"' % osd_id}, 500

        return osd[0]


    @catch
    @auth
    def patch(self, osd_id):
        """
        Modify the state (up, in) of the OSD id or reweight it
        """
        args = json.loads(request.data)

        commands = []

        if 'in' in args:
            if args['in']:
                commands.append({
                    'prefix': 'osd in',
                    'ids': [str(osd_id)]
                })
            else:
                commands.append({
                    'prefix': 'osd out',
                    'ids': [str(osd_id)]
                })

        if 'up' in args:
            if args['up']:
                return {'message': "It is not valid to set a down OSD to be up"}, 500
            else:
                commands.append({
                    'prefix': 'osd down',
                    'ids': [str(osd_id)]
                })

        if 'reweight' in args:
            commands.append({
                'prefix': 'osd reweight',
                'id': osd_id,
                'weight': args['reweight']
            })

        return module.instance.submit_request([commands])



class OsdIdCommand(Resource):
    _endpoint = '/osd/<int:osd_id>/command'

    @catch
    @auth
    def get(self, osd_id):
        """
        Show implemented commands for the OSD ID
        """
        osd = module.instance.get_osd_by_id(osd_id)

        if not osd:
            return {'message': 'Failed to identify the OSD id "%d"' % osd_id}, 500

        if osd['up']:
            return common.OSD_IMPLEMENTED_COMMANDS
        else:
            return []



class OsdIdCommandId(Resource):
    _endpoint = '/osd/<int:osd_id>/command/<string:command>'

    @catch
    @auth
    def post(self, osd_id, command):
        """
        Run the implemented command for the OSD id
        """
        osd = module.instance.get_osd_by_id(osd_id)

        if not osd:
            return {'message': 'Failed to identify the OSD id "%d"' % osd_id}, 500

        if not osd['up'] or command not in common.OSD_IMPLEMENTED_COMMANDS:
            return {'message': 'Command "%s" not available' % command}, 500

        return module.instance.submit_request([[{
            'prefix': 'osd ' + command,
            'who': str(osd_id)
        }]])



class Pool(Resource):
    _endpoint = '/pool'

    @catch
    @auth
    def get(self):
        """
        Show the information for all the pools
        """
        pools = module.instance.get('osd_map')['pools']

        # pgp_num is called pg_placement_num, deal with that
        for pool in pools:
            if 'pg_placement_num' in pool:
                pool['pgp_num'] = pool.pop('pg_placement_num')

        return pools


    @catch
    @auth
    def post(self):
        """
        Create a new pool
        Requires name and pg_num dict arguments
        """
        args = json.loads(request.data)

        # Check for the required arguments
        pool_name = args.pop('name', None)
        if pool_name is None:
            return {'message': 'You need to specify the pool "name" argument'}, 500

        pg_num = args.pop('pg_num', None)
        if pg_num is None:
            return {'message': 'You need to specify the "pg_num" argument'}, 500

        # Run the pool create command first
        create_command = {
            'prefix': 'osd pool create',
            'pool': pool_name,
            'pg_num': pg_num
        }

        # Check for invalid pool args
        invalid = common.invalid_pool_args(args)
        if invalid:
            return {'message': 'Invalid arguments found: "%s"' % str(invalid)}, 500

        # Schedule the creation and update requests
        return module.instance.submit_request(
            [[create_command]] +
            common.pool_update_commands(pool_name, args)
        )



class PoolId(Resource):
    _endpoint = '/pool/<int:pool_id>'

    @catch
    @auth
    def get(self, pool_id):
        """
        Show the information for the pool ID
        """
        pool = module.instance.get_pool_by_id(pool_id)

        if not pool:
            return {'message': 'Failed to identify the pool id "%d"' % pool_id}, 500

        # pgp_num is called pg_placement_num, deal with that
        if 'pg_placement_num' in pool:
            pool['pgp_num'] = pool.pop('pg_placement_num')
        return pool


    @catch
    @auth
    def patch(self, pool_id):
        """
        Modify the information for the pool id
        """
        args = json.loads(request.data)

        # Get the pool info for its name
        pool = module.instance.get_pool_by_id(pool_id)
        if not pool:
            return {'message': 'Failed to identify the pool id "%d"' % pool_id}, 500

        # Check for invalid pool args
        invalid = common.invalid_pool_args(args)
        if invalid:
            return {'message': 'Invalid arguments found: "%s"' % str(invalid)}, 500

        # Schedule the update request
        return module.instance.submit_request(common.pool_update_commands(pool['pool_name'], args))


    @catch
    @auth
    def delete(self, pool_id):
        """
        Remove the pool data for the pool id
        """
        pool = module.instance.get_pool_by_id(pool_id)

        if not pool:
            return {'message': 'Failed to identify the pool id "%d"' % pool_id}, 500

        return module.instance.submit_request([[{
            'prefix': 'osd pool delete',
            'pool': pool['pool_name'],
            'pool2': pool['pool_name'],
            'sure': '--yes-i-really-really-mean-it'
        }]])



class Request(Resource):
    _endpoint = '/request'

    @catch
    @auth
    def get(self):
        """
        List all the available requests and their state
        """
        states = {}
        for _request in module.instance.requests:
            states[_request.id] = _request.get_state()

        return states


    @catch
    @auth
    @lock
    def delete(self):
        """
        Remove all the finished requests
        """
        num_requests = len(module.instance.requests)

        module.instance.requests = filter(
            lambda x: not x.is_finished(),
            module.instance.requests
        )

        # Return the job statistics
        return {
            'cleaned': num_requests - len(module.instance.requests),
            'remaining': len(module.instance.requests),
        }



class RequestId(Resource):
    _endpoint = '/request/<string:request_id>'

    @catch
    @auth
    def get(self, request_id):
        """
        Show the information for the request id
        """
        request = filter(
            lambda x: x.id == request_id,
            module.instance.requests
        )

        if len(request) != 1:
            return {'message': 'Unknown request UUID "%s"' % str(request_id)}, 500

        request = request[0]
        return request.humanify()


    @catch
    @auth
    @lock
    def delete(self, request_id):
        """
        Remove the request id from the database
        """
        for index in range(len(module.instance.requests)):
            if module.instance.requests[index].id == request_id:
                return module.instance.requests.pop(index).humanify()

        # Failed to find the job to cancel
        return {'message': 'No such request id'}, 500



class Server(Resource):
    _endpoint = '/server'

    @catch
    @auth
    def get(self):
        """
        Show the information for all the servers
        """
        return module.instance.list_servers()



class ServerFqdn(Resource):
    _endpoint = '/server/<string:fqdn>'

    @catch
    @auth
    def get(self, fqdn):
        """
        Show the information for the server fqdn
        """
        return module.instance.get_server(fqdn)


class Shutdown(Resource):
    _endpoint = '/shutdown'

    @catch
    def post(self):
        """
        Shutdown the server
        For internal use only
        """
        args = json.loads(request.data)

        if args.get('key', None) != module.instance.shutdown_key:
            return {'message': 'Invalid shutdown key'}, 500

        shutdown = request.environ.get('werkzeug.server.shutdown')

        if shutdown is None:
            return {'message': 'Not running with the Werkzeug Server'}, 500

        shutdown()
