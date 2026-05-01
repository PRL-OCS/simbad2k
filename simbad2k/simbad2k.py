#!/usr/bin/env python
from datetime import datetime
import hashlib
import logging
from logging.config import dictConfig
import math
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_astroquery_proxies():
    """
    Retrieve proxy settings from environment variables.
    Specifically looks for ASTROQUERY_HTTP_PROXY and ASTROQUERY_HTTPS_PROXY.
    """
    proxies = {}
    http_proxy = os.environ.get('ASTROQUERY_HTTP_PROXY')
    https_proxy = os.environ.get('ASTROQUERY_HTTPS_PROXY')
    if http_proxy:
        proxies['http'] = http_proxy
    if https_proxy:
        proxies['https'] = https_proxy
    return proxies

def apply_proxies_to_session(session):
    """
    Inject proxy settings into a requests.Session object if they are defined.
    """
    proxies = get_astroquery_proxies()
    if proxies:
        session.proxies.update(proxies)

from astroquery.exceptions import RemoteServiceError
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
from lcogt_logging import LCOGTFormatter

config = {
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 60
}

dictConfig({
    'version': 1,
    'formatters': {'default': {
        '()': LCOGTFormatter,
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
CORS(app)


class PlanetQuery(object):
    def __init__(self, query, scheme):
        self.query = query.lower()
        self.scheme = scheme

    def get_result(self):
        import json
        with open(os.path.join(os.path.dirname(__file__), 'planets.json'), 'r') as planets:
            p_json = json.loads(planets.read())
        return p_json.get(self.query)


class SimbadQuery(object):
    def __init__(self, query, scheme):
        self.simbad = self._get_simbad_instance()
        self.query = query
        self.scheme = scheme

    def _get_simbad_instance(self):
        from astroquery.simbad import Simbad
        # The imported `Simbad` is already an instance of the `SimbadClass`, but we need to create a new instance
        # of it so that we only add the votable fields once
        simbad = Simbad()
        apply_proxies_to_session(simbad._session)
        simbad.add_votable_fields('pmra', 'pmdec', 'ra', 'dec', 'plx_value', 'main_id')
        return simbad

    def get_result(self):
        result = self.simbad.query_object(self.query)
        if result:
            ret_dict = {}
            for key in ['pmra', 'pmdec', 'ra', 'dec', 'plx_value', 'main_id']:
                if str(result[key][0]) not in ['--', '']:
                    ret_dict[key.lower()] = result[key][0]
            if ret_dict.get('main_id'):
                ret_dict['name'] = ret_dict['main_id']
                del ret_dict['main_id']
            # Earlier versions of Simbad returned both sexagesimal and decimal coordinates. We return ra_d and dec_d
            # to maintain backwards compatibility with the old API.
            ret_dict['ra_d'] = ret_dict['ra']
            ret_dict['dec_d'] = ret_dict['dec']
            return ret_dict
        return None


class MPCQuery(object):
    """
    Query the Minor Planet Center for orbital elements of a given object.
    First submit the object's name to the MPC's query-identifier API to get the object's primary designation.
    Next submit the primary designation to the MPC via astroquery to get the object's orbital elements.
    Returns a dictionary of the object's orbital elements.
    """
    def __init__(self, query, scheme):
        self.query = query
        self.keys = [
            'argument_of_perihelion', 'ascending_node', 'eccentricity',
            'inclination', 'mean_anomaly', 'semimajor_axis', 'perihelion_date_jd',
            'epoch_jd', 'perihelion_distance'
        ]
        self.scheme_mapping = {'mpc_minor_planet': 'asteroid', 'mpc_comet': 'comet'}
        self.query_params_mapping = {
            'mpc_minor_planet': ['name', 'designation', 'number'], 'mpc_comet': ['number', 'designation']
        }
        # Object types as described by the MPC: https://www.minorplanetcenter.net/mpcops/documentation/object-types/
        # 50 is for Interstellar Objects
        self.mpc_type_mapping = {'mpc_minor_planet': [0,1,6,20], 'mpc_comet': [6,10,11,20,50]}
        self.scheme = scheme

    def _clean_result(self, result):
        """
        Converts the results from the MPC into a dictionary of floats.
        Extracts the object's name from the query results and adds it to the dictionary.
        """
        cleaned_result = {}
        for key in self.keys:
            try:
                value = float(result[key])
            except (ValueError, TypeError):
                value = None
            cleaned_result[key] = value
        # Build object name from returned Data
        if result.get('number'):
            if result.get('name'):
                # If there is a number and a name, use both with the format "name (number)", otherwise just use number
                cleaned_result['name'] = f"{result['name']} ({result['number']})"
            else:
                cleaned_result['name'] = f"{result['number']}"
                if result.get('object_type'):
                    # Add comet object type if it exists
                    cleaned_result['name'] += result['object_type']
        else:
            cleaned_result['name'] = result.get('designation')
        return cleaned_result

    def get_primary_designation(self):
        """
        Submit the object's name to the MPC's query-identifier API to get the object's preferred primary and
        preliminary designations.
        In the case of multiple possible targets (usually happens for multiple objects with the same name),
        try to disambiguate with the following criteria:
            * Choose the first target with a 'permid' that could be converted into an INT if searching for an asteroid.
            * Return the first target with a 'permid' if searching for a comet.
            * If no 'permid' is found, query the MPC again using the first target with a preliminary designation.
        """
        proxies = get_astroquery_proxies()
        response = requests.get("https://data.minorplanetcenter.net/api/query-identifier",
                                data=self.query.replace("+", " ").upper(),
                                proxies=proxies)
        identifications = response.json()
        if identifications.get('object_type') and\
                identifications.get('object_type')[1] not in self.mpc_type_mapping[self.scheme]:
            return None, None
        if identifications.get('disambiguation_list'):
            for target in identifications['disambiguation_list']:
                if self.scheme_mapping[self.scheme] == 'asteroid':
                    # If the Target is an asteroid, then the PermID should be an integer
                    try:
                        return int(target['permid']), None
                    except (ValueError, KeyError, TypeError):
                        continue
                elif self.scheme_mapping[self.scheme] == 'comet':
                    # If the Target is a comet, then the PermID should contain a letter (P/C/I)
                    perm_id = target.get('permid')
                    if perm_id:
                        try:
                            # If the PermID is an integer, then it is not a comet, so we keep looking
                            int(target['permid'])
                            continue
                        except (ValueError, KeyError, TypeError):
                            return perm_id, None
                if target.get('unpacked_primary_provisional_designation'):
                    # We need to re-check preliminary designations for multiple targets because these are sometimes
                    # returned by the MPC for disambiguation even though the targets have primary IDs
                    response = requests.get("https://data.minorplanetcenter.net/api/query-identifier",
                                data=target['unpacked_primary_provisional_designation'])
                    identifications = response.json()
                    break
        return identifications['permid'], identifications['unpacked_primary_provisional_designation']

    def get_result(self):
        if self.scheme not in self.scheme_mapping:
            return None
        from astroquery.mpc import MPC
        apply_proxies_to_session(MPC._session)
        schemes = [self.scheme]
        # Get the primary designation of the object and preferred provisional designation if available
        primary_designation, primary_provisional_designation = self.get_primary_designation()
        for scheme in schemes:
            # Make sure the primary designation can be expressed as an integer for asteroids to keep them from being
            # confused for comets
            if primary_designation:
                if scheme == 'mpc_minor_planet':
                    try:
                        primary_designation = int(primary_designation)
                    except ValueError:
                        return None
                params = {'target_type': self.scheme_mapping[scheme], 'number': primary_designation}
                designation = primary_designation
            elif primary_provisional_designation:
                params = {'target_type': self.scheme_mapping[scheme], 'designation': primary_provisional_designation}
                designation = primary_provisional_designation
            else:
                return None
            result = MPC.query_objects_async(**params).json()
            # There are 2 conditions under which we can get back multiple sets of elements:
            # 1. When the search is for a comet and there are multiple types with the same number (e.g. 1P/1I)
            # 2. When the search has multiple sets of elements with different epochs
            if len(result) > 1:
                # Limit results to those that match the object type
                results_that_match_query_type = [elements for elements in result
                                                 if elements.get('object_type', '').lower() in designation.lower()]
                if results_that_match_query_type:
                    result = results_that_match_query_type
            if len(result) > 1:
                recent = None
                recent_time_diff = None
                now = datetime.now()
                # Select the set of elements that are closest to the current date
                for elements in result:
                    if not recent or not recent_time_diff:
                        recent = elements
                        recent_time_diff = math.fabs(
                            (datetime.strptime(recent['epoch'].rstrip('0').rstrip('.'), '%Y-%m-%d') - now).days
                        )
                    else:
                        elements_time_diff = math.fabs(
                            (datetime.strptime(elements['epoch'].rstrip('0').rstrip('.'), '%Y-%m-%d') - now).days
                        )
                        if elements_time_diff < recent_time_diff:
                            recent = elements
                            recent_time_diff = math.fabs(
                                (datetime.strptime(recent['epoch'].rstrip('0').rstrip('.'), '%Y-%m-%d') - now).days
                            )
                ret = self._clean_result(recent)
            elif result:
                ret = self._clean_result(result[0])
            else:
                continue
            try:
                eph = MPC.get_ephemeris(str(designation))
                if eph is not None and len(eph) > 0:
                    # Add tracking rates calculated from Proper Motion and Direction
                    pa_rad = math.radians(float(eph['Direction'][0]))
                    pm = float(eph['Proper motion'][0])
                    ret['ephemeris_ra_rate'] = pm * math.sin(pa_rad)
                    ret['ephemeris_dec_rate'] = pm * math.cos(pa_rad)
            except Exception as e:
                logger.error(f"MPC ephemeris query failed for {designation}: {e}")
            return ret
        return None


class JPLQuery(object):
    def __init__(self, query, scheme):
        self.query = query
        self.scheme = scheme
        self.scheme_mapping = {'jpl_minor_planet': None, 'jpl_major_planet': None}

    def get_result(self):
        if self.scheme not in self.scheme_mapping:
            return None
        from astroquery.jplhorizons import Horizons
        apply_proxies_to_session(Horizons._session)
        try:
            # `id_type` is deprecated in newer astroquery versions, replaced by `None`
            obj = Horizons(id=self.query, location='@sun', id_type=None)
            el = obj.elements()
            if not el or len(el) == 0:
                return None
            result = el[0]
            cleaned_result = {
                'name': result['targetname'] if 'targetname' in el.colnames else self.query,
                'epoch_jd': float(result['datetime_jd']),
                'eccentricity': float(result['e']),
                'perihelion_distance': float(result['q']),
                'inclination': float(result['incl']),
                'ascending_node': float(result['Omega']),
                'argument_of_perihelion': float(result['w']),
                'perihelion_date_jd': float(result['Tp_jd']),
                'mean_daily_motion': float(result['n']),
                'mean_anomaly': float(result['M']),
                'semimajor_axis': float(result['a']),
            }
            try:
                eph_obj = Horizons(id=self.query, location='500@399', id_type=None)
                eph = eph_obj.ephemerides()
                if eph is not None and len(eph) > 0:
                    cleaned_result['ephemeris_ra_rate'] = float(eph['RA_rate'][0])
                    cleaned_result['ephemeris_dec_rate'] = float(eph['DEC_rate'][0])
            except Exception as e:
                logger.error(f"JPL Horizons ephemeris query failed for {self.query}: {e}")
            return cleaned_result
        except Exception as e:
            logger.error(f"JPL Horizons query failed: {e}")
            return None


class NEDQuery(object):
    def __init__(self, query, scheme):
        self.query = query
        self.scheme = scheme

    def get_result(self):
        from astroquery.ipac.ned import Ned
        apply_proxies_to_session(Ned._session)
        ret_dict = {}
        try:
            result_table = Ned.query_object(self.query)
        except RemoteServiceError:
            return None
        if len(result_table) == 0:
            return None
        ret_dict['ra_d'] = result_table['RA'][0]
        ret_dict['dec_d'] = result_table['DEC'][0]
        ret_dict['name'] = result_table['Object Name'][0]
        return ret_dict


SIDEREAL_QUERY_CLASSES = [SimbadQuery, NEDQuery]
NON_SIDEREAL_QUERY_CLASSES = [PlanetQuery, MPCQuery, JPLQuery]
QUERY_CLASSES_BY_TARGET_TYPE = {'sidereal': SIDEREAL_QUERY_CLASSES, 'non_sidereal': NON_SIDEREAL_QUERY_CLASSES}


def generate_cache_key(query, scheme, target_type):
    cache_key = hashlib.sha3_256()
    cache_key.update(query.encode())
    cache_key.update(scheme.encode())
    cache_key.update(target_type.encode())
    return cache_key.hexdigest()


@app.route('/<path:query>')
def root(query):
    if query == 'favicon.ico':
        return jsonify({})
    logger.log(msg=f'Received query for target {query}.', level=logging.INFO)
    target_type = request.args.get('target_type', '')
    scheme = request.args.get('scheme', '')
    logger.log(msg=f'Using search parameters scheme={scheme}, target_type={target_type}', level=logging.INFO)
    cache_key = generate_cache_key(query, scheme, target_type)
    result = cache.get(cache_key)

    if not result:
        query_classes = SIDEREAL_QUERY_CLASSES + NON_SIDEREAL_QUERY_CLASSES
        if target_type:
            query_classes = QUERY_CLASSES_BY_TARGET_TYPE[target_type.lower()]
        for query_class in query_classes:
            result = query_class(query, scheme.lower()).get_result()
            if result:
                cache.set(cache_key, result, timeout=60 * 60 * 60)
                logger.log(msg=f'Found target for {query} via {query_class.__name__} with data {result}',
                           level=logging.INFO)
                return jsonify(**result)
        logger.log(msg=f'Unable to find result for name {query}.', level=logging.INFO)
        return jsonify({'error': 'No match found'})
    logger.log(msg=f'Found cached target for {query} with data {result}', level=logging.INFO)
    return jsonify(**result)


@app.route('/')
def index():
    instructions = ('This is simbad2k. To query for a sidereal object, use '
                    '/&lt;object&gt;?target_type=&lt;sidereal or non_sidereal&gt;. '
                    'For non_sidereal targets, you must include scheme, which can be '
                    'mpc_minor_planet, mpc_comet, jpl_major_planet, or jpl_minor_planet. '
                    'Ex: <a href="/103P?target_type=non_sidereal&scheme=mpc_comet">'
                    '/103P?target_type=non_sidereal&scheme=mpc_comet</a>')
    return instructions


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
