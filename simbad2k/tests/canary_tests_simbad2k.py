"""
test_simbad2k.py - Tests for the simbad2k service.
"""
import pytest
import urllib.parse
from time import sleep

from simbad2k import simbad2k


@pytest.fixture(autouse=True)
def client():
    simbad2k.app.config['TESTING'] = True
    # Prevent throttling by MPC by waiting 1 second between queries
    sleep(1)

    with simbad2k.app.test_client() as client:
        yield client

    simbad2k.cache.clear()


class TestSimbad:
    def test_random_star(self, client):
        query  = 'HD 289002'
        expected_name = 'HD 289002'
        expected_ra = 101.30571895498
        expected_dec = 2.13741256961
        expected_paralax = 0.4561
        precision = 3 # make sure the position is good to ~1 arcsecond
        simbad_response = client.get(f'/{query}?target_type=sidereal').json
        assert simbad_response['name'] == expected_name
        assert round(simbad_response['ra'], precision) == round(expected_ra, precision)
        assert round(simbad_response['dec'], precision) == round(expected_dec, precision)
        assert round(simbad_response['plx_value'], precision) == round(expected_paralax, precision)


    def test_named_star(self, client):
        query  = 'Polaris'
        expected_name = '* alf UMi'
        expected_ra = 37.95456067018985
        expected_dec = 89.26410896994187
        expected_paralax = 7.54
        precision = 3 # make sure the position is good to ~1 arcsecond
        simbad_response = client.get(f'/{query}?target_type=sidereal').json
        assert simbad_response['name'] == expected_name
        assert round(simbad_response['ra'], precision) == round(expected_ra, precision)
        assert round(simbad_response['dec'], precision) == round(expected_dec, precision)
        assert round(simbad_response['plx_value'], precision) == round(expected_paralax, precision)


    def test_messier_object(self, client):
        query  = 'M55'
        expected_name = 'M  55'
        expected_ra = 294.9987916666667
        expected_dec = -30.96475
        expected_paralax = 0.209
        precision = 3  # make sure the position is good to ~1 arcsecond
        simbad_response = client.get(f'/{query}?target_type=sidereal').json
        assert simbad_response['name'] == expected_name
        assert round(simbad_response['ra'], precision) == round(expected_ra, precision)
        assert round(simbad_response['dec'], precision) == round(expected_dec, precision)
        assert round(simbad_response['plx_value'], precision) == round(expected_paralax, precision)


class TestMPC:
    def test_named_asteroid(self, client):
        query  = 'Ceres'
        scheme = 'mpc_minor_planet'
        expected_name = 'Ceres (1)'
        mpc_response = client.get(f'/{query}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_number_asteroid(self, client):
        query  = '2'
        scheme = 'mpc_minor_planet'
        expected_name = 'Pallas (2)'
        mpc_response = client.get(f'/{query}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_preliminary_number_asteroid_with_name(self, client):
        query  = '2000 AB3'
        scheme = 'mpc_minor_planet'
        expected_name = 'Albinocarbognani (27269)'
        mpc_response = client.get(f'/{query}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_preliminary_asteroid_with_no_number(self, client):
        query  = '2012 FN62'
        scheme = 'mpc_minor_planet'
        expected_name = '2012 FN62'
        mpc_response = client.get(f'/{query}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_preliminary_comet_with_no_name(self, client):
        query  = 'C/2020 F3'
        scheme = 'mpc_comet'
        expected_name = 'C/2020 F3'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_preliminary_comet_with_lower_case_type(self, client):
        query  = '13p'
        scheme = 'mpc_comet'
        expected_name = '13P'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_named_asteroid_with_same_name_as_moon(self, client):
        query  = 'Titania'
        scheme = 'mpc_minor_planet'
        expected_name = 'Titania (593)'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_asteroid_with_multiple_provisional_designations(self, client):
        query  = '2017 UH33'
        scheme = 'mpc_minor_planet'
        expected_name = '2017 SL83'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_asking_for_asteroid_using_comet_name(self, client):
        query  = '13P'
        scheme = 'mpc_minor_planet'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['error'] == 'No match found'

    def test_asking_for_asteroid_using_comet_name_for_dual_classified_object(self, client):
        query  = '133P'
        scheme = 'mpc_minor_planet'
        expected_name = 'Elst-Pizarro (7968)'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_comets_with_same_number_but_different_type(self, client):
        query  = '2P'
        scheme = 'mpc_comet'
        expected_name = '2P'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_comets_with_same_number_but_different_type_2(self, client):
        query  = '2I'
        scheme = 'mpc_comet'
        expected_name = '2I'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_comets_with_multiple_possible_targets(self, client):
        query  = 'shoemaker'
        scheme = 'mpc_comet'
        expected_name = '102P'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['name'] == expected_name

    def test_comets_being_interpreted_as_asteroid(self, client):
        query  = 'neowise'
        scheme = 'mpc_minor_planet'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['error'] == 'No match found'

    def test_giberish_name(self, client):
        query  = '1234notarock'
        scheme = 'mpc_minor_planet'
        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['error'] == 'No match found'

    def test_asteroid_name_for_comet_elements(self, client):
        query  = 'Vesta'
        scheme = 'mpc_comet'

        mpc_response = client.get(f'/{urllib.parse.quote_plus(query)}?target_type=non_sidereal&scheme={scheme}').json
        assert mpc_response['error'] == 'No match found'

class TestJPL:
    def test_jpl_moon(self, client):
        query  = '502'
        scheme = 'jpl_major_planet'
        jpl_response = client.get(f'/{query}?target_type=non_sidereal&scheme={scheme}').json
        assert 'Europa' in jpl_response.get('name', '')
        assert 'semimajor_axis' in jpl_response
        assert 'eccentricity' in jpl_response

    def test_jpl_minor_planet(self, client):
        query  = 'Ceres'
        scheme = 'jpl_minor_planet'
        jpl_response = client.get(f'/{query}?target_type=non_sidereal&scheme={scheme}').json
        assert 'Ceres' in jpl_response.get('name', '')
        assert 'perihelion_distance' in jpl_response


class TestNED:
    def test_ned_target(self, client):
        query  = 'WISEA%20J115959.34+120011.3'
        expected_name = 'WISEA J115959.34+120011.3'
        expected_dec = 12.00316
        expected_ra = 179.99728
        precision = 3 # make sure the position is good to ~1 arcsecond
        ned_response = client.get(f'/{query}?target_type=sidereal').get_json()
        assert ned_response['name'] == expected_name
        assert round(ned_response['ra_d'], precision) == round(expected_ra, precision)
        assert round(ned_response['dec_d'], precision) == round(expected_dec, precision)
