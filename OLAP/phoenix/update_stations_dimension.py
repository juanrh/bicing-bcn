#!/usr/bin/env python
# -*- coding: UTF-8 -*-

'''
Tested in Python 2.7.5

Dependencies:
    * lxml
        - Site: http://lxml.de/tutorial.html
        - Installation:

(py27env)[cloudera@localhost bicing-bcn]$ sudo yum install python-lxml.x86_64
<-- that installs it for python2.6 omming with the system

For python 2.7 installed from sources in CentOS 6 (http://stackoverflow.com/questions/5178416/pip-install-lxml-error), we first need to manually install some dependencies:

(py27env)[cloudera@localhost bicing-bcn]$ sudo yum install libxslt-python.x86_64 libxslt-devel.x86_64 libxslt-devel.i686 libxml2-devel.x86_64 libxml2-devel.i686 libxml2-python.x86_64

    (py27env)[cloudera@localhost bicing-bcn]$ sudo pip2.7 install lxml

    * pygeocoder: 
        - Site: http://code.xster.net/pygeocoder/wiki/Home. See https://github.com/rapidfat/pygeocoder/blob/master/pygeocoder.py for a more detailed "documentation"
        - Installation:
(py27env)[cloudera@localhost ~]$ pip2.7 install pygeocoder

    * wikipedia:
        - Site: https://pypi.python.org/pypi/wikipedia/
        - Installation:
(py27env)[cloudera@localhost phoenix]$ sudo pip2.7 install wikipedia

    * BeautifulSoup
        - Site: http://www.crummy.com/software/BeautifulSoup/, see tutorial from http://adesquared.wordpress.com/2013/06/16/using-python-beautifulsoup-to-scrape-a-wikipedia-table/
        - Installation: 
(py27env)[cloudera@localhost phoenix]$ sudo pip2.7 install beautifulsoup4
'''

import os, re, sys
from lxml import etree
from pygeocoder import Geocoder
import wikipedia
from bs4 import BeautifulSoup

script_dir = os.path.realpath(os.path.dirname(__file__))
default_bicing_file = os.path.join(script_dir, "bicing_2014-05-31_15.53.07_UTC.xml")

def lxml_tests():
    with open(path, 'r') as in_f:
        tree = etree.parse(in_f)
        # FIXME
        root = tree.getroot()
        print root.tag
            # print the contents
        print root[0].tag, "text =", root[0].text
            # no text, only childs
        print root[1].tag, "text =", root[1].text
        print root[1].tag, "=>", station_element_to_dict(root[1])

        print root[-1].tag, "=>", root[-1][0].tag, '=', root[-1][0].text
        station_ids = sorted((int(child[0].text) for child in root[1:]))
        print len(station_ids), 'station ids:', station_ids
        # prints whole tree
        print etree.tostring(root, pretty_print = True)

def station_element_to_dict(station_element):
    '''
    :param station_element: representation in lxml of a <station> element
    :type station_element: lxml.Element
    :returns a dictionary from each subelement tag to its value as string. This makes sense because <station> tags nesting stops at the child level

    Example: for station_element representing the following XML fragment

     <station>
        <id>1</id>
        <lat>41.3979520</lat>
        <long>2.18004200</long>
        <street><![CDATA[Gran Via Corts Catalanes]]></street>
        <height>21</height>
         <streetNumber>760</streetNumber>
         <nearbyStationList>24,369,387,426</nearbyStationList>
         <status>OPN</status>
         <slots>18</slots>
         <bikes>6</bikes>
    </station>

    this function returns

    {'status': 'OPN', 'bikes': '6', 'long': '2.18004200', 'height': '21', 'street': 'Gran Via Corts Catalanes', 'nearbyStationList': '24,369,387,426', 'streetNumber': '760', 'lat': '41.3979520', 'slots': '18', 'id': '1'}
    '''
    return {child.tag : child.text for child in station_element.getchildren()}

def parse_bicing_stations_from_file(path=default_bicing_file):
    '''
    :param path to a bicing data XML file
    :type path: string
    :returns an iterator that for each <station> in path returns a dictionary with the format returned by station_element_to_dict(), i.e., from string to string with the keys the tag of the children of a station element and the values the text for that child elements

    Check with 
    >>> print ("\n"*2).join(map(str,parse_bicing_stations_from_file()))
    '''
    with open(path, 'r') as in_f:
        tree = etree.parse(in_f)
        root = tree.getroot()
        # We don't need updatetime here
        # updatetime = root[0].text
        return (station_element_to_dict(station_element) for station_element in root[1:])

# Assuming Wikipedia results for Barcelona wikin Català will be more accurate
# NOTE: this is fundamental for the assumptions made for using BeautifulSoup
wikipedia.set_lang('ca')

def enrich_stations(station):
    return (enrich_station(station) for station in stations)

_postalcode_re = re.compile('.* (?P<postalcode>\d+) Barcelona, Spain')
_size_soup_re = re.compile("\s*(?P<size>\d+,\d+)")
_population_and_density_soup_re = re.compile("\s*(?P<population>\d+,\d+)\D+(?P<density>\d+(\.\d+)?,\d+)")

# according to the name currenly used in wikipedia
_district_to_neighbourhoods = {
    "Ciutat Vella de Barcelona" : ['La Barceloneta', 'el Gòtic', 'el Raval', 'Sant Pere, Santa Caterina i la Ribera'], 
    "Eixample de Barcelona" : ["L'Antiga Esquerra de l'Eixample", "la Nova Esquerra de l'Eixample", "Dreta de l'Eixample", 'Fort Pienc', 'Barri de la Sagrada Família', 'Sant Antoni (Eixample)'],
    "Sants-Montjuïc" : ['La Bordeta (Sants-Montjuïc)', 'la Font de la Guatlla', 'Hostafrancs', 'La Marina de Port', 'La Marina del Prat Vermell', 'El Poble-sec (Sants-Montjuïc)', 'Sants', 'Sants-Badal', 'Montjuïc (Barcelona)', 'Zona Franca - Port'], 
    "Les Corts" : ['Barri de les Corts', 'La Maternitat i Sant Ramon', 'Pedralbes'], 
    "Sarrià - Sant Gervasi" : ['El Putget i Farró', 'Sarrià (Barcelona)', 'Sant Gervasi - la Bonanova', 'Sant Gervasi - Galvany', 'Les Tres Torres', 'Vallvidrera, el Tibidabo i les Planes'], 
    "Gràcia" : ['Vila de Gràcia', "Camp d'en Grassot i Gràcia Nova", 'La Salut', 'El Coll', 'Vallcarca i els Penitents'], 
    "Horta-Guinardó" : ['El Baix Guinardó', 'El Guinardó', 'Can Baró', 'El Carmel', "La Font d'en Fargues", "Barri d'Horta", 'La Clota', 'Montbau', 'Sant Genís dels Agudells', 'La Teixonera', "La Vall d'Hebron"], 
    "Nou Barris" : ['Can Peguera', 'Canyelles (Nou Barris)', 'Ciutat Meridiana', 'La Guineueta', 'Porta (Nou Barris)', 'La Prosperitat', 'Les Roquetes (Nou Barris)', ' Torre Baró', 'La Trinitat Nova', 'El Turó de la Peira', 'Vallbona (Nou Barris)', 'Verdum (Nou Barris)', 'Vilapicina i la Torre Llobeta'], 
    "Districte de Sant Andreu" : ['Baró de Viver', 'Bon Pastor (Sant Andreu)', 'el Congrés i els Indians', 'Navas', 'Sant Andreu de Palomar', 'La Sagrera', 'Trinitat Vella'], 
    "Districte de Sant Martí" : ['El Besós i el Maresme', 'El Clot', "Wl Camp de l'Arpa del Clot", 'Diagonal Mar i el Front Marítim del Poblenou', 'El Parc i la Llacuna del Poblenou', 'Poblenou', 'Provençals del Poblenou', 'Sant Martí de Provençals', 'La Verneda i la Pau', 'La Vila Olímpica del Poblenou']}

def enrich_station(station):
    '''
    :param station: dictionary in the format returned by station_element_to_dict(), i.e., from string to string with the keys the tag of the children of a station element and the values the text for that child elements

    .retuns TODO the dictionary enriched with additional info
    '''
    def get_geo_info(station):
        '''
        FIXME: some stations get None for district and neighborhood, maybe could be fixed by some algortithm that moves the point around until some non None result is obtained for both. See wikipedia also approach above

        TODO: the wikipedia approach could be improved by replacing exact matching by approximate matching, maybe with NLTK
        '''
        longitude, latitude = float(station['long']), float(station['lat'])
        # reverse geocoding
        # NOTE Geocoder.reverse_geocode returns a kind of iterator 
        # with objects that can only be consumed once, beware
        district, neighborhood, postalcode = None, None, None
        for result in Geocoder.reverse_geocode(latitude, longitude):
            district = result.sublocality if (district == None) else district
            neighborhood = result.neighborhood if (neighborhood == None) else neighborhood
            if postalcode == None:
                postalcode_match = _postalcode_re.match(str(result))
                postalcode = postalcode_match.groupdict()['postalcode'] if (postalcode_match != None) else None
            if (filter(lambda x: x == None, [district, neighborhood, postalcode]) == []):
                break
        geo_info = {"district" : district, "neighborhood" : neighborhood, "postalcode" : postalcode}

        return geo_info

    def normalize_float_string(fstr):
        return str(float(fstr.replace(".", "").replace(",", ".")))

    def get_wikipedia_info(district):
        '''
        :param district: district name as computed by get_geo_info
        '''
        if district == None:
            return {"size" : None, "population" : None, "density" : None}

        def get_wikipedia_info_for_page(page):
            soup = BeautifulSoup(page.html())
            try: 
                # instead of some defensive mechanism that will only get partial coverage, 
                # assume everything is fine and otherwise assume we know nothing
                soup_result = list((soup.find_all(href="/wiki/Superf%C3%ADcie") + soup.find_all(title="Superfície"))[0].parent.parent.children)[-1].text
                size = normalize_float_string(_size_soup_re.match(soup_result).groupdict()['size'])
            except:
                size = None
            try: 
                # instead of some defensive mechanism that will only get partial coverage, 
                # assume everything is fine and otherwise assume we know nothing
                soup_result = list((soup.find_all(href="/wiki/Poblaci%C3%B3") + soup.find_all(title="Població"))[0].parent.parent.children)[-1].text
                population_and_density_dict = _population_and_density_soup_re.match(soup_result).groupdict()
                population = normalize_float_string(population_and_density_dict["population"])
                density = normalize_float_string(population_and_density_dict["density"])
            except:
                population, density = None, None

            return {"size" : size, "population" : population, "density" : density}

        # adding "Barcelona to help with disambiguation"
        for page_name in wikipedia.search(district + " Barcelona"):
            try:
                page = wikipedia.page(page_name, auto_suggest=False)
            except wikipedia.exceptions.DisambiguationError as de:
                # FIXME: very simple heuristic
                page = wikipedia.page(de.options[0])
            info = get_wikipedia_info_for_page(page)
            if (info["size"] != None and info["population"] != None and info["density"] != None):
                return info

        return {"size" : None, "population" : None, "density" : None}

    station.update(get_geo_info(station))
    station.update(get_wikipedia_info(station["district"]))

    sys.stderr.write('station ' + station['id'] + " updated : " + str(station) + "\n"*2)

    return station
    
if __name__ == '__main__':
    stations = parse_bicing_stations_from_file()
    # stations_list = list(stations)
    # print stations_list[9]
    # print enrich_station(stations_list[9])

    print ("\n"*2).join(map(str, list(enrich_stations(stations))))

    # print ("\n"*2).join(map(str,parse_bicing_stations_from_file()))

    # get_geo_info for station 10: {'postalcode': '08003', 'neighborhood': None, 'district': None}

    # print _getDistrictInfo()

'''

>>> print Geocoder.geocode('Carrer del Comerç 27, Barcelona, Spain')
Carrer del Comerç, 27, 08003 Barcelona, Spain

'''