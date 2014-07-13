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
_district_to_neighborhoods = {
    u"Ciutat Vella" : [u'La Barceloneta', u'el Gòtic', u'el Raval', u'Sant Pere, Santa Caterina i la Ribera'], 
    u"Eixample" : [u"L'Antiga Esquerra de l'Eixample", u"la Nova Esquerra de l'Eixample", u"Dreta de l'Eixample", u'Fort Pienc', u'Barri de la Sagrada Família', u'Sant Antoni (Eixample)'],
    u"Sants-Montjuïc" : [u'La Bordeta (Sants-Montjuïc)', u'la Font de la Guatlla', u'Hostafrancs', u'La Marina de Port', u'La Marina del Prat Vermell', u'El Poble-sec (Sants-Montjuïc)', u'Sants', u'Sants-Badal', u'Montjuïc (Barcelona)', u'Zona Franca - Port'], 
    u"Les Corts" : [u'Barri de les Corts', u'La Maternitat i Sant Ramon', u'Pedralbes'], 
    u"Sarrià - Sant Gervasi" : [u'El Putget i Farró', u'Sarrià (Barcelona)', u'Sant Gervasi - la Bonanova', u'Sant Gervasi - Galvany', u'Les Tres Torres', u'Vallvidrera, el Tibidabo i les Planes'], 
    u"Gràcia" : [u'Vila de Gràcia', u"Camp d'en Grassot i Gràcia Nova", u'La Salut', u'El Coll', u'Vallcarca i els Penitents'], 
    u"Horta-Guinardó" : [u'El Baix Guinardó', u'El Guinardó', u'Can Baró', u'El Carmel', u"La Font d'en Fargues", u"Barri d'Horta", u'La Clota', u'Montbau', u'Sant Genís dels Agudells', u'La Teixonera', u"La Vall d'Hebron"], 
    u"Nou Barris" : [u'Can Peguera', u'Canyelles (Nou Barris)', u'Ciutat Meridiana', u'La Guineueta', u'Porta (Nou Barris)', u'La Prosperitat', u'Les Roquetes (Nou Barris)', u'Torre Baró', u'La Trinitat Nova', u'El Turó de la Peira', u'Vallbona (Nou Barris)', u'Verdum (Nou Barris)', u'Vilapicina i la Torre Llobeta'], 
    u"Districte de Sant Andreu" : [u'Baró de Viver', u'Bon Pastor (Sant Andreu)', u'El Congrés i els Indians', u'Navas', u'Sant Andreu de Palomar', u'La Sagrera', u'Trinitat Vella'], 
    u"Districte de Sant Martí" : [u'El Besós i el Maresme', u'El Clot', u"El Camp de l'Arpa del Clot", u'Diagonal Mar i el Front Marítim del Poblenou', u'El Parc i la Llacuna del Poblenou', u'Poblenou', u'Provençals del Poblenou', u'Sant Martí de Provençals', u'La Verneda i la Pau', u'La Vila Olímpica del Poblenou']}

_neighborhood_to_distric = { n : d for d, ns in _district_to_neighborhoods.iteritems()
                                   for n in ns}

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

        # try with wikipedia if needed
            # try by neighborhood
        if neighborhood == None:
            wikipedia_places = wikipedia.geosearch(latitude, longitude)
            for place in wikipedia_places:
                if place in _neighborhood_to_distric.keys():
                    neighborhood, district = place, _neighborhood_to_distric[place]
                    break
             # try by district
        if district == None:
            wikipedia_places = wikipedia_places if (wikipedia_places != None) else wikipedia.geosearch(longitude, latitude)
            for place in wikipedia_places:
                if place in _district_to_neighborhoods.keys():
                    district = place
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