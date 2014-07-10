#!/usr/bin/env python
# -*- coding: UTF-8 -*-

'''
What I need to do is called "reverse geocoding"
'''

'''
With pygeocoder: 
    - Site: http://code.xster.net/pygeocoder/wiki/Home
    - Installation:
(py27env)[cloudera@localhost ~]$ pip2.7 install pygeocoder

I obtain the postal code here
'''

'''
With geopy:
    - Site: 
        . http://code.google.com/p/geopy/
        . http://code.google.com/p/geopy/wiki/ReverseGeocoding

'''
import json
from pygeocoder import Geocoder
longitude, latitude = 41.3979520, 2.18004200
results = Geocoder.reverse_geocode(longitude, latitude)

print "Info for coordinates", longitude, latitude, ":\n\t", results
# This where we have the raw info
print json.dumps(results.__dict__, indent=2)

'''
We obtain information about the district, postal code, and neighborhood acccording
to its original denomination in Catalá http://ca.wikipedia.org/wiki/Districtes_i_barris_de_Barcelona 

The point is that the result object is iterable, and each iteration "zooms out" a step, 
showing information for address, neighborhood, zip code, district, ... and then zooms 
too far to be interesting given our focus in Barcelona

>>> for r in results:
...    print r
... 
Gran Via de les Corts Catalanes, 760, 08013 Barcelona, Spain
El Fort Pienc, Barcelona, Spain
08013 Barcelona, Spain
Eixample, Barcelona, Spain
Barcelona, Spain
Barcelona, Spain
El Barcelonès, Spain
Barcelona, Spain
Catalonia, Spain
Spain
'''
for r in results:
    print "\t-", r

'''
TODO: enrich the dimension table with neighborhood, zip code and district, 
and also with additional demographic information from Wikipedia using https://pypi.python.org/pypi/wikipedia/
Do this with a python script that parses a XML bicing file and prints SQL insert statements for phoenix
to stdout
'''


'''
{
  "current_index": 0, 
  "data": [
    {
      "geometry": {
        "location": {
          "lat": 41.39797129999999, 
          "lng": 2.1800415
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.39932028029149, 
            "lng": 2.181390480291502
          }, 
          "southwest": {
            "lat": 41.39662231970849, 
            "lng": 2.178692519708498
          }
        }, 
        "location_type": "ROOFTOP"
      }, 
      "address_components": [
        {
          "long_name": "760", 
          "types": [
            "street_number"
          ], 
          "short_name": "760"
        }, 
        {
          "long_name": "Gran Via de les Corts Catalanes", 
          "types": [
            "route"
          ], 
          "short_name": "Gran Via de les Corts Catalanes"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "locality", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalu\u00f1a, Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "Catalu\u00f1a, Catalonia"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }, 
        {
          "long_name": "08013", 
          "types": [
            "postal_code"
          ], 
          "short_name": "08013"
        }
      ], 
      "formatted_address": "Gran Via de les Corts Catalanes, 760, 08013 Barcelona, Spain", 
      "types": [
        "street_address"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 41.4030049, 
            "lng": 2.1868999
          }, 
          "southwest": {
            "lat": 41.391278, 
            "lng": 2.173317200000001
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.4030049, 
            "lng": 2.1868999
          }, 
          "southwest": {
            "lat": 41.391278, 
            "lng": 2.173317200000001
          }
        }, 
        "location": {
          "lat": 41.3981098, 
          "lng": 2.181871000000001
        }
      }, 
      "address_components": [
        {
          "long_name": "El Fort Pienc", 
          "types": [
            "neighborhood", 
            "political"
          ], 
          "short_name": "El Fort Pienc"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "locality", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_4", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "El Fort Pienc, Barcelona, Spain", 
      "types": [
        "neighborhood", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 41.4086971, 
            "lng": 2.1870311
          }, 
          "southwest": {
            "lat": 41.3921201, 
            "lng": 2.1697776
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.4086971, 
            "lng": 2.1870311
          }, 
          "southwest": {
            "lat": 41.3921201, 
            "lng": 2.1697776
          }
        }, 
        "location": {
          "lat": 41.4031163, 
          "lng": 2.181871000000001
        }
      }, 
      "address_components": [
        {
          "long_name": "08013", 
          "types": [
            "postal_code"
          ], 
          "short_name": "08013"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "locality", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "08013 Barcelona, Spain", 
      "types": [
        "postal_code"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 41.412005, 
            "lng": 2.1868999
          }, 
          "southwest": {
            "lat": 41.374956, 
            "lng": 2.1424718
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.412005, 
            "lng": 2.1868999
          }, 
          "southwest": {
            "lat": 41.374956, 
            "lng": 2.1424718
          }
        }, 
        "location": {
          "lat": 41.391843, 
          "lng": 2.1641969
        }
      }, 
      "address_components": [
        {
          "long_name": "Eixample", 
          "types": [
            "sublocality_level_1", 
            "sublocality", 
            "political"
          ], 
          "short_name": "Eixample"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "locality", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_4", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "Eixample, Barcelona, Spain", 
      "types": [
        "sublocality_level_1", 
        "sublocality", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 41.4695761, 
            "lng": 2.2280099
          }, 
          "southwest": {
            "lat": 41.320004, 
            "lng": 2.0695258
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.4695761, 
            "lng": 2.2280099
          }, 
          "southwest": {
            "lat": 41.320004, 
            "lng": 2.0695258
          }
        }, 
        "location": {
          "lat": 41.3850639, 
          "lng": 2.1734035
        }
      }, 
      "address_components": [
        {
          "long_name": "Barcelona", 
          "types": [
            "locality", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_4", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "Barcelona, Spain", 
      "types": [
        "locality", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 41.46794269999999, 
            "lng": 2.2280099
          }, 
          "southwest": {
            "lat": 41.320004, 
            "lng": 2.0524976
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.46794269999999, 
            "lng": 2.2280099
          }, 
          "southwest": {
            "lat": 41.320004, 
            "lng": 2.0524976
          }
        }, 
        "location": {
          "lat": 41.3850494, 
          "lng": 2.1733247
        }
      }, 
      "address_components": [
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_4", 
            "political"
          ], 
          "short_name": "Barcelona"
        }, 
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "Barcelona, Spain", 
      "types": [
        "administrative_area_level_4", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 41.4929026, 
            "lng": 2.270536
          }, 
          "southwest": {
            "lat": 41.3200453, 
            "lng": 2.0529518
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 41.4929026, 
            "lng": 2.270536
          }, 
          "southwest": {
            "lat": 41.3200453, 
            "lng": 2.0529518
          }
        }, 
        "location": {
          "lat": 41.4016668, 
          "lng": 2.1253854
        }
      }, 
      "address_components": [
        {
          "long_name": "El Barcelon\u00e8s", 
          "types": [
            "administrative_area_level_3", 
            "political"
          ], 
          "short_name": "El Barcelon\u00e8s"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "El Barcelon\u00e8s, Spain", 
      "types": [
        "administrative_area_level_3", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 42.32330109999999, 
            "lng": 2.7777843
          }, 
          "southwest": {
            "lat": 41.1927452, 
            "lng": 1.3596215
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 42.32330109999999, 
            "lng": 2.7777843
          }, 
          "southwest": {
            "lat": 41.1927452, 
            "lng": 1.3596215
          }
        }, 
        "location": {
          "lat": 41.3850477, 
          "lng": 2.1733131
        }
      }, 
      "address_components": [
        {
          "long_name": "Barcelona", 
          "types": [
            "administrative_area_level_2", 
            "political"
          ], 
          "short_name": "B"
        }, 
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "Barcelona, Spain", 
      "types": [
        "administrative_area_level_2", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 42.8614502, 
            "lng": 3.3325445
          }, 
          "southwest": {
            "lat": 40.5230466, 
            "lng": 0.1591811
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 42.8614502, 
            "lng": 3.3325445
          }, 
          "southwest": {
            "lat": 40.5230466, 
            "lng": 0.1591811
          }
        }, 
        "location": {
          "lat": 41.5911589, 
          "lng": 1.5208624
        }
      }, 
      "address_components": [
        {
          "long_name": "Catalonia", 
          "types": [
            "administrative_area_level_1", 
            "political"
          ], 
          "short_name": "CT"
        }, 
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "Catalonia, Spain", 
      "types": [
        "administrative_area_level_1", 
        "political"
      ]
    }, 
    {
      "geometry": {
        "location_type": "APPROXIMATE", 
        "bounds": {
          "northeast": {
            "lat": 43.7923795, 
            "lng": 4.3277839
          }, 
          "southwest": {
            "lat": 27.6377894, 
            "lng": -18.160788
          }
        }, 
        "viewport": {
          "northeast": {
            "lat": 45.244, 
            "lng": 5.098
          }, 
          "southwest": {
            "lat": 35.17300000000001, 
            "lng": -12.524
          }
        }, 
        "location": {
          "lat": 40.46366700000001, 
          "lng": -3.74922
        }
      }, 
      "address_components": [
        {
          "long_name": "Spain", 
          "types": [
            "country", 
            "political"
          ], 
          "short_name": "ES"
        }
      ], 
      "formatted_address": "Spain", 
      "types": [
        "country", 
        "political"
      ]
    }
  ], 
  "current_data": {
    "geometry": {
      "location": {
        "lat": 41.39797129999999, 
        "lng": 2.1800415
      }, 
      "viewport": {
        "northeast": {
          "lat": 41.39932028029149, 
          "lng": 2.181390480291502
        }, 
        "southwest": {
          "lat": 41.39662231970849, 
          "lng": 2.178692519708498
        }
      }, 
      "location_type": "ROOFTOP"
    }, 
    "address_components": [
      {
        "long_name": "760", 
        "types": [
          "street_number"
        ], 
        "short_name": "760"
      }, 
      {
        "long_name": "Gran Via de les Corts Catalanes", 
        "types": [
          "route"
        ], 
        "short_name": "Gran Via de les Corts Catalanes"
      }, 
      {
        "long_name": "Barcelona", 
        "types": [
          "locality", 
          "political"
        ], 
        "short_name": "Barcelona"
      }, 
      {
        "long_name": "Barcelona", 
        "types": [
          "administrative_area_level_2", 
          "political"
        ], 
        "short_name": "B"
      }, 
      {
        "long_name": "Catalu\u00f1a, Catalonia", 
        "types": [
          "administrative_area_level_1", 
          "political"
        ], 
        "short_name": "Catalu\u00f1a, Catalonia"
      }, 
      {
        "long_name": "Spain", 
        "types": [
          "country", 
          "political"
        ], 
        "short_name": "ES"
      }, 
      {
        "long_name": "08013", 
        "types": [
          "postal_code"
        ], 
        "short_name": "08013"
      }
    ], 
    "formatted_address": "Gran Via de les Corts Catalanes, 760, 08013 Barcelona, Spain", 
    "types": [
      "street_address"
    ]
  }, 
  "len": 10
}

'''

'''
More with (py27env)[cloudera@localhost OLAP]$ pip2.7 search geo


Interesting:
    geonode-arcrest           - Wrapper to the ArcGIS REST API, and a Python analogue to the Javascript
                            APIs. Modified to have a version number more pypi friendly by the
                            GeoNode team.

    smartystreets             - An easy-to-use SmartyStreets Geocoding API wrapper. <-- Only USA
               
'''