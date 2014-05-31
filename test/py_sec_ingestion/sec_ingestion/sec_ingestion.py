'''
Created on May 28, 2014

@author: Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>

Tested in Python 2.7

TODO: add dependencies install to installation list pip style

$ sudo pip2.7 install requests
$ sudo pip2.7 install boto

TODO: proper comments

Dependencies:
    - Requests: http://docs.python-requests.org/en/latest/index.html
    - Boto
'''

import requests, re, time, os
from datetime import datetime

'''
Configuration
'''
_bicing_url = "http://wservice.viabicing.cat/getstations.php?v=1"
_update_time_re = re.compile('<updatetime>\<!\[CDATA\[(?P<updatetime>\d+)\]\]></updatetime>')
_script_dir = os.path.dirname(os.path.realpath(__file__))
_data_dir = os.path.join(_script_dir, 'data')
_update_time_strf = "%Y-%m-%d_%H.%M.%S_UTC"

def get_bicing_xml():
    bicing_req = requests.get(_bicing_url)
    if bicing_req.status_code != 200:
        raise RuntimeError("Error requesting data from {url}, status code {status_code}".format(url=_bicing_url, status_code=bicing_req.status_code))
    try:
        update_time_dict = _update_time_re.search(bicing_req.text).groupdict()
        update_time_dict['updatetime'] = float(update_time_dict['updatetime'])
    except Exception as e:
        raise RuntimeError("Error parsing update time " + str(e))
    return dict(update_time_dict, data = bicing_req.text)

def _ensure_created_dir(dirpath):
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)

def ingest_bicing_xml():
    '''
    Several TODOs
    1. Download bicing xml
    2. Read checkpoint file and discard if the file was already downloaded
    3. Owise write to disk, upload to S3, delete file and update checkpoint file
    '''
    _ensure_created_dir(_data_dir)
    bicing_data = get_bicing_xml()
    # TODO checkpoint and so
    # Use UTC time
    updatetime = datetime.utcfromtimestamp(bicing_data['updatetime']).strftime(_update_time_strf)
    trg_path = os.path.join(_data_dir, 'bicing_{updatetime}.xml'.format(updatetime=updatetime))
    with open(trg_path, 'w') as out_f:
        out_f.write(bicing_data['data'])

def estatimate_bicing_data_refresh_rate(trials=110, sleep_time=0.1):
    first_uptime = get_bicing_xml()['updatetime']
    for _ in xrange(trials):
        time.sleep(sleep_time)
        new_uptime = get_bicing_xml()['updatetime']
        if first_uptime != new_uptime:
            return new_uptime - first_uptime
    # owise implicit return of None

if __name__ == '__main__':
    bicing_data = get_bicing_xml()
    print 'data:', bicing_data['data']
    print 'updatetime:', datetime.fromtimestamp(bicing_data['updatetime'])

    # In practice it looks this data is updated each minute, around second 06 =>
    # it would be enough pulling each 30 seconds, checking the last checkpoint
    # to avoid replicated ingestions
#     print 'estimated bicing data refresh rate: ', str(estatimate_bicing_data_refresh_rate()), " seconds"

    print 'ingesting data'
    ingest_bicing_xml()
    print 'done'