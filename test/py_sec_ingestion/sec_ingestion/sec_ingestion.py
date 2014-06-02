'''
Created on May 28, 2014

@author: Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>

Preconditions:
    * Tested in Python 2.7
    * Assuming a file "aws_credentials.json" in this directory defining with a
    JSON object with the following keys:

        "AWS_ACCESS_KEY_ID" - Your AWS Access Key ID
        "AWS_SECRET_ACCESS_KEY" - Your AWS Secret Access Key

Dependencies:
    - Requests: http://docs.python-requests.org/en/latest/index.html
    - Boto: http://docs.pythonboto.org/en/latest/s3_tut.html
'''

import requests, re, time, os, json, sys, logging, glob, shutil, smtplib
from datetime import datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.mime.application import MIMEApplication

'''
Configuration
'''
_bicing_url = "http://wservice.viabicing.cat/getstations.php?v=1"
_update_time_re = re.compile('<updatetime>\<!\[CDATA\[(?P<updatetime>\d+)\]\]></updatetime>')
_script_dir = os.path.dirname(os.path.realpath(__file__))
# _data_dir = os.path.join(_script_dir, 'data')
_data_dir = 'data' # use relative path for S3 keys to work ok, working directory is set below
_log_file = os.path.join(_script_dir, os.path.basename(__file__) + '.log')
_update_time_strf = "%Y-%m-%d_%H.%M.%S_UTC"
_aws_credentials_file = os.path.join(_script_dir, "aws_credentials.json")
_trg_s3_bucket = "juanrh.bicingbcn"
_trg_s3_key_prefix = 'data'
_admin_email = "juan.rodriguez.hortala@gmail.com"

# Use the script dir as current directory: this is needed for the
# S3 keys to work correctly
os.chdir(_script_dir)

# Initialize log
_logger = logging.getLogger(os.path.basename(__file__))
_logger.setLevel(logging.DEBUG)
# create file handler for warnings of worse
fh = logging.FileHandler(_log_file)
fh.setLevel(logging.WARNING)
# a console handler logs everything
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to _logger
_logger.addHandler(fh)
_logger.addHandler(ch)

# Connect to S3A
_aws_credentials = None
with open(_aws_credentials_file, 'r') as in_f:
    _aws_credentials = json.load(in_f)
_s3_conn = S3Connection(_aws_credentials["AWS_ACCESS_KEY_ID"], _aws_credentials["AWS_SECRET_ACCESS_KEY"])
_s3_bucket = _s3_conn.get_bucket(_trg_s3_bucket)

def store_file_to_s3(filename):
    '''
    Stores the local file at filename at the S3 bucket specified in the config
    variable _s3_bucket, using filename as the key

    :param path: local to this script directory of the file to be uploaded
    :type filename: string

    Example: store_file_to_s3("data/bicing_2014-05-31_15.53.07_UTC.xml")
    '''
    trg_key = Key(_s3_bucket, filename)
    def report_progress(bytes_transmitted, bytes_total):
        _logger.info(str(bytes_transmitted) + " / " + str(bytes_total) + " bytes transmitted to S3")
    with open(os.path.join(_script_dir,filename), 'r') as in_f:
        trg_key.set_contents_from_file(in_f, replace = True, cb = report_progress, num_cb = 3)

def get_bicing_xml(bicing_url=_bicing_url):
    '''
    Downloads the bicing xml data that is currently served at bicing_url, and return
    it as a string in a dictionary

    :returns: A dictionary with keys:
        - "updatetime": float with the updatetime specified in the downloaded xml (a Unix Epoch in UTC)
        - "data": the contents of the downloaded xml as an string

    :raises: RuntimeError in case the request fails
    '''
    bicing_req = requests.get(bicing_url)
    if bicing_req.status_code != 200:
        raise RuntimeError("Error requesting data from {url}, status code {status_code}".format(url=bicing_url, status_code=bicing_req.status_code))
    try:
        update_time_dict = _update_time_re.search(bicing_req.text).groupdict()
        update_time_dict['updatetime'] = float(update_time_dict['updatetime'])
    except Exception as e:
        raise RuntimeError("Error parsing update time " + str(e))
    return dict(update_time_dict, data = bicing_req.text)


def _send_email(sender, recipients, subject, smtp_host="localhost", body="", attachment_file=None):
    '''
    :param sender:
    :param recipients: string or list of strings with emails
    :param subject: email subject
    :param smtp_host: SMTP host to use to send the email
    :param body: message body
    :param attachment_file: file to attach to the message
    '''
    recipients_list = recipients if (type(recipients) is list) else [recipients]
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ','.join(recipients_list)
    msg.preamble = 'Multipart message.\n'
    msg.attach(MIMEText(body))
    if attachment_file != None:
        with open(attachment_file, 'r') as fp:
            attachment = MIMEApplication(fp.read())
        attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_file))
        msg.attach(attachment)
    try:
        s = smtplib.SMTP(smtp_host)
        s.ehlo()
        s.sendmail(sender, recipients_list, msg.as_string())
        s.quit()
    except smtplib.SMTPException as smtpe:
        sys.stderr.write("SMTPException occurred sending email:" + str(smtpe) + "\n")
    except Exception as e:
        sys.stderr.write("Exception occurred sending email: " + str(e) + "\n")

def _ensure_created_dir(dirpath):
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)

def _delete_glob(glob_pattern):
    '''
    Delete the files defined by glob_pattern.
    '''
    for path in glob.iglob(glob_pattern):
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors = True)
        else:
            os.remove(path)

_get_previous_updatetime_re = re.compile('bicing_(.*).xml')
def _get_previous_updatetime(data_dir):
    '''
    Returns the time string for the file at _data_dir (e.g. '2014-05-31_15.53.07_UTC'),
    or None if no file is present at that path

    Assumes at most one data file is present at _data_dir
    '''
    # FIXME: hardcoded filename pattern
    previuos_files = glob.glob(os.path.join(data_dir, 'bicing_*.xml'))
    if previuos_files == []:
        return None
    return _get_previous_updatetime_re.search(previuos_files[0]).group(1)

def _do_ingest_bicing_xml():
    '''
    Downloads a new XML file from the bicing service and uploads it to S3.
        To check if the file is new the last uploaded file is kept at _data_dir,
    and the updatetime is kept in the filename
        If the file downloaded is not new then nothing is done

    :returns: True if the process was successful, owise returns False
    '''
    global _data_dir, _bicing_url
    # Ensure local data dir is created
    try:
        _ensure_created_dir(_data_dir)
    except Exception as e:
        _logger.error("creating local data dir: " +  str(e))
        return False

    # Download file from the bicing service to a local file
    try:
        bicing_data = get_bicing_xml(_bicing_url)
        # NOTE: using UTC time for the update time
        updatetime = datetime.utcfromtimestamp(bicing_data['updatetime']).strftime(_update_time_strf)
        if updatetime == _get_previous_updatetime(_data_dir):
            # We have downloaded a file that was previously downloaded =>
            # do nothing and save some traffic to S3
            return True
        # New file, first delete the old one
            # FIXME: hardcoded filename pattern
        _delete_glob(os.path.join(_data_dir, 'bicing_*.xml'))
        # Now create the new file.
            # FIXME: hardcoded filename pattern
        trg_path = os.path.join(_data_dir, 'bicing_{updatetime}.xml'.format(updatetime=updatetime))
        with open(trg_path, 'w') as out_f:
            out_f.write(bicing_data['data'])
    except Exception as e:
        _logger.error("downloading file from bicing service dir: " +  str(e))
        return False

    # Upload file to S3
    try:
        store_file_to_s3(trg_path)
    except Exception as e:
        _logger.error("uploading bicing file to S3: " +  str(e))
        return False

    # We are done ok
    return True

def ingest_bicing_xml(times=3, period=1/3):
    '''
    To allow to schedule a polling frequency to the bicing service from crontab
    faster than one minute from crontab, this function performs the ingetion from
    bicing "times" with a "period" seconds between executions
    '''
    for _ in xrange(times):
        _logger.info("ingesting")
        _do_ingest_bicing_xml()
        time.sleep(period)

def estatimate_bicing_data_refresh_rate(trials=110, sleep_time=0.1, bicing_url=_bicing_url):
    '''
    In practice it looks this data is updated once each minute, therefore
    it would be enough pulling each 30 seconds, checking the last checkpoint
    to avoid replicated ingestions (and file transfers to S3)
    '''
    first_uptime = get_bicing_xml(bicing_url)['updatetime']
    for _ in xrange(trials):
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(sleep_time)
        new_uptime = get_bicing_xml(bicing_url)['updatetime']
        if first_uptime != new_uptime:
            return new_uptime - first_uptime
    # owise implicit return of None

def report_errors():
    '''
    If present, email log file to admin, and then delete it (to avoid reporting errors twice)
    '''
    # Must use os.stat because the log file is always created with zero size
    # by the logger
    if os.path.isfile(_log_file) and os.stat(_log_file).st_size > 0:
        _logger.info("Errors found, sending email to admin")
        _send_email(__file__, _admin_email,
                          "Bicing ingestion error report",
                          body = "Some errors occurred during ingestion, see attachment for details",
                          attachment_file = _log_file)

        os.remove(_log_file)

def heartbeat():
    '''
    Send an email to admin to acknowledge that the ingestion system is alive
    '''
    _send_email(__file__, _admin_email, "Bicing ingestion heartbeat", body = "I'm still alive!!!")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage:', sys.argv[0], '[estimate_refresh | ingest | report_errors | heartbeat]'
        sys.exit(1)

    action = sys.argv[1]
    if action == 'estimate_refresh':
        print os.linesep, 'Estimating bicing data refresh rate: ', str(estatimate_bicing_data_refresh_rate()), " seconds"
    elif action == 'ingest':
        ingest_bicing_xml()
    elif action == 'report_errors':
        report_errors()
    elif action == 'heartbeat':
        heartbeat()
