"""

This module provides methods to fetch data from OpenTSDB HTTP interface and convert them into Python's Pandas Timeseries object.

2014.02, Tomasz Wiktorski, University of Stavanger, Norway
"""

import pandas as pd
import urllib2
import datetime as dt
import subprocess

def ts_get(metric, start, end, tags='', agg='avg', rate=False, downsample='', hostname='spui.grid.sara.nl', port=4242, trim=True):
  """
  This function returns a Python's Pandas Timeseries object with data fetched from OpenTSDB basing on the provided parameters.
  If there are no results it returns an empty Pandas Series, in case of any other exception it throws that exception.
  
  Parameters:
  metric - metric name as in OpenTSDB, one metric only, e.g. 'cipsi.test1.temperature'
  start, end - start and end time for the query, should be of type datetime from datetime module, e.g. dt.datetime(2013, 4, 3, 14, 10), assuming: import datetime as dt
  tags - tags formatted according to OpenTSDB specification e.g. 'host=foo,type=user|system'
  agg - aggregate function to be used, default is 'avg', options are min, sum, max, avg
  rate - specifies if rate should be calculated instead of raw data, default False
  downsample - specifies downsample function and interval in OpenTSDB format, default none, e.g. '60m-avg'
  trim - specifies if values received from OpneTSDB should be trimed to exactly match start and end parameters, OpenTSDB by default returns additional values before the start and after the end
  hostname - address of OpenTSB installation, default localhost
  port - port of OpenTSDB installation, default 4242
    
  Example usage:
  import opentsdb_pandas as opd
  import datetime as dt
  ts1 = opd.ts_get('cipsi.test1.temperature', dt.datetime(2013, 4, 3, 14, 10), dt.datetime(2013, 4, 10, 11, 30), 'node=0024C3145172746B', hostname='opentsdb.at.your.place.edu')
  ts1
  """
  url = "http://%s:%s/q?start=%s&end=%s&m=%s%s%s:%s{%s}&ascii" %(hostname,port,start.strftime("%Y/%m/%d-%H:%M:%S"),end.strftime("%Y/%m/%d-%H:%M:%S"), agg , ':'+downsample if downsample.strip() else '', ':rate' if rate else '', metric, tags)
  answer = urllib2.urlopen(url).read().strip()
  if answer:
    answer_by_line = answer.split('\n')
  else:
    return pd.Series()
  ti = [dt.datetime.fromtimestamp(int(x.split(' ')[1])) for x in answer_by_line]
  val = [float(x.split(' ')[2]) for x in answer_by_line]
  ts = pd.Series(val, ti)
  hst={}
  try:  #Rounding necessary when creating hash for several metrics of same step
    hst['timestamp']=pd.Timestamp(long(round(ti[0].value,-9)))
    hst['host']=x.split(' ')[3]
  except:
    pass
  if trim:
    ts = ts.ix[(ts.index >= start) & (ts.index <= end)]
  return ts,hst


def process_ts_list(tslist,trim=False):
    if not tslist:
        return pd.Series(),{}
    ti = [dt.datetime.fromtimestamp(int(x.split(' ')[1])/1000.) for x in tslist]
    val = [float(x.split(' ')[2]) for x in tslist]
    ts = pd.Series(val, ti)
    hst={}
    try:
        hst['timestamp']=pd.Timestamp(long(round(pd.Timestamp(ti[0]).value,-9)))
        hst['host']=x.split(' ')[3]
    except:
        pass
    if trim:
        ts = ts.ix[(ts.index >= start) & (ts.index <= end)]
    return ts,hst


def get_series_from_file(metric_name,out_file,tags=[]):
    results=None
    g=subprocess.Popen(["grep",metric_name,out_file],stdout=subprocess.PIPE)
    results=g.communicate()[0].split('\n')
    trimmed_results=[]
    for line in results: #TODO: filter by tags as well
        if 'put' in line and all(tag in line for tag in tags):
            trimmed_results.append(line.split('put ')[1])
    return trimmed_results


def dropcaches(hostname='localhost', port=4242):
  """
  This function drops caches in OpenTSDB. It returns True if caches were dropped and False otherwise.
  
  Parameters:
  hostname - address of OpenTSB installation, default localhost
  port - port of OpenTSDB installation, default 4242

  Example usage:
  import opentsdb_pandas as opd
  opd.ts_dropcaches
  """

  url = "http://%s:%s/dropcaches" %(hostname,port)
  answer = urllib2.urlopen(url).read().strip()
  return answer == 'Caches dropped.'
