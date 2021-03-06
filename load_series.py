import os
import subprocess
import re
import opentsdb_pandas as odb
import pandas as pd
import datetime as dt

import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt



def get_metrics_file_remote(host, HBase_loc, filename):
    """Uses ssh and scp to get the list of metrics available
        using a hbase shell command. Copies the metric filename in
        the $PWD
        host: hostname that has HBASE running
        HBase_loc: Location of HBase1.2.3/bin on $host
    """
    HOST = host
    COMMAND = """cd %s ; export JAVA_HOME=/usr ; ./hbase shell ./scan > ~/%s
        """ % (HBase_loc, filename)
    ssh = subprocess.Popen(["ssh", "%s" % HOST, COMMAND],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    os.system("scp " + os.environ["USER"] + "@" + host + ":" + filename + " .")
    return str(os.environ["PWD"] + "/" + filename)


def strip_file(filename, trimmed_filename="metrics"):
    """The HBase Metric file from the tsdb-uid table contains two versions of
        every metric (for easy forw/back lookup) This strips the symbol->string
        version returning the metrics in the first column (also kept are tags)
    """
    filer = open(filename, 'r')
    filew = open(trimmed_filename, 'w')
    s = re.compile(r'^ \\x')
    for line in filer:
        if not re.search(s, line):
            filew.write(line)
    filer.close()
    filew.close()
    return str(os.environ["PWD"] + "/" + trimmed_filename)


def get_all_steps(filename):
    """gets all the pipeline steps
       The metrics are prefixed exe.[step_name].[PID].[statistic_metric]
    """
    stepnames = []
    with open(filename, 'r') as st_file:
        for line in st_file:
            if 'exe' in line:
                stepnames.append(line.split()[0].split('.')[1])
    return list(set(stepnames))

def get_host_metrics(filename):
    """Gets all the steps that are only host specific. IE all the steps
       that don't start with 'exe' .
    """
    h_steps = []
    with open(filename, 'r') as st_file:
        for line in st_file:
            if 'metrics' in line and 'exe' not in line and 'net' not in line and not 'tcollector' in line:
                h_steps.append(line.split()[0])
    return list(set(h_steps))


def get_all_step_instances(stepname, trimmed_filename):
    """Each step name has multiple instances. This function gets them all
        removes duplicates (that's what list(set(a)) does) and returns a list
        of unique steps to pull
    """
    step_instances = []
    with open(trimmed_filename, 'r') as st_file:
        for line in st_file:
            if 'exe' in line and line.split()[0].split('.')[1] == stepname:
                step_instances.append(".".join(line.split()[0].split('.')[:3]))
    return list(set(step_instances))


def get_all_metrics_for_step(stepname, trimmed_filename):
    """Given a step name, it returns a list of all metrics
        that follow this stepname inside the file. You can now query
        the TSDB with stepname+metric[1] for example
        TODO: Needs to differentiate between step and instance!
    """
    metrics = []
    with open(trimmed_filename, 'r') as st_file:
        for line in st_file:
            if 'exe' in line and stepname in line:
                metrics.append(".".join(line.split(stepname)[1].split()[0].split('.')[2:]))
    return list(set(metrics))


def get_time_series(metric_name):
    """Sends an HTTP query to the tsdb getting a single
        time_series corresponding to a metric
    """
    series = odb.ts_get(metric_name,
                        dt.datetime(2016, 1, 18, 14, 10),
                        dt.datetime(2016, 12, 24, 11, 30))
    return series


def align_series_at_zero(series,start=None):
    """As each run starts at a different time, to put everything in 
       the same DataFrame, the indices need to align. This aligns all 
       runs to start at 0.00 seconds, BUT keeps their start time to allow
       extraction of global metrics
    """
    idx = series.index
    j = idx.to_julian_date()
    start_time=j[0] if not start else pd.Timestamp(start).to_julian_date()
    j = [ float("%09.2f" % ((i - start_time) * 60 * 60 * 24)) for i in j]
    series.index = j
    s = series.groupby(series.index).first()  # Removes duplicates for concat
    return s[0:]


def make_single_metric_dataframe(list_s,list_t=None,key='timestamp'):
    '''takes in a list of all time series and concatenates them
       into a dataframe where the keys are a list of timestamps
    '''
    if not list_t:
        list_t = [i[1] for i in list_s] #TODO: No longer series(dict?)
        list_s = [i[0] for i in list_s]
    else:
        list_s = [i for i in list_s]
        list_t = [i for i in list_t]
    df = pd.concat(list_s, axis=1, keys=list_t)
    return df

def mk_df_all_metrics(stepname,trimmed_file):
    '''For a step, it makes a dataframe where each slice is
       a frame with different runs as columns and values each
       time period along the column
    '''
    ms = get_all_metrics_for_step(stepname, trimmed_file)
    frames = {}
    h = []
    hosts = {}
    for metric in ms:  #This may need multithreading
        print "Creating a frame for metric "+metric
        d,hos = mk_df_from_step_metric(stepname, metric, trimmed_file)
        frames[metric]=d
        h.append(hos)
    final = pd.concat(frames, axis=1) 
    for m in h:
        for t in m:
            hosts[t]=m[t]
    return final,hosts


def mk_df_from_step_metric(stepname,metric,trimmed_file):
    """Takes a step name, and combines all instances 
        of that step and one metric into a DataFrame
    """
    inst = get_all_step_instances(stepname, trimmed_file) #Check if instances are within time period
    series = []
    times = []
    hosts = []
    hostdict={}
    for step in inst:
        try:
            temp_s,temp_h = get_time_series(str(step) + "." + str(metric))
        except ValueError:
            continue
        if len(temp_s)>0:
            temp_hash = hash(str(temp_h['timestamp']) + str(temp_h['host']) + str(step))
            hosts.append(temp_hash) 
            hostdict[temp_hash]=temp_h #links a hash to metadata
            series.append(align_series_at_zero(temp_s))
    df = make_single_metric_dataframe(series, hosts)
    return df,hostdict


def get_host_series(metric_name,host,ts,tdelta):
    """Sends an HTTP query to the tsdb getting a single
        time_series corresponding to a (host) metric using a start datetime
        and a delta in seconds
    """
    series = odb.ts_get(metric_name,
                        ts,
                        ts+dt.timedelta(0,tdelta),tags=host)
    return series


def mk_host_metric_df_from_step_df(step_df,hostmetric,hostdict):
    list_ts=[]
    for key in step_df['mem.shr-pgs'].keys():
        if not isinstance(step_df['io.syscw'][key].count(),int):
            run_length=max(step_df['io.syscw'][key].count()) #used to fix multiplicate keys
        else:
            run_length=step_df['io.syscw'][key].count()
        try:
            tmp,d=get_host_series(hostmetric,hostdict[key]['host'],hostdict[key]['timestamp'],run_length)
            list_ts.append([align_series_at_zero(tmp,hostdict[key]['timestamp']),key])
        except ValueError:
            continue
    if len(list_ts)==0:
        return pd.DataFrame({'A' : [0]})
    df1=make_single_metric_dataframe(list_ts) 
    return df1



def mk_all_host_df_from_step_df(step_df, hostdict,trimmed_file):
    hs = get_host_metrics(trimmed_file)  
    frames = {}
    for host_metric in hs:
        print "Creating a frame for metric " + host_metric
        d = mk_host_metric_df_from_step_df(step_df,host_metric, hostdict)
        frames[host_metric]=d
    final = pd.concat(frames, axis=1)
    return final

#PLOTTING

def plot_length_step(df,statname='stat.stime'):
    """Plots the lengths of time the executables took to complete
       for a step
    """
    for s in df[statname].keys(): # Match timestamp and value to plot
        df[statname][s].count().plot()
    plt.title('Length of runs for the step')
    plt.xlabel('date of exe run')
    plt.ylabel('run length (s)')
    plt.show()
    ts=[]
    for i in df[statname].count():
        ts.append(i)
    plt.hist(ts,bins=50)
    plt.title("Histogram of all bbs-reducer runtimes")
    plt.xlabel("Length of run (s)")
    plt.ylabel("number of executions")
    plt.show()


