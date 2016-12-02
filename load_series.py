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
            if 'metrics' in line and 'exe' not in line:
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


def align_series_at_zero(series):
    """As each run starts at a different time, to put everything in 
       the same DataFrame, the indices need to align. This aligns all 
       runs to start at 0.00 seconds, BUT keeps their start time to allow
       extraction of global metrics
    """
    start_time = series.index[0]
    idx = series.index
    j = idx.to_julian_date()
    j = [ float("%09.2f" % ((i - j[0]) * 60 * 60 * 24)) for i in j]
    series.index = j
    s = series.groupby(series.index).first()  # Removes duplicates for concat
    return (s, start_time)


def make_single_metric_dataframe(list_s,list_t=None,key='timestamp'):
    '''takes in a list of all time series and concatenates them
       into a dataframe where the keys are a list of timestamps
    '''
    if not list_t:
        list_t = [i[1][key] for i in list_s] #TODO: No longer series(dict?)
        list_s = [i[0] for i in list_s]
    else:
        list_s = [i[0] for i in list_s]
        list_t = [i[key] for i in list_t]
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
        m = mk_df_from_step_metric(stepname, metric, trimmed_file)
        frames[metric]=m[0]
        h.append(m[1])
    final = pd.concat(frames, axis=1) 
    for m in h:
        for t in m:
            hosts[t[t.keys()[0]]] = t[t.keys()[1]]
    return final,hosts


def mk_df_from_step_metric(stepname,metric,trimmed_file):
    """Takes a step name, and combines all instances 
        of that step and one metric into a DataFrame
    """
    inst = get_all_step_instances(stepname, trimmed_file) #Check if instances are within time period
    series = []
    times = []
    hosts = []
    for step in inst:
        try:
            temp_s,temp_h = get_time_series(str(step) + "." + str(metric))
        except ValueError:
            continue
        if len(temp_s)>0:
            temp_hash = hash(str(temp_h['timestamp']) + str(temp_h['host']) + str(step))
            hosts.append(temp_h) # Create hash here
            series.append(align_series_at_zero(temp_s))
    df = make_single_metric_dataframe(series, hosts)
    return df,hosts


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
            run_length=max(step_df['io.syscw'][key].count())
        else:
            run_length=step_df['io.syscw'][key].count()
        list_ts.append(get_host_series(hostmetric,hostdict[key],key,run_length))
    return list_ts





#PLOTTING

def plot_length_step(df,statname='stats.stime'):
    """Plots the lengths of time the executables took to complete
       for a step
    """
    for s in df.keys()[0]:
        df[s].count().plot()
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


