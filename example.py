from load_series import *
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

s=get_all_steps("trimmed_test")
s
df,h=mk_df_all_metrics('expand_sourcedb_ateam',"trimmed_test")
ddd=mk_host_metric_df_from_step_df(df,'proc.meminfo.committed_as',h)
final=mk_all_host_df_from_step_df(df,h,'trimmed_test')

