from load_series import *
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

s=get_all_steps("trimmed_test")
s
df=mk_df_all_metrics('expand_sourcedb_ateam',"trimmed_test")

