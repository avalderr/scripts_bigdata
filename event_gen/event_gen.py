#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sched
import pandas as pd
import os
import subprocess
import time
import datetime
from sys import argv

arg2 = float(argv[1])
day_fraction = arg2 if arg2 <= 1 else 1/arg2
relative_speed = argv[2]

df = pd.read_csv(os.path.join('data_crunched','trade_data_01-03-2020.csv')).rename(columns={'Unnamed: 0':'ind'})
df = df.iloc[:int(df.shape[0]*day_fraction)]
df.loc[:,'TIME'] = pd.to_datetime(df.TIME)
df.loc[:,'delta'] = (df.TIME - df.iloc[0].TIME).apply(lambda x: x.seconds/float(relative_speed))

run_scripts = ['echo', '', '|', '~/kafka/bin/kafka-console-producer.sh', '--broker-list', 'localhost:9092', '--topic', '', '>', '/dev/null']

def exec_thing(string, topic):
    run_scripts[1] = string
    run_scripts[7] = topic
    print(run_scripts)
    # subprocess.Popen(['echo',string)])
    # subprocess.Popen([run_scripts]))
    return
    
def create_sched(s, x):
    string = str(x[['Topic','Mnemonic','Price']].values)
    s.enter(x.delta,x.ind,exec_thing,(string,x.Topic))
    return 

s = sched.scheduler(time.time, time.sleep)
# df.iloc[210:230].apply(lambda x: create_sched(s,x), axis=1)
df.apply(lambda x: create_sched(s,x), axis=1)

s.run()

