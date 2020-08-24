#!/usr/bin/env python
# coding: utf-8
# python event_gen.py n_examples speed batch_size
# In[ ]:


import sched
import pandas as pd
import os
import subprocess
import time
import datetime
from sys import argv
from kafka import KafkaProducer

arg2 = int(argv[1])
#day_fraction = arg2 if arg2 <= 1 else 1/arg2
relative_speed = argv[2]
batch = int(argv[3])

df = pd.read_csv(os.path.join('data_crunched','trade_data_01-03-2020.csv'),nrows=10,skiprows=1, names=['ind','Mnemonic','Price','TIME','Topic'])#.rename(columns={'Unnamed: 0':'ind'})

#df = df.iloc[:int(df.shape[0]*day_fraction)]
df.loc[:,'TIME'] = pd.to_datetime(df.TIME)
start_time = df.iloc[0].TIME
prod = KafkaProducer(bootstrap_servers='localhost:9092')#,value_serializer=str.encode)

def exec_thing(string, topic,prod):
    print(str(string))
    prod.send(topic,str(string))
    return
    
def create_sched(s, x, prod):
    string = x[['Mnemonic','Price']].values
    s.enter(x.delta,x.ind,exec_thing,(string,x.Topic,prod))
    return 

s = sched.scheduler(time.time, time.sleep)
for i in range(int(arg2/batch)):
    skip = i*batch+1
    nrows = batch

    df = pd.read_csv(os.path.join('data_crunched','trade_data_01-03-2020.csv'),nrows=nrows,skiprows=skip, names=['ind','Mnemonic','Price','TIME','Topic'])#.rename(columns={'Unnamed: 0':'ind'})
    df.loc[:,'TIME'] = pd.to_datetime(df.TIME)
    df.loc[:,'delta'] = (df.TIME - start_time).apply(lambda x: x.seconds/float(relative_speed))

#df.iloc[210:230].apply(lambda x: create_sched(s,x), axis=1)

    df.apply(lambda x: create_sched(s,x,prod), axis=1)
lalal = raw_input("RUN NOW?")
s.run()
