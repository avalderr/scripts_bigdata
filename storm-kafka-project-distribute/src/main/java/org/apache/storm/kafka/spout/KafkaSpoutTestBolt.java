/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpoutTestBolt extends BaseRichBolt {
    protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        System.out.print("input = [" + input + "]");
        /*
            LOG.debug("input = [" + input + "]");
            collector.ack(input);*/
        String sentence = input.getString(4);
        try {

            //String sentence1 = input.getStringByField("palabra");
            if (!isNullOrEmpty(sentence)) {
                System.out.println(sentence + "***");
                String[] keyValue = sentence.split(":");
                if (!isNullOrEmpty(keyValue[1]) && keyValue.length == 2) {
                    boolean numeric = true;
                    try {
                        Double num = Double.parseDouble(keyValue[1].trim().replace("]", ""));
                    } catch (NumberFormatException e) {
                        numeric = false;
                    }
                    //if (numeric) {
                    String values = keyValue[1].trim().replace("]", "");
                    collector.emit(new Values(keyValue[0].trim().replace("[", ""), values));
                    //} else {
                    //collector.fail(input);
                    //}

                } else {
                    collector.fail(input);
                }

                /*
                for (String w : words) {
                    collector.emit(new Values(w));
                }*/
            }

            //collector.ack(input);
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
            System.out.println(e + "sigue fallando");
        }

    }

    public static boolean isNullOrEmpty(String str) {
        if (str != null && !str.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "value"));
    }
}
