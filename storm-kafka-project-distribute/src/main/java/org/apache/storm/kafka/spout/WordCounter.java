/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;



public class WordCounter implements IBasicBolt {
    private Map<String, Double> companyIndex = Maps.newHashMap();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("Inside WordCounter " + input);
        List<Object> words = input.getValues();
        System.out.println("Adentro de words " + words);
        String company = input.getStringByField("company");
        String [] index = new String[]{"min", "max", "count"};
        boolean numeric = true;

        try {
            //Double tuplevalue = Double.parseDouble(input.getStringByField("value").toString());
            Double tuplevalue = Double.parseDouble((String) words.get(1));
        } catch (NumberFormatException e) {
            numeric = false;
        }
        if (numeric) {
            Double tuplevalue = Double.parseDouble((String) words.get(1));
            //Double tuplevalue = Double.valueOf(input.getStringByField("value").toString());
            System.out.println("palabra " + company);
            Double value = Double.valueOf(0);
            String cindex;
            for (String w : index) {
                cindex = company + "-" + w;
                if (companyIndex.containsKey(cindex)) {
                    value = companyIndex.get(cindex).doubleValue();
                    switch (w) {
                        case "min":
                            if (tuplevalue < value) {
                                value = tuplevalue;
                            }
                            // code block
                            break;
                        case "max":
                            if (tuplevalue > value) {
                                value = tuplevalue;
                            }
                            // code block
                            break;
                        case "count":
                            value++;
                            // code block
                            break;
                        default:
                    }
                    companyIndex.put(company, value + Double.valueOf(1));
                } else {
                    if (w == "count") {
                        value = Double.valueOf(1);
                    } else {
                        value = tuplevalue;
                    }
                }
                System.out.println(value + "counting" + words);
                companyIndex.put(cindex, value);
                collector.emit(new Values(cindex, String.valueOf(value)));
            }
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "index"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
