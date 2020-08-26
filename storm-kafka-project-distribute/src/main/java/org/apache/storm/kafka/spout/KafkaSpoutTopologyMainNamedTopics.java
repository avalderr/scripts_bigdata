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

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaProducerTopology;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

/**
 * This example sets up 3 topologies to put data in Kafka via the KafkaBolt,
 * and shows how to set up a topology that reads from some Kafka topics using the KafkaSpout.
 */
public class KafkaSpoutTopologyMainNamedTopics {

    private static final String TOPIC_2_STREAM = "test_2_stream";
    private static final String TOPIC_0_1_STREAM = "test_0_1_stream";
    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPIC_0 = "kafka-spout-test";
    public static final String TOPIC_1 = "kafka-spout-test-1";
    public static final String TOPIC_2 = "kafka-spout-test-2";

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String STORE_BOLT = "STORE_BOLT";

    private static final String TOPIC_BOLSA_0 = "TEC";
    private static final String TOPIC_BOLSA_1 = "CHEM";
    private static final String TOPIC_BOLSA_2 = "SERV";
    private static final String TOPIC_BOLSA_3 = "IND";
    private static final String TOPIC_BOLSA_4 = "RET";
    private static final String TOPIC_BOLSA_5 = "CAR";

    private static final String TEST_REDIS_HOST = /*"127.0.0.1"*/"localhost";
    private static final int TEST_REDIS_PORT = 6379;

    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainNamedTopics().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
        System.out.println("Running with broker url: " + brokerUrl);

        Config tpConf = getConfig();

        // Producers. This is just to get some data in Kafka, normally you would be getting this data from elsewhere
        //StormSubmitter.submitTopology(TOPIC_0 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_0));
        //StormSubmitter.submitTopology(TOPIC_1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_1));
        //StormSubmitter.submitTopology(TOPIC_2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_2));

        //Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
        StormSubmitter.submitTopology("storm-kafka-client-spout-test", tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setNumWorkers(8);
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt())
                //.shuffleGrouping("kafka_spout0", TOPIC_BOLSA_0)
                .shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
        //.shuffleGrouping("kafka_spout2", TOPIC_BOLSA_2)
        //.shuffleGrouping("kafka_spout3", TOPIC_BOLSA_3);
        //.shuffleGrouping("kafka_spout4", TOPIC_BOLSA_4)
        //.shuffleGrouping("kafka_spout5", TOPIC_BOLSA_5);
        //tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", TOPIC_2_STREAM);

        String host = TEST_REDIS_HOST;
        int port = TEST_REDIS_PORT;
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();
        RedisStoreMapper storeMapper = setupStoreMapper("bolsa");
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
        RedisMineBolt storageBolt = new RedisMineBolt(poolConfig, storeMapper);
        WordCounter bolt = new WordCounter();

        tp.setBolt(COUNT_BOLT, bolt, 1).fieldsGrouping("kafka_bolt", new Fields("company", "value"));
        //tp.setBolt("Price Cut Off", bolt, 1).fieldsGrouping("kafka_bolt", new Fields("company", "value"));
        //tp.setBolt(STORE_BOLT, storeBolt, 1).shuffleGrouping(COUNT_BOLT);
        tp.setBolt(STORE_BOLT, storeBolt, 1).shuffleGrouping(COUNT_BOLT);
        //tp.setBolt(STORE_BOLT + "testing", storageBolt, 1).shuffleGrouping(COUNT_BOLT);
        return tp.createTopology();
        /*
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt())
            .shuffleGrouping("kafka_spout", TOPIC_0_1_STREAM)
            .shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
        tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", TOPIC_2_STREAM);

        String host = TEST_REDIS_HOST;
        int port = TEST_REDIS_PORT;
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();
        RedisStoreMapper storeMapper = setupStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
        RedisMineBolt storageBolt = new RedisMineBolt(poolConfig, storeMapper);
        WordCounter bolt = new WordCounter();

        tp.setBolt(COUNT_BOLT, bolt, 1).fieldsGrouping("kafka_bolt_1", new Fields("palabra"));
        //tp.setBolt(STORE_BOLT, storeBolt, 1).shuffleGrouping(COUNT_BOLT);
        tp.setBolt(STORE_BOLT, storeBolt, 1).shuffleGrouping(COUNT_BOLT);
        //tp.setBolt(STORE_BOLT + "testing", storageBolt, 1).shuffleGrouping(COUNT_BOLT);
        return tp.createTopology();

         */
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
            (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
            new Fields("topic", "partition", "offset", "key", "value"), TOPIC_0_1_STREAM);
        trans.forTopic(TOPIC_2,
            (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
            new Fields("topic", "partition", "offset", "key", "value"), TOPIC_2_STREAM);
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{TOPIC_0, TOPIC_1, TOPIC_2})
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
            .setRetry(getRetryService())
            .setRecordTranslator(trans)
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
            TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    @org.jetbrains.annotations.NotNull
    @org.jetbrains.annotations.Contract(value = " -> new", pure = true)
    private static RedisStoreMapper setupStoreMapper(String hash) {
        return new WordCountStoreMapper(hash);
    }

    private static class WordCountStoreMapper implements RedisStoreMapper {
        private RedisDataTypeDescription description;
        private final String hashKey;

        WordCountStoreMapper(String hashKey) {
            this.hashKey = hashKey;
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            System.out.print("dentro Redis word" + tuple);
            return tuple.getString(0);
            //return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            System.out.print("dentro Redis count" + tuple);
            //return tuple.getString(1);
            return tuple.getStringByField("index").toString();
        }
    }


}
