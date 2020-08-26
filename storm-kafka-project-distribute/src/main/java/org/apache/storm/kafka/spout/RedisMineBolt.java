package org.apache.storm.kafka.spout;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription.RedisDataType;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

public class RedisMineBolt extends AbstractRedisBolt {
    private final RedisStoreMapper storeMapper;
    private final RedisDataType dataType;
    private final String additionalKey;

    public RedisMineBolt(JedisPoolConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;
        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    public RedisMineBolt(JedisClusterConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;
        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    public void process(Tuple input) {
        String key = this.storeMapper.getKeyFromTuple(input);
        String value = this.storeMapper.getValueFromTuple(input);
        System.out.print(key + "clave-valor" + value);
        redis.clients.jedis.JedisCommands jedisCommand = null;

        try {
            jedisCommand = this.getInstance();
            System.out.print(key + "clave-valor-tipo" + value);
            jedisCommand.hset(this.additionalKey, key, value);
            System.out.print("entro-hash-prueba");
            /*
                case HYPER_LOG_LOG:
                    jedisCommand.pfadd(key, new String[]{value});
                    break;
                case GEO:
                    String[] array = value.split(":");
                    if (array.length != 2) {
                        throw new IllegalArgumentException("value structure should be longitude:latitude");
                    }

                    double longitude = Double.valueOf(array[0]);
                    double latitude = Double.valueOf(array[1]);
                    jedisCommand.geoadd(this.additionalKey, longitude, latitude, key);
                    break;
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + this.dataType);*/


            this.collector.ack(input);
        } catch (Exception var13) {
            this.collector.reportError(var13);
            this.collector.fail(input);
        } finally {
            this.returnInstance(jedisCommand);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
