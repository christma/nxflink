package nx.UV;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class RedisUtils implements Serializable {

    public static transient Jedis jedis;

    public RedisUtils() {
        jedis = new Jedis("localhost", 6379);
    }
}
