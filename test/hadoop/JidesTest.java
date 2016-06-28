package hadoop;

import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class JidesTest {
	
	@Test
	public void testJedis() {
		Jedis jedis = new Jedis("hadoop3");
		
		jedis.zadd("bbbbbbbbbbbb", 10, "b");

	}
	
	
}
