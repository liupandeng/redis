package com.dream.common.redis.dao;

import org.apache.log4j.Logger;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

public class DBRedisPool implements IDBRedisPool{
	
	private static final Logger logger = Logger.getLogger(DBRedisPool.class);

	// 可用连接实例的最大数目，默认值为8；
	// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	private int MAX_ACTIVE = 20000; // 20000

	// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private int MAX_IDLE = 5000; //5000

	// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	private int MAX_WAIT = 100000; //5000

	private int TIMEOUT = 100000; //20000 

	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private boolean TEST_ON_BORROW = true;

	private JedisPool jedisPool = null;

	public DBRedisPool(String ip, String port, String password, String database){

		init(ip, port, password, database);

	}


	/**
	 * 初始化Redis连接池
	 */
	public void init(String ip, String port, String password, String database) {
		try {
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(MAX_ACTIVE);
			config.setMaxIdle(MAX_IDLE);
			config.setMaxWaitMillis(MAX_WAIT);
			config.setTestOnBorrow(TEST_ON_BORROW);
			config.setTestOnReturn(true);
			jedisPool = new JedisPool(config, ip, Integer.valueOf(port), TIMEOUT, password, Integer.valueOf(database));

		} catch (Exception e) {
			logger.error("error JedisPool init --------> "+e.getMessage());
		}
	}

	/** 
	 *  获取Jedis实例
	 *  @return Jedis
	 */
	public Jedis getConnection() throws Exception {
		try {
			if (jedisPool != null) {
				return jedisPool.getResource();
			} else {
				return null;
			}
		} catch (Exception e) {
			logger.error("error Jedis getConnection --------> "+e.getMessage());
			return null;
		}
	}

	/** 释放jedis资源  **/
	public void close(Jedis jedis) {
		
		if (jedis != null) {
			//jedisPool.returnResource(jedis);
			jedis.close();
		}
	}
	
	/** 释放jedis资源  
	 * @throws IOException **/
	public void close(Transaction tx) throws IOException {
		
		if (tx != null) {
			tx.close();
		}
	}
}