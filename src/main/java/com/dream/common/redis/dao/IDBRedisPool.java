package com.dream.common.redis.dao;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public interface IDBRedisPool {
	Jedis getConnection() throws Exception;
	void close(Jedis jedis) ;
	void close(Transaction tx) throws IOException;
}