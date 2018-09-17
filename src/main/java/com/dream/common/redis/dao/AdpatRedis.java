package com.dream.common.redis.dao;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class AdpatRedis {
	
	protected Logger logger = Logger.getLogger(getClass());
	protected IDBRedisPool pool = null;
	
	public AdpatRedis() {
	}
	
	public AdpatRedis(IDBRedisPool pool) {
		this.pool = pool;
	}
	
	public void setDBPool(IDBRedisPool pool) {
		this.pool = pool;
	}
	
	protected static interface Callback<T> {
		/**
		 * 回调方法，只处理实际的DAO操作，不必关系conn,pstmt和rs资源的资源释放问题
		 * @return
		 * @throws Exception
		 */
		T run(Jedis jedis) throws Exception;
	}
	
	/**
	 * 数据库操作的模板方法（模板模式）
	 * @return
	 * @throws Exception
	 * @see {@link Callback} 
	 */
	protected <T> T execute(Transaction tx, Callback<T> callback) throws Exception {
		Jedis jedis = null;
		boolean b = tx == null;
		try {
			if (b) {
				jedis = this.pool.getConnection();
			}
			
			return callback.run(jedis);
		} finally {
			if (b) {
				if (jedis != null) {
					this.pool.close(jedis);
				}
			}
		}
	}
}