package com.dream.common.redis.dao;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class BaseRedisDao extends BaseDao {
	private static Logger logger = Logger.getLogger(BaseRedisDao.class);

	public BaseRedisDao() {}

	public BaseRedisDao(IDBRedisPool pool) {
		super(pool);
	}
	
	public static interface CallbackRedis {
		/**
		 * 回调方法，只处理实际的DAO操作，不必关系conn,pstmt和rs资源的资源释放问题
		 * @return
		 * @throws Exception
		 */
		public void run(Transaction tx) throws Exception;
	}
	
	/**
	 * 数据库操作的模板方法（模板模式） 
	 * 
	 * 注意：这里的事务
	 * 
	 * @throws Exception
	 * @see {@link CallbackRedis} 
	 */
	public void execute(CallbackRedis callback) throws Exception {
		Jedis jedis = null;
		Transaction tx = null;
		try {
			jedis = this.pool.getConnection();
			tx = jedis.multi();
			callback.run(tx);
			List<Object> result = tx.exec();
			if (result == null || result.isEmpty()) {
				throw new Exception("redis 事务异常，可能的原因是数据修改失败，而非语法错误！！！！");
			}
		} finally {
			if (tx != null) {
				try {
					this.pool.close(tx);
				}catch (IOException e) {
					logger.error("execute error:" + e.getMessage());
					throw e;
				}
			}
			if (jedis != null) {
				this.pool.close(jedis);
			}
		}
	}

// ------------------------------String类型接口----------------------------------------------------------------
	
	public boolean stringAdd(String key, String value) throws Exception {
		return super.stringAddDao(null, key, value);
	}
	
	/** 成功返回OK **/
	public void stringAdd(Transaction tx, String key, String value) throws Exception {
		super.stringAddDao(tx, key, value);
	}
	
	public boolean stringAddAndSetExpire(String key, String value, int expireSecond) throws Exception {
		return super.stringAddAndSetExpireDao(null, key, value, expireSecond);
	}
	
	/** 成功返回OK 
	 * @return **/
	public void stringAddAndSetExpire(Transaction tx, String key, String value, int expireSecond) throws Exception {
		super.stringAddAndSetExpireDao(tx, key, value, expireSecond);
	}
	
	
	public boolean stringAddSerial(byte[] key, byte[] value)  throws Exception {
		return super.stringAddSerialDao(null, key, value);
	}
	
	/** 成功返回OK **/
	public void stringAddSerial(Transaction tx,byte[] key, byte[] value)  throws Exception {
		super.stringAddSerialDao(tx, key, value);
	}
	
	public boolean stringAdd(String key,long offset, String value)  throws Exception {
		return super.stringAddDao(null, key, offset, value);
	}
	
	/** 成功返回大于0L **/
	public void stringAdd(Transaction tx, String key,long offset, String value) throws Exception {
		super.stringAddDao(tx, key, offset, value);
	}

	public String stringGet(String key) {
		return super.stringGet(key);
	}
	
	public byte[] stringGet(byte[] key) {
		return super.stringGet(key);
	}
	
	public List<String> stringGet(String... keys) {
		return super.stringGet(keys);
	}

	public String stringGetSubString(String key, long startOff, long endOff) {
		return super.stringGetSubString(key, startOff, endOff);
	}
	
	public String stringGetSet(String key, String value) throws Exception {
		return super.stringGetSetDao(null, key, value);
	}
	
	
	/** 成功返回：设置之前的旧值，失败返回null **/
	public void stringGetSet(Transaction tx,String key, String value) throws Exception {
		super.stringGetSetDao(tx, key, value);
	}
	
	public boolean stringAddKeyWhenNoExistsKey(String key, String value) throws Exception {
		return super.stringAddKeyWhenNoExistsKeyDao(null, key, value);
	}

	/** 成功返回大于0L的值 **/
	public void stringAddKeyWhenNoExistsKey(Transaction tx,String key, String value) throws Exception {
		super.stringAddKeyWhenNoExistsKeyDao(tx, key, value);
	}
	
	public long stringLen(String key) {
		return super.stringLen(key);
	}

	public boolean stringAddOrReplace(Map<String, String> map) throws Exception {
		return super.stringAddOrReplaceDao(null, map);
	}
	
	/** 成功返回OK **/
	public void stringAddOrReplace(Transaction tx,Map<String, String> map) throws Exception {
		super.stringAddOrReplaceDao(tx, map);
	}
	
	public boolean stringAddWhenNoExistsKey(Map<String, String> map) throws Exception {
		return super.stringAddWhenNoExistsKeyDao(null, map);
	}
	
	/** 成功返回大于0L **/
	public void stringAddWhenNoExistsKey(Transaction tx,Map<String, String> map) throws Exception {
		super.stringAddWhenNoExistsKeyDao(tx, map);
	}
	
	public boolean stringAppend(String key, String value) throws Exception {
		return super.stringAppendDao(null, key, value);
	}
	
	/** 成功返回大于0L **/
	public void stringAppend(Transaction tx,String key, String value) throws Exception {
		super.stringAppendDao(tx, key, value);
	}
	
	/** 成功返回大于0L **/
	public long stringSequence(String key){
		return super.stringSequence(key);
	}
	
// ------------------------------List类型接口----------------------------------------------------------------	
	
	public long listAddToHeader(String key, String... value) throws Exception {
		return super.listAddToHeaderDao(null, key, value);
	}
	
	/** 成功返回大于0L **/
	public void listAddToHeader(Transaction tx,String key, String... value) throws Exception {
		super.listAddToHeaderDao(tx, key, value);
	}
	
	public long listAddToFooter(String key, String... value) throws Exception {
		return super.listAddToFooterDao(null, key, value);
	}
	
	/** 成功返回大于0L **/
	public void listAddToFooter(Transaction tx,String key, String... value) throws Exception {
		super.listAddToFooterDao(tx, key, value);
	}
	
	public boolean listAddIndexValue(String key, int index, String value) throws Exception {
		return super.listAddIndexValueDao(null, key, index, value);
	}
	
	/** 成功返回OK **/
	public void listAddIndexValue(Transaction tx,String key, int index, String value) throws Exception {
		super.listAddIndexValueDao(tx, key, index, value);
	}
	
	public String listGetByIndex(String key, int index) {
		return super.listGetByIndex(key, index);
	}
	
	public long listSize(String key) {
		return super.listSize(key);
	}
	
	public String listRemoveFirst(String key) throws Exception {
		return super.listRemoveFirstDao(null, key);
	}
	
	/** 成功返回移除的值，失败返回null **/
	public void listRemoveFirst(Transaction tx,String key) throws Exception {
		super.listRemoveFirstDao(tx, key);
	}
	
	public String listRemoveEnd(String key) throws Exception {
		return super.listRemoveEndDao(null, key);
	}
	
	/** 成功返回移除的值，失败返回null **/
	public void listRemoveEnd(Transaction tx,String key) throws Exception {
		super.listRemoveEndDao(tx, key);
	}
	
	public boolean listRemoveIndexIFEqValue(String key, int index, String value) throws Exception {
		return super.listRemoveIndexIFEqValueDao(null, key, index, value);
	}
	
	/** 成功返回大于0L **/
	public void listRemoveIndexIFEqValue(Transaction tx,String key, int index, String value) throws Exception {
		super.listRemoveIndexIFEqValueDao(tx, key, index, value);
	}
	
	
// ------------------------------Map类型接口----------------------------------------------------------------
	
	public boolean mapAddMap(String key, Map<String, String> valueMap) throws Exception {
		return super.mapAddMapDao(null, key, valueMap);
	}
	
	/** 成功返回OK **/
	public void mapAddMap(Transaction tx,String key, Map<String, String> valueMap) throws Exception {
		super.mapAddMapDao(tx, key, valueMap);
	}
	
	public boolean mapAdd(String key, String subKey, String value) throws Exception {
		return super.mapAddDao(null, key, subKey, value);
	}
	
	/** 成功返回大等于0L **/
	public void mapAdd(Transaction tx,String key, String subKey, String value) throws Exception {
		super.mapAddDao(tx, key, subKey, value);
	}

	public String mapGet(String key, String subKey) {
		return super.mapGet(key, subKey);
	}
	
	public Map<String, String> mapGetAll(String key) {
		return super.mapGetAll(key);
	}
	
	public List<String> mapGetAllValue(String key) {
		return super.mapGetAllValue(key);
	}
	
	public boolean mapRemoveKey(String key, String... subKeys) throws Exception {
		return super.mapRemoveKeyDao(null, key, subKeys);
	}
	
	/** 成功返回大于0L **/
	public void mapRemoveKey(Transaction tx,String key, String... subKeys) throws Exception {
		super.mapRemoveKeyDao(tx, key, subKeys);
	}
	
	public boolean mapExistsKey(String key, String subKey) {
		return super.mapExistsKey(key, subKey);
	}
	
	public Set<String> mapGetAllKey(String key) {
		return super.mapGetAllKey(key);
	}
	
	public long mapSize(String key) {
		return super.mapSize(key);
	}
	
	public List<String> mapGetValueByKey(String key, String... subKeys) {
		return super.mapGetValueByKey(key, subKeys);
	}

	public Long mapIncr(String key, String subKey, long val) {
		return super.mapIncr(key, subKey, val);
	}

// ------------------------------Set接口----------------------------------------------------------------
	
	public long setAddValues(String key, String... values) throws Exception {
		return super.setAddValuesDao(null, key, values);
	}
	
	/** 成功返回大于0L **/
	public void setAddValues(Transaction tx,String key, String... values) throws Exception {
		super.setAddValuesDao(tx, key, values);
	}
	
	public long setSize(String key) {
		return super.setSize(key);
	}
	
	public Set<String> setGetAllValue(String key) {
		return super.setGetAllValue(key);
	}
	
	public boolean setExistsValue(String key, String value) {
		return super.setExistsValue(key, value);
	}
	
	public boolean setRemoveValues(String key, String... values) throws Exception {
		return super.setRemoveValuesDao(null, key, values);
	}
	
	/** 成功返回大于0L **/
	public void setRemoveValues(Transaction tx,String key, String... values) throws Exception {
		super.setRemoveValuesDao(tx, key, values);
	}
	
	public boolean setMove(String key1, String vlaue, String key2) throws Exception {
		return super.setMoveDao(null, key1, vlaue, key2);
	}
	
	/** 成功返回大于0L **/
	public void setMove(Transaction tx,String key1, String vlaue, String key2) throws Exception {
		super.setMoveDao(tx, key1, vlaue, key2);
	}
	
	public Set<String> setJoin(String... keys) {
		return super.setJoin(keys);
	}
	
	public Set<String> setSinter(String... keys) {
		return super.setSinter(keys);
	}
	
	public Set<String> setDiff(String... keys) {
		return super.setDiff(keys);
	}
	
// ------------------------------LinkedHash（sorted set）接口----------------------------------------------------------------
	//排序规则：按照分数（scope）升序排序，如果分数（scope）相同，按照分数对应的value(值)的字典顺序排序
	
	public boolean zsetAdd(String key, Map<String, Double> scoreMembers) throws Exception {
		return super.zsetAddDao(null, key, scoreMembers);
	}
	
	/** 成功返回大于0L **/
	public void zsetAdd(Transaction tx,String key, Map<String, Double> scoreMembers) throws Exception {
		super.zsetAddDao(tx, key, scoreMembers);
	}
	
	public long zsetSize(String key) {
		return super.zsetSize(key);
	}
	
	public long zsetCount(String key, double scoreStart, double scoreEnd) {
		return super.zsetCount(key, scoreStart, scoreEnd);
	}
	
	public Set<String> zsetDescOrder(String key, long start, long end) {
		return super.zsetDescOrder(key, start, end);
	}
	
	public Set<String> zsetDic(String key, String min, String max) {
		return super.zsetDic(key, min, max);
	}
	
	public Set<String> zsetScore(String key, double min, double max) {
		return super.zsetScore(key, min, max);
	}
	
	public long zsetGetIndexByValue(String key, String value) {
		return super.zsetGetIndexByValue(key, value);
	}
	
	public boolean zsetRemoveByValue(String key, String... values) throws Exception {
		return super.zsetRemoveByValueDao(null, key, values);
	}
	
	/** 成功返回大于0L **/
	public void zsetRemoveByValue(Transaction tx,String key, String... values) throws Exception {
		super.zsetRemoveByValueDao(tx, key, values);
	}
	
	public long zsetGetRankNumByValue(String key, String value) {
		return super.zsetGetRankNumByValue(key, value);
	}
	
	public Double zsetGetScoreNumByValue(String key, String value) {
		return super.zsetGetScoreNumByValue(key, value);
	}
	
// ------------------------------通用接口----------------------------------------------------------------	
	
	public boolean exists(String key) {
		return super.exists(key);
	}
	
	public boolean removeKeys(String... key) throws Exception {
		return super.removeKeysDao(null, key);
	}
	
	/** 成功返回大于0L **/
	public void removeKeys(Transaction tx,String... key) throws Exception {
		super.removeKeysDao(tx, key);
	}
	
	public String type(String key) {
		return super.type(key);
	}
	
	public long getExpire(String key) {
		return super.getExpire(key);
	}
	
	public long getExpireMills(String key) {
		return super.getExpireMills(key);
	}
	
	public Set<String> getKeys(String pattern) {
		return super.getKeys(pattern);
	}
	
	public boolean expire(Transaction tx, String key, int seconds) throws Exception {
		return super.expireDao(tx, key, seconds);
	}
	
	public boolean expire(String key, int seconds) throws Exception {
		return super.expireDao(null, key, seconds);
	}
	
}