package com.dream.common.redis.dao;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseDao extends AdpatRedis {
    private final static Logger logger = Logger.getLogger(BaseDao.class);
    protected IDBRedisPool pool = null;

    public BaseDao() {
    }

    public BaseDao(IDBRedisPool pool) {
        super(pool);
        this.pool = pool;
    }

    public void setDBPool(IDBRedisPool pool) {
        this.pool = pool;
    }

    public IDBRedisPool getDBPool() {
        return this.pool;
    }

// ------------------------------String类型接口----------------------------------------------------------------

    /**
     * 将key-value存储到String类型的数据里
     * <p>
     * 注意：如果该key原来不是String类型，执行该方法将会把原来key的类型改变成为String类型
     *
     * @return 是否设置成功
     */
    protected boolean stringAddDao(final Transaction tx, final String key, final String value) throws Exception {
        try {
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.set(key, value);
                        return true;
                    } else {
                        String statusCode = jedis.set(key, value);
                        return "OK".equals(statusCode);
                    }
                }
            });
        } catch (Throwable e) {
            throw new Exception("error stringAdd :" + e.getMessage(), e);
        }

		/*try {
			return this.execute(tx, jedis -> {
                if (tx != null) {
                    tx.set(key, value);
                    return true;
                }else {
                    String statusCode = jedis.set(key, value);
                    return "OK".equals(statusCode);
                }
            });
		} catch (Throwable e) {
			throw new Exception("error stringAdd :" + e.getMessage(), e);
		}*/
    }

    /**
     * expireSecond的单位为妙
     **/
    protected boolean stringAddAndSetExpireDao(final Transaction tx, final String key, final String value, final int expireSecond) throws Exception {

        try {
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.setex(key, expireSecond, value);
                        return true;
                    } else {
                        String statusCode = jedis.setex(key, expireSecond, value);
                        return "OK".equals(statusCode);
                    }
                }
            });
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.setex(key, expireSecond, value);
//                    return true;
//                } else {
//                    String statusCode = jedis.setex(key, expireSecond, value);
//                    return "OK".equals(statusCode);
//                }
//            });
        } catch (Throwable e) {
            throw new Exception("error stringAdd :" + e.getMessage(), e);
        }
    }

    /**
     * 将key-value存储到String类型的数据里
     * <p>
     * 注意：如果该key原来不是String类型，执行该方法将会把原来key的类型改变成为String类型
     *
     * @return 是否设置成功
     */
    protected boolean stringAddSerialDao(final Transaction tx, final byte[] key, final byte[] value) throws Exception {

        try {
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.set(key, value);
                        return true;
                    } else {
                        String statusCode = jedis.set(key, value);
                        return "OK".equals(statusCode);
                    }
                }
            });
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.set(key, value);
//                    return true;
//                } else {
//                    String statusCode = jedis.set(key, value);
//                    return "OK".equals(statusCode);
//                }
//            });
        } catch (Throwable e) {
            throw new Exception("error stringAddSerial:" + e.getMessage(), e);
        }
    }

    /**
     * 是否设置成功
     * <p>
     * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始
     * eg: key1-"acbdef"
     *
     * @return boolean true:设置成功    false：设置失败
     **/
    protected boolean stringAddDao(final Transaction tx, final String key, final long offset, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.setrange(key, offset, value);
//                    return true;
//                }
//                return jedis.setrange(key, offset, value) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.setrange(key, offset, value);
                        return true;
                    }
                    return jedis.setrange(key, offset, value) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error stringAdd:" + e.getMessage(), e);
        }
    }

    /**
     * 获取该key的值
     *
     * @return String
     **/
    protected String stringGet(final String key) {

        try {
//            return this.execute(null, jedis -> jedis.get(key));
            return this.execute(null, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    return jedis.get(key);
                }
            });
        } catch (Exception e) {
            logger.error("error stringGet:" + e.getMessage());
        }

        return null;
    }

    /**
     * 获取该key的值
     *
     * @return String
     **/
    protected byte[] stringGet(final byte[] key) {

        try {
//            return this.execute(null, jedis -> jedis.get(key));
            return this.execute(null, new Callback<byte[]>() {
                @Override
                public byte[] run(Jedis jedis) throws Exception {
                    return jedis.get(key);
                }
            });
        } catch (Exception e) {
            logger.error("error stringGet:" + e.getMessage());
        }

        return null;
    }

    /**
     * 返回 指定key的值得集合
     *
     * @return List<String> ,该list里可能会出现null,即表示该key不存在，或不是String类型
     **/
    protected List<String> stringGet(final String... keys) {

        try {
//            return this.execute(null, jedis -> jedis.mget(keys));
            return this.execute(null, new Callback<List<String>>() {
                @Override
                public List<String> run(Jedis jedis) throws Exception {
                    return jedis.mget(keys);
                }
            });
        } catch (Exception e) {
            logger.error("error stringGet :" + e.getMessage());
        }

        return null;
    }


    /**
     * 返回 key中字符串值的子字符
     *
     * @return String
     **/
    protected String stringGetSubString(final String key, final long startOff, final long endOff) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "string")) {
//                    return jedis.getrange(key, startOff, endOff);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "string")) {
                        return jedis.getrange(key, startOff, endOff);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error stringGetSubString :" + e.getMessage());
        }

        return null;
    }

    /**
     * 替换key的值为新value，必须该key存在
     * <p>
     * 注意：如果该key原来不是String类型，执行该方法将会把原来key的类型改变成为String类型
     *
     * @return String 返回设置之前的旧值, 如果返回null表示key不存在
     **/
    protected String stringGetSetDao(final Transaction tx, final String key, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.getSet(key, value);
//                    return null;
//                }
//                return jedis.getSet(key, value);
//            });
            return this.execute(tx, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.getSet(key, value);
                        return null;
                    }
                    return jedis.getSet(key, value);
                }
            });
        } catch (Throwable e) {
            throw new Exception("error stringGetSet:" + e.getMessage(), e);
        }
    }

    /**
     * 是否设置成功
     * <p>
     * 必须当key不存在时，才为指定key设置新值，否则不设置
     *
     * @return boolean true:设置成功    false：设置失败
     **/
    protected boolean stringAddKeyWhenNoExistsKeyDao(final Transaction tx, final String key, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.setnx(key, value);
//                    return true;
//                }
//                return jedis.setnx(key, value) > 0;
//            });

            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.setnx(key, value);
                        return true;
                    }
                    return jedis.setnx(key, value) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error stringAddKeyWhenNoExistsKey:" + e.getMessage(), e);
        }
    }

    /**
     * 获取String类型key的值得长度
     *
     * @return long 如果该key不是String类型的，或者该key不存在，都返回 -1
     **/
    protected long stringLen(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "string")) {
//                    return jedis.strlen(key);
//                }
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "string")) {
                        return jedis.strlen(key);
                    }
                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error stringLen:" + e.getMessage());
        }

        return -1L;
    }

    /**
     * 添加/替换多个key-value组合，不管key存在与否
     * 如果该key存在，且不是String类型的，那么将会被设置成String类型的
     *
     * @return 是否添加/替换成功
     **/
    protected boolean stringAddOrReplaceDao(final Transaction tx, Map<String, String> map) throws Exception {

        final String[] keysvalues = caseMapToArray(map);

        if (keysvalues != null) {
            try {
//                return this.execute(tx, jedis -> {
//                    if (tx != null) {
//                        tx.mset(keysvalues);
//                        return true;
//                    }
//                    return "OK".equals(jedis.mset(keysvalues));
//                });
                return this.execute(tx, new Callback<Boolean>() {
                    @Override
                    public Boolean run(Jedis jedis) throws Exception {
                        if (tx != null) {
                            tx.mset(keysvalues);
                            return true;
                        }
                        return "OK".equals(jedis.mset(keysvalues));
                    }
                });
            } catch (Throwable e) {
                throw new Exception("error stringAddOrReplace:" + e.getMessage(), e);
            }
        }

        return false;
    }

    /**
     * 添加多个key-value组合，要添加的key必须都不存在，有一个存在，就一个也添加不进去
     *
     * @return 是否添加/替换成功
     **/
    protected boolean stringAddWhenNoExistsKeyDao(final Transaction tx, Map<String, String> map) throws Exception {

        final String[] keysvalues = caseMapToArray(map);
        if (keysvalues != null) {
            try {
//                return this.execute(tx, jedis -> {
//                    if (tx != null) {
//                        tx.msetnx(keysvalues);
//                        return true;
//                    }
//                    return jedis.msetnx(keysvalues) > 0;
//                });
                return this.execute(tx, new Callback<Boolean>() {
                    @Override
                    public Boolean run(Jedis jedis) throws Exception {
                        if (tx != null) {
                            tx.msetnx(keysvalues);
                            return true;
                        }
                        return jedis.msetnx(keysvalues) > 0;
                    }
                });
            } catch (Throwable e) {
                throw new Exception("error stringAddWhenNoExistsKey:" + e.getMessage(), e);
            }
        }
        return false;
    }

    /**
     * 添加value到指定key的值得末尾，该key必须存在，且必须为String类型的
     *
     * @return boolean 是否添加成功
     **/
    protected boolean stringAppendDao(final Transaction tx, final String key, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//
//                if (isType(jedis, key, "string")) {
//                    if (tx != null) {
//                        tx.append(key, value);
//                        return true;
//                    }
//                    return jedis.append(key, value) > 0;
//                }
//
//                return false;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "string")) {
                        if (tx != null) {
                            tx.append(key, value);
                            return true;
                        }
                        return jedis.append(key, value) > 0;
                    }
                    return false;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error stringAppend:" + e.getMessage(), e);
        }
    }

    /**
     * 获取String类型key自增一后的值
     *
     * @return long 如果该key不是String类型的，或者该key不存在，都返回 -1
     **/
    protected long stringSequence(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "string")) {
//                    return jedis.incr(key);
//                }
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "string")) {
                        return jedis.incr(key);
                    }
                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error stringLen:" + e.getMessage());
        }

        return -1L;
    }


// ------------------------------List类型接口----------------------------------------------------------------	

    /**
     * 将key-values往List头部添加， 该kye必须为List类型，或该key存在
     *
     * @return 返回添加到list的索引，如果添加失败就返回-1
     */
    protected long listAddToHeaderDao(final Transaction tx, final String key, final String... value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.lpush(key, value);
//                    return 0L;
//                }
//                return jedis.lpush(key, value);
//            });
            return this.execute(tx, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.lpush(key, value);
                        return 0L;
                    }
                    return jedis.lpush(key, value);
                }
            });
        } catch (Throwable e) {
            throw new Exception("error listAddToHeader:" + e.getMessage(), e);
        }
    }

    /**
     * 将key-values往List尾部添加，  该kye必须为List类型，或该key存在
     *
     * @return @return 返回添加到list的索引，如果添加失败就返回-1
     */
    protected long listAddToFooterDao(final Transaction tx, final String key, final String... value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.rpush(key, value);
//                    return 0L;
//                }
//                return jedis.rpush(key, value);
//            });
            return this.execute(tx, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.rpush(key, value);
                        return 0L;
                    }
                    return jedis.rpush(key, value);
                }
            });
        } catch (Throwable e) {
            throw new Exception("error listAddToFooter:" + e.getMessage(), e);
        }
    }

    /**
     * 设置指定索引的值，该key必须存在且为List类型的
     *
     * @return 返回是否存储成功
     */
    protected boolean listAddIndexValueDao(final Transaction tx, final String key, final int index, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.lset(key, index, value);
//                    return true;
//                }
//                return "OK".equals(jedis.lset(key, index, value));
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.lset(key, index, value);
                        return true;
                    }
                    return "OK".equals(jedis.lset(key, index, value));
                }
            });
        } catch (Throwable e) {
            throw new Exception("error listAddIndexValue:" + e.getMessage(), e);
        }
    }

    /**
     * 获取List指定索引的值，如果该key存在，或不是List的就返回null
     *
     * @return 返回指定索引的值
     */
    protected String listGetByIndex(final String key, final int index) {

        try {
//            return this.execute(null, jedis -> jedis.lindex(key, index));
            return this.execute(null, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    return jedis.lindex(key, index);
                }
            });
        } catch (Exception e) {
            logger.error("error listGetByIndex:" + e.getMessage());
        }

        return null;
    }

    /**
     * 获取List的长度
     * <p>
     * return 返回-1：表示该key不存在，或不是List类型的
     */
    protected long listSize(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "list")) {
//                    return jedis.llen(key);
//                }
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "list")) {
                        return jedis.llen(key);
                    }
                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error listSize:" + e.getMessage());
        }

        return -1;
    }

    /**
     * 移除List第一条数据
     *
     * @return 返回移除的值，如果key不存在，或key不是list，返回null
     */
    protected String listRemoveFirstDao(final Transaction tx, final String key) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.lpop(key);
//                    return null;
//                }
//                return jedis.lpop(key);
//            });
            return this.execute(tx, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.lpop(key);
                        return null;
                    }
                    return jedis.lpop(key);
                }
            });
        } catch (Throwable e) {
            throw new Exception("error listRemoveFirst:" + e.getMessage(), e);
        }

    }

    /**
     * 移除List最后一条数据
     *
     * @return 返回移除的值，如果key不存在，或key不是list，返回null
     */
    protected String listRemoveEndDao(final Transaction tx, final String key) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.rpop(key);
//                    return null;
//                }
//                return jedis.rpop(key);
//            });
            return this.execute(tx, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.rpop(key);
                        return null;
                    }
                    return jedis.rpop(key);
                }
            });
        } catch (Throwable e) {
            throw new Exception("error listRemoveEnd:" + e.getMessage());
        }
    }

    /**
     * 移除List指定index索引开始，往表尾搜索；如果index为负数，表示从末尾倒数多少个开始往表头搜索。移除与传人value值相等的所有值
     *
     * @return 返回如果移除key的个数大于0表示移除成功，否则一个都没移除
     */
    protected boolean listRemoveIndexIFEqValueDao(final Transaction tx, final String key, final int index, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.lrem(key, index, value);
//                    return true;
//                }
//                return jedis.lrem(key, index, value) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.lrem(key, index, value);
                        return true;
                    }
                    return jedis.lrem(key, index, value) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error listRemoveIndexIFEqValue:" + e.getMessage(), e);
        }
    }


// ------------------------------Map类型接口----------------------------------------------------------------

    /**
     * 将key-values存储到Map类型的数据里
     * 注意：该key必须是map类型的，或该key不存在，否则报错
     *
     * @return 返回是否存储成功
     */
    protected boolean mapAddMapDao(final Transaction tx, final String key, final Map<String, String> valueMap) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.hmset(key, valueMap);
//                    return true;
//                }
//                return "OK".equals(jedis.hmset(key, valueMap));
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.hmset(key, valueMap);
                        return true;
                    }
                    return "OK".equals(jedis.hmset(key, valueMap));
                }
            });
        } catch (Throwable e) {
            throw new Exception("error mapAddMap:" + e.getMessage());
        }
    }

    /**
     * 设置Map中指定field的值为新值
     * 注：key如果存在，必须为Map类型的，如果不存在，就新添加一个key
     *
     * @return 返回是否设置成功
     */
    protected boolean mapAddDao(final Transaction tx, final String key, final String subKey, final String value) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.hset(key, subKey, value);
//                    return true;
//                }
//                return jedis.hset(key, subKey, value) >= 0L;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.hset(key, subKey, value);
                        return true;
                    }
                    return jedis.hset(key, subKey, value) >= 0L;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error mapAdd:" + e.getMessage());
        }
    }

    /**
     * @return 获取Map中指定field的value
     **/
    protected String mapGet(final String key, final String subKey) {

        try {
//            return this.execute(null, jedis -> jedis.hget(key, subKey));
            return this.execute(null, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    return jedis.hget(key, subKey);
                }
            });
        } catch (Exception e) {
            logger.error("error mapGet:" + e.getMessage());
        }

        return null;
    }

    /**
     * @return 获取指定Map中所有的field
     **/
    protected Map<String, String> mapGetAll(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "hash")) {
//                    return jedis.hgetAll(key);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<Map<String, String>>() {
                @Override
                public Map<String, String> run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "hash")) {
                        return jedis.hgetAll(key);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error mapGetAll:" + e.getMessage());
        }
        return null;
    }

    /**
     * @return 获取指定Map中所有的field的value
     **/
    protected List<String> mapGetAllValue(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "hash")) {
//                    return jedis.hvals(key);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<List<String>>() {
                @Override
                public List<String> run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "hash")) {
                        return jedis.hvals(key);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error mapGetAllValue:" + e.getMessage());
        }

        return null;
    }

    /**
     * @return 根据key 和  value删除指定Map中的subkey
     **/
    protected boolean mapRemoveKeyDao(final Transaction tx, final String key, final String... subKeys) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.hdel(key, subKeys);
//                    return true;
//                }
//                return jedis.hdel(key, subKeys) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.hdel(key, subKeys);
                        return true;
                    }
                    return jedis.hdel(key, subKeys) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error mapRemoveKey:" + e.getMessage(), e);
        }
    }

    /**
     * @return 查看该Map中是否存在指定的subKey
     **/
    protected boolean mapExistsKey(final String key, final String subKey) {

        try {
//            return this.execute(null, jedis -> jedis.hexists(key, subKey));
            return this.execute(null, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    return jedis.hexists(key, subKey);
                }
            });
        } catch (Exception e) {
            logger.error("error mapExistsKey:" + e.getMessage());
        }

        return false;
    }

    /**
     * 获取指定Map类型的所有key集合，只要该key是Map类型的，就会被查询出来
     *
     * @return Set<String>
     **/
    protected Set<String> mapGetAllKey(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "hash")) {
//                    return jedis.hkeys(key);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "hash")) {
                        return jedis.hkeys(key);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error mapGetAllKey:" + e.getMessage());
        }

        return null;
    }

    /**
     * 获取指定Map的长度，如果该key不存在，或不为Map类型，返回-1
     *
     * @return 如果异常或该key不是map类型的返回-1
     **/
    protected long mapSize(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "hash")) {
//                    return jedis.hlen(key);
//                }
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "hash")) {
                        return jedis.hlen(key);
                    }
                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error mapSize:" + e.getMessage());
        }

        return -1;
    }

    /**
     * 获取指定Map的指定subKey的value
     * <p>
     * 1、如果key不是Map类型的: 返回null
     * 2、如果key是Map类型的，但是subKey不存在，返回[null]
     *
     * @return
     **/
    protected List<String> mapGetValueByKey(final String key, final String... subKeys) {

        try {
//            return this.execute(null, jedis -> jedis.hmget(key, subKeys));
            return this.execute(null, new Callback<List<String>>() {
                @Override
                public List<String> run(Jedis jedis) throws Exception {
                    return jedis.hmget(key, subKeys);
                }
            });
        } catch (Exception e) {
            logger.error("error mapGetValueByKey:" + e.getMessage());
        }

        return null;
    }

    /**
     * 获取指定Map的指定subKey的incr(自增值)
     **/
    protected Long mapIncr(final String key, final String subKey, Long val) {

        try {
//            return this.execute(null, (j) -> j.hincrBy(key, subKey, val));
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    return jedis.hincrBy(key, subKey, val);
                }
            });
        } catch (Exception e) {
            logger.error("error mapIncr:" + e.getMessage());
        }

        return null;
    }


// ------------------------------Set接口----------------------------------------------------------------

    /**
     * 往指定Set集合中添加多个值
     * 返回值：
     * 1、如果为-1：表示该key已经存在，但不是Set类型，或出现异常
     * 2、否则返回添加了多少个值的count数
     *
     * @return long
     **/
    protected long setAddValuesDao(final Transaction tx, final String key, final String... values) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.sadd(key, values);
//                    return 0L;
//                }
//                return jedis.sadd(key, values);
//            });
            return this.execute(tx, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.sadd(key, values);
                        return 0L;
                    }
                    return jedis.sadd(key, values);
                }
            });
        } catch (Throwable e) {
            throw new Exception("error setAddValues:" + e.getMessage(), e);
        }
    }

    /**
     * 获取指定Set集合的长度
     *
     * @return -1:表示指定的key不是Set类型的，
     **/
    protected long setSize(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "set")) {
//                    return jedis.scard(key);
//                }
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "set")) {
                        return jedis.scard(key);
                    }
                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error setSize:" + e.getMessage());
        }

        return -1L;
    }

    /**
     * 获取指定Set集合中所有的value
     * 注：如果该key存在但不是Set类型的，或key不存在，返回null
     *
     * @return Set<String>
     **/
    protected Set<String> setGetAllValue(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "set")) {
//                    return jedis.smembers(key);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "set")) {
                        return jedis.smembers(key);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error setGetAllValue:" + e.getMessage());
        }

        return null;
    }

    /**
     * @return 查看指定Set集合中是否存在指定的value
     **/
    protected boolean setExistsValue(final String key, final String value) {

        try {
//            return this.execute(null, jedis -> jedis.sismember(key, value));
            return this.execute(null, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    return jedis.sismember(key, value);
                }
            });
        } catch (Exception e) {
            logger.error("error setExistsValue:" + e.getMessage());
        }

        return false;
    }

    /**
     * 移除Set集合中指定的value
     * 注：如果该key不是Set类型或不存在，返回false
     *
     * @return boolean 表示是否删除成功
     **/
    protected boolean setRemoveValuesDao(final Transaction tx, final String key, final String... values) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.srem(key, values);
//                    return true;
//                }
//                return jedis.srem(key, values) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.srem(key, values);
                        return true;
                    }
                    return jedis.srem(key, values) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error setRemoveValues:" + e.getMessage(), e);
        }
    }

    /**
     * 移动Set集合key1中的value到Set集合key2中
     * 注：
     * 1、key1 和 key2都必须存在，且必须为Set类型的，否则移动不成功
     * 2、key1和key2可以相等，等效于没有移动，但是返回true
     *
     * @return 返回是否移动成功
     **/
    protected boolean setMoveDao(final Transaction tx, final String key1, final String vlaue, final String key2) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.smove(key1, key2, vlaue);
//                    return true;
//                }
//                return jedis.smove(key1, key2, vlaue) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.smove(key1, key2, vlaue);
                        return true;
                    }
                    return jedis.smove(key1, key2, vlaue) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error setMove:" + e.getMessage());
        }
    }

    /**
     * 将多个Set连接在一起，返回一个连接之后的Set集合
     * 注：
     * 1、如果不传参，返回null
     * 2、如果传参的第一个参数不为Set类型的，返回null
     * 3、如果从第二个参数开始，如果有某个参数不是Set类型的，结果返回null
     * 4、如果从第二个参数开始，有某些参数不存在，但存在的参数都是Set类型的，可以返回非null的值
     * 5、该操作不修改内存数据库的数据,所以不需要事务
     *
     * @return
     **/
    protected Set<String> setJoin(final String... keys) {

        try {
//            return this.execute(null, jedis -> {
//                if (keys.length > 0 && isType(jedis, keys[0], "set")) {
//                    return jedis.sunion(keys);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    if (keys.length > 0 && isType(jedis, keys[0], "set")) {
                        return jedis.sunion(keys);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error setJoin:" + e.getMessage());
        }

        return null;
    }

    /**
     * 返回给定Set集合的交集
     * 注：
     * 1、如果有某个参数存在，但不是Set类型的，结果返回null
     * 2、如果有某个参数不存在，在求交集时忽略该参数
     * 3、该操作不修改内存数据库的数据,所以不需要事务
     *
     * @return Set<String>
     **/
    protected Set<String> setSinter(final String... keys) {

        try {
//            return this.execute(null, jedis -> jedis.sinter(keys));
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    return jedis.sinter(keys);
                }
            });
        } catch (Exception e) {
            logger.error("error setSinter:" + e.getMessage());
        }

        return null;
    }


    /**
     * 返回Set集合key1在集合key2[key3...]中的差集
     * 注：
     * 1、如果有某个参数存在，但不是Set类型的，结果返回null
     * 2、如果有某个参数不存在，在求差集时忽略该参数
     * 3、该操作不修改内存数据库的数据,所以不需要事务
     *
     * @return Set<String>
     **/
    protected Set<String> setDiff(final String... keys) {

        try {
//            return this.execute(null, jedis -> jedis.sdiff(keys));
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    return jedis.sdiff(keys);
                }
            });
        } catch (Exception e) {
            logger.error("error setDiff:" + e.getMessage());
        }

        return null;
    }


// ------------------------------LinkedHash（sorted set）接口----------------------------------------------------------------
    //排序规则：按照分数（scope）升序排序，如果分数（scope）相同，按照分数对应的value(值)的字典顺序排序

    /**
     * 向LinkedHashSet集合添加元素
     * 注：
     * 1、如果key存在，必须为zset类型的
     * 2、如果value在集合中存在，将返回false
     * 3、scoreMembers 这个Map中如果有某个值在集合中存在，该条数据将不能被添加进去，其他如果被添加进去了，最终返回的结果还是true
     *
     * @return 是否添加成功
     **/
    protected boolean zsetAddDao(final Transaction tx, final String key, final Map<String, Double> scoreMembers) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.zadd(key, scoreMembers);
//                    return true;
//                }
//                return jedis.zadd(key, scoreMembers) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.zadd(key, scoreMembers);
                        return true;
                    }
                    return jedis.zadd(key, scoreMembers) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error zsetAdd:" + e.getMessage(), e);
        }
    }

    /**
     * 获取LinkedHashSet集合长度
     * -1:表示该key存在，但不是zset类型的；或出现异常
     *
     * @return long
     **/
    protected long zsetSize(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "zset")) {
//                    return jedis.zcard(key);
//                }
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "zset")) {
                        return jedis.zcard(key);
                    }
                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error zsetSize:" + e.getMessage());
        }

        return -1L;
    }

    /**
     * 获取LinkedHashSet集合中指定区间分数的个数
     * <p>
     * 注：
     * 1、key必须为zset类型的
     * 2、-1：表示异常或key不存在，或key存在，或key存在，但不是zset类型的
     *
     * @return long
     **/
    protected long zsetCount(final String key, final double scoreStart, final double scoreEnd) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "zset")) {
//                    return jedis.zcount(key, scoreStart, scoreEnd);
//                }
//
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "zset")) {
                        return jedis.zcount(key, scoreStart, scoreEnd);
                    }

                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error zsetCount:" + e.getMessage());
        }

        return -1L;
    }

    /**
     * 获取LinkedHashSet集合指定区间降序（从大到小）排序,如果分数相同，在按照字典顺序排序（而不是从大到小排序哦！）的子集合
     *
     * @return Set<String>
     **/
    protected Set<String> zsetDescOrder(final String key, final long start, final long end) {

        try {
//            return this.execute(null, jedis -> jedis.zrange(key, start, end));
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    return jedis.zrange(key, start, end);
                }
            });
        } catch (Exception e) {
            logger.error("error zsetDescOrder:" + e.getMessage());
        }

        return null;
    }


    /**
     * 获取LinkedHashSet集合字典区间(即：获取“a” ~ "k" 字典区间的子集合数)有序集合的成员
     * 注：key必须是zset类型的，且min和max不能为null
     *
     * @return Set<String>
     **/
    protected Set<String> zsetDic(final String key, final String min, final String max) {

        try {
//            return this.execute(null, jedis -> {
//
//                String demoMin = "", demoMax = "";
//                if (isType(jedis, key, "zset") && min != null && max != null) {
//                    demoMin = "[" + min;
//                    demoMax = "[" + max;
//                    return jedis.zrangeByLex(key, demoMin, demoMax);
//                }
//
//                return null;
//            });
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    String demoMin = "", demoMax = "";
                    if (isType(jedis, key, "zset") && min != null && max != null) {
                        demoMin = "[" + min;
                        demoMax = "[" + max;
                        return jedis.zrangeByLex(key, demoMin, demoMax);
                    }

                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error zsetDic:" + e.getMessage());
        }

        return null;
    }

    /**
     * 获取LinkedHashSet集合分数区间有序集合的成员
     * 注：
     * 1、key必须为zset类型
     * 2、min必须小等于max
     *
     * @return Set<String>
     **/
    protected Set<String> zsetScore(final String key, final double min, final double max) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "zset") && min <= max) {
//                    return jedis.zrangeByScore(key, min, max);
//                }
//                return null;
//            });
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "zset") && min <= max) {
                        return jedis.zrangeByScore(key, min, max);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error zsetScore:" + e.getMessage());
        }

        return null;
    }

    /**
     * 获取LinkedHashSet集合指定成员（value）值得索引
     * 注：
     * 1、key必须是zset类型的
     * 2、value必须存在
     *
     * @return long
     **/
    protected long zsetGetIndexByValue(final String key, final String value) {

        try {
//            return this.execute(null, jedis -> jedis.zrank(key, value));
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    return jedis.zrank(key, value);
                }
            });
        } catch (Exception e) {
            logger.error("error zsetGetIndexByValue:" + e.getMessage());
        }

        return -1;
    }

    /**
     * 移除LinkedHashSet集合指定成员value(即通过value删除集合中的成员)
     * 注：
     * 1、key必须是zset类型的
     * 2、values中只要有一个被移除了，都将返回true
     *
     * @return 是否移除成功
     **/
    protected boolean zsetRemoveByValueDao(final Transaction tx, final String key, final String... values) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.zrem(key, values);
//                    return true;
//                }
//                return jedis.zrem(key, values) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.zrem(key, values);
                        return true;
                    }
                    return jedis.zrem(key, values) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error zsetRemoveByValue:" + e.getMessage(), e);
        }
    }


    /**
     * 获取LinkedHashSet集合指定值(value)的排名数，排名最大的排名数为0  （即获取出来的值表示在集合中第几大）
     * 也就是说：分数值越大的value，排名值越小
     * <p>
     * 注：
     * 1、key必须为zset
     * 2、value必须在zset中存在
     * 3、-1：表示key是zset类型的，或value不存在，或出现异常
     *
     * @return long
     **/
    protected long zsetGetRankNumByValue(final String key, final String value) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "zset")) {
//                    return jedis.zrevrank(key, value);
//                }
//
//                return -1L;
//            });
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "zset")) {
                        return jedis.zrevrank(key, value);
                    }

                    return -1L;
                }
            });
        } catch (Exception e) {
            logger.error("error zsetGetRankNumByValue:" + e.getMessage());
        }

        return -1;
    }

    /**
     * 获取LinkedHashSet集合指定成员value的分数
     * 注：
     * 1、如果key不是zset类型的、或存在，返回null
     * 2、如果value在集合中不存在，返回null
     *
     * @return Double
     **/
    protected Double zsetGetScoreNumByValue(final String key, final String value) {

        try {
//            return this.execute(null, jedis -> {
//                if (isType(jedis, key, "zset")) {
//                    return jedis.zscore(key, value);
//                }
//
//                return null;
//            });
            return this.execute(null, new Callback<Double>() {
                @Override
                public Double run(Jedis jedis) throws Exception {
                    if (isType(jedis, key, "zset")) {
                        return jedis.zscore(key, value);
                    }

                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error zsetGetScoreNumByValue:" + e.getMessage());
        }

        return null;
    }


// ------------------------------通用接口----------------------------------------------------------------	

    /**
     * 是否存在该key，不管该key是什么类型的
     *
     * @return boolean
     **/
    protected boolean exists(final String key) {

        try {
//            return this.execute(null, jedis -> jedis.exists(key));
            return this.execute(null, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    return jedis.exists(key);
                }
            });
        } catch (Exception e) {
            logger.error("error exists:" + e.getMessage());
        }

        return false;
    }

    /**
     * 删除指定key，不管该key是什么类型的
     * 注：如果key不存在，返回false
     *
     * @return boolean
     **/
    protected boolean removeKeysDao(final Transaction tx, final String... key) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.del(key);
//                    return true;
//                }
//                return jedis.del(key) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.del(key);
                        return true;
                    }
                    return jedis.del(key) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error removeKeys:" + e.getMessage(), e);
        }
    }

    /**
     * 获取key的类型
     *
     * @return boolean
     **/
    protected String type(final String key) {

        try {
//            return this.execute(null, jedis -> {
//                String typeStr = jedis.type(key);
//                return !"none".equals(typeStr) ? typeStr : null;
//            });
            return this.execute(null, new Callback<String>() {
                @Override
                public String run(Jedis jedis) throws Exception {
                    String typeStr = jedis.type(key);
                    return !"none".equals(typeStr) ? typeStr : null;
                }
            });
        } catch (Exception e) {
            logger.error("error type:" + e.getMessage());
        }

        return null;
    }

    /**
     * 设置key的的过期时间
     * 注：
     * 1、key必须存在
     * 2、seconds必须大于0，seconds表示设置多少秒
     *
     * @return boolean 是否设置成功
     **/
    protected boolean expireDao(final Transaction tx, final String key, final int seconds) throws Exception {

        try {
//            return this.execute(tx, jedis -> {
//                if (tx != null) {
//                    tx.expire(key, seconds);
//                    return true;
//                }
//                return jedis.expire(key, seconds) > 0;
//            });
            return this.execute(tx, new Callback<Boolean>() {
                @Override
                public Boolean run(Jedis jedis) throws Exception {
                    if (tx != null) {
                        tx.expire(key, seconds);
                        return true;
                    }
                    return jedis.expire(key, seconds) > 0;
                }
            });
        } catch (Throwable e) {
            throw new Exception("error expire:" + e.getMessage(), e);
        }
    }

    /**
     * 获取key的过期时间，以秒为单位返回
     * 注：
     * 1、key必须存在
     * 2、-1：表示key永久
     * 3、-2：表示key过期，或异常
     *
     * @return long
     **/
    protected long getExpire(final String key) {

        try {
//            return this.execute(null, jedis -> jedis.ttl(key));
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    return jedis.ttl(key);
                }
            });
        } catch (Exception e) {
            logger.error("error getExpire:" + e.getMessage());
        }

        return -2L;
    }

    /**
     * 获取key的过期时间，以毫秒为单位返回
     * 注：
     * 1、key必须存在
     * 2、-1：表示key永久
     * 3、-2：表示key过期，或异常
     *
     * @return long
     **/
    protected long getExpireMills(final String key) {

        try {
//            return this.execute(null, jedis -> jedis.pttl(key));
            return this.execute(null, new Callback<Long>() {
                @Override
                public Long run(Jedis jedis) throws Exception {
                    return jedis.pttl(key);
                }
            });
        } catch (Exception e) {
            logger.error("error getExpireMills:" + e.getMessage());
        }

        return -2L;
    }

    /**
     * 获取指定正则的所有keys
     * 注：
     * 1、如果要获取所有key，则pattern为"*"
     *
     * @return Set<String>
     **/
    protected Set<String> getKeys(final String pattern) {

        try {
//            return this.execute(null, jedis -> jedis.keys(pattern));
            return this.execute(null, new Callback<Set<String>>() {
                @Override
                public Set<String> run(Jedis jedis) throws Exception {
                    return jedis.keys(pattern);
                }
            });
        } catch (Exception e) {
            logger.error("error getKeys:" + e.getMessage());
        }

        return null;
    }

    private boolean isType(Jedis jedis, String key, String typeName) {

        if (jedis.exists(key)) {
            return typeName.equals(jedis.type(key));
        }

        return true;
    }

    private String[] caseMapToArray(Map<String, String> map) {
        if (map != null && !map.isEmpty()) {

            String[] keysvalues = new String[map.size() * 2];

            int i = 0;
            for (String key : map.keySet()) {
                keysvalues[i++] = key;
                keysvalues[i++] = map.get(key);
            }

            return keysvalues;
        }

        return null;
    }
}