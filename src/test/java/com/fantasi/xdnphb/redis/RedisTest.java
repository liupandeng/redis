//package com.fantasi.xdnphb.redis;
//
//import org.apache.log4j.Logger;
//
//import redis.clients.jedis.Transaction;
//
//import com.dream.common.redis.dao.BaseRedisDao;
//import com.dream.common.redis.dao.BaseRedisDao.CallbackRedis;
//import com.dream.common.redis.dao.DBRedisPoll;
//import com.dream.common.redis.dao.IDBRedisPool;
//
//public class RedisTest {
//
//	private final static Logger logger = Logger.getLogger(BaseRedisDao.class);
//	private static BaseRedisDao redisDao = null;
//
//	public static IDBRedisPool initRedisPool() {
//		DBRedisPoll pool = new DBRedisPoll();
//		pool.init("127.0.0.1",  "6379",  "", "0");
//		return pool;
//	}
//
//	public static BaseRedisDao getBaseRedisDao() {
//		return new BaseRedisDao(initRedisPool());
//	}
//
//	static {
//		redisDao = getBaseRedisDao();
//	}
//
//	public static void test() {
//		/*Map<String, String> map = new HashMap<String, String>();
//		map.put("key4", "value4");
//		map.put("key5", "value5");
//
//		logger.info("---------结果为："+ redisDao.mapSetMap("map1", map));*/
//
////		logger.info("---------结果为："+ redisDao.mapAddField("mm1", "11", "22"));
////		logger.info("---------结果为："+ redisDao.mapGetAllValue("map0"));
////		logger.info("---------结果为："+ redisDao.mapGetAll("map"));
////		logger.info("---------结果为："+ redisDao.mapGet("map", "key0"));
////		logger.info("---------结果为："+ redisDao.mapRemoveKey("map", "key4"));
////		logger.info("---------结果为："+ redisDao.mapExistsKey("map", "key3"));
////		logger.info("---------结果为："+ redisDao.mapGetKeys("map0"));
////		logger.info("---------结果为："+ redisDao.mapSize("map0"));
////		logger.info("---------结果为："+ redisDao.mapGetAllKey("map"));
////		logger.info("---------结果为："+ redisDao.mapSize("xc"));
////		logger.info("---------结果为："+ redisDao.mapGetValueByKey("map", new String[]{"key1", "key0"}));
////		logger.info("---------结果为："+ redisDao.setAddValues("set", new String[]{"value1", "value2"}));
////		logger.info("---------结果为："+ redisDao.setSize("set"));
////		logger.info("---------结果为："+ redisDao.setGetAllValue("set"));
////		logger.info("---------结果为："+ redisDao.setGetAllValue("set1"));
////		logger.info("---------结果为："+ redisDao.setExistsValue("set", "1"));
////		logger.info("---------结果为："+ redisDao.setRemoveValues("set", "1"));
////		logger.info("---------结果为："+ redisDao.setMove("set", "2", "set0"));
////		logger.info("---------结果为："+ redisDao.setJoin("set", "xc", "set1"));
////		logger.info("---------结果为："+ redisDao.setSinter("set0", "xc0", "set1"));
////		logger.info("---------结果为："+ redisDao.setDiff("set", "xc0", "set1"));
////		logger.info("---------结果为："+ redisDao.zsetSize("zset1"));
////		logger.info("---------结果为："+ redisDao.zsetCount("zset", 0,1 ));
////		logger.info("---------结果为："+ redisDao.zsetDescOrder("zset2", 0,100 ));
////		logger.info("---------结果为："+ redisDao.zsetDic("zset2", "0", "1000"));
////		logger.info("---------结果为："+ redisDao.zsetGetIndexByValue("zset2", "value11"));
////		logger.info("---------结果为："+ redisDao.zsetRemoveByValue("zset2", new String[]{"9", "111"}));
////		logger.info("---------结果为："+ redisDao.zsetGetRankNumByValue("zset2", "value2"));
////		logger.info("---------结果为："+ redisDao.zsetGetScoreNumByValue("zset2", "value2"));
////		logger.info("---------结果为："+ redisDao.exists("xc"));
////		logger.info("---------结果为："+ redisDao.type("xc"));
//
////		logger.info("---------结果为："+ redisDao.expire("xc", 100));
////		logger.info("---------结果为："+ redisDao.getExpire("xc"));
////		logger.info("---------结果为："+ redisDao.getExpireMills("xc"));
////		logger.info("---------结果为："+ redisDao.getKeys("*"));
//
////		logger.info("---------结果为："+ redisDao.removeKey("xc"));
//	}
//
//	public static Boolean testTran() {
//		try {
//
//			/*System.out.println(redisDao.stringGet("xc1"));
//			System.out.println(redisDao.stringAdd("xc1", "xxxxx"));
//			*/
//			redisDao.execute(tx -> {
//                redisDao.stringAdd(tx, "xc", "xxxxx");
////					int i = 1/0;
//                redisDao.setAddValues(tx, "keyMap", "222");
////					redisDao.mapAdd(tx, "map", "maps", "1");
//            });
//			System.err.println(111);
////			System.out.println(redisDao.stringAdd("xc1", "xxxxx"));
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return false;
//	}
//
//	public static void test1() {
//		System.out.println(redisDao.stringGet("lastUpdateTime"));
//	}
//
//	public static void main(String[] args) {
////		test();
//
////		testTran();
//
////		test1();
//	}
//
//
//}
