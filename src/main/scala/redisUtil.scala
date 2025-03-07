/**
 * redis连接库地址
 * https://github.com/debasishg/scala-redis
 */

import com.redis.RedisClient

object redisUtil {
    private[this] var redisClient: RedisClient = null

    /**
     * 连接 Redis 服务器
     * @param host      Redis服务器地址
     * @param port      Redis服务器端口
     * @param timeout   连接超时时间 单位ms 默认20ms 0则不设置超时时间
     * @param password  连接密码 默认None
     * @param database  连接数据库序号 默认0号
     */
    def connect(host: String = "localhost", port: Int = 6379, database: Int = 0, password: Option[Any] = None,timeout: Int = 0): Boolean = {
        //println(timeout)
        if(redisClient == null){
            redisClient = new RedisClient(host, port, database) //为何初始化函数加上 password 和 timeout 两个参数就连接不上 redis了
            return redisClient.connect
        }
        false
    }

    /**
     *  设置 Redis 的值
     * @param key
     */
    def get(key: String): Option[String] = {
        var result:Option[String] = null
        if(redisClient == null){
            throw new Exception("未连接Redis服务器")
        } else {
            result = redisClient.get(key)
        }
        result
    }

    /**
     *  设置 Redis 的值
     */
    def set(key: String,value: String): Boolean = {
        var result = false
        if(redisClient == null){
            throw new Exception("未连接Redis服务器")
        } else {
            result = redisClient.set(key, value)
        }
        result
    }
}


