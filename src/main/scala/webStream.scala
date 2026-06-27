import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable

//json解析相关
import org.json4s._
import org.json4s.jackson.JsonMethods._

object webStream {
  private val TODYA = this.getTODAY
  implicit val formats: Formats = DefaultFormats    //隐式声明需要放置在类内部

  private def getNOW = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
  }

  private def getTODAY = {
    new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  }

  private def getKafkaParams(_prop:Properties,_topic:String) = {
    val _map =  Map[String, Object](
      "bootstrap.servers" -> _prop.getProperty("kafkaParams.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> _topic,
      "receive.buffer.bytes"->Integer.valueOf(_prop.getProperty("kafkaParams.receive.buffer.bytes")),
      "auto.offset.reset"->_prop.getProperty("kafkaParams.auto.offset.reset"),
      "enable.auto.commit"->_prop.getProperty("kafkaParams.enable.auto.commit")
    )
    (List(_topic),_map)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("sparkStream")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5)) //spark3.4弃用
    val prop = new Properties();
    val config = ConfigFactory.parseFile(new File("./common_config/application.properties"))
    val entries = config.entrySet().asScala
    for (entry <- entries) {
      val key = entry.getKey
      val value = entry.getValue.unwrapped.toString
      //println(s"$key = $value")
      prop.setProperty(key, value)
    }
    streamingContext.checkpoint("./saveCheckPoint2")
    val launchKafkaParams = this.getKafkaParams(prop,"webLaunch")
    val kafkaDStreamForLaunch = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, Subscribe[String,String](launchKafkaParams._1, launchKafkaParams._2))
    kafkaDStreamForLaunch.map(x=>{
      //println(x.value)
      //json转map
      val map = parse(x.value).extract[Map[String, Any]]
      map
    }).foreachRDD(rdd => {
      rdd.foreachPartition(data => {
        data.foreach { webInfoMap =>
          //println(msg)          // 这里就是每条消息的实际内容
          webLaunchData(webInfoMap)
        }
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
   * 统计web端激活数
   * @param data
   */
  private def webLaunchData(data: Map[String, Any]): Unit = {
    val connection: Connection = JDBCutil.getConnection
    val TODAY = this.getTODAY
    val planExistSql = "SELECT * FROM lionu_statistics_base WHERE app_id=? AND media_creativeid=? AND stat_date=?"
    val planExistRes = JDBCutil.executeQuery(connection, planExistSql, Array(data("app_id").toString, data("media_creativeid").toString, TODAY))
    if (planExistRes.isEmpty) {
      //没有记录写入一条空白记录
      val insertBaseSql = "INSERT INTO lionu_statistics_base(media_accountid, media_campaignid, media_advgroupid, media_creativeid, app_id, plan_id, channel_id, stat_date) VALUES(?,?,?,?)"
      JDBCutil.executeUpdate(connection, insertBaseSql, Array(data("media_accountid").toString, data("media_campaignid").toString, data("media_advgroupid").toString, data("media_creativeid").toString, data("app_id").toString, data("plan_id").toString, data("channel_id").toString, TODAY))
    }

    //两者都为true则写留存表
    var moreThan2Days = false
    var todayNoLaunch = false
    var active_date: String = ""    //激活日期
    var retentionDays: Int = 0      //留存天数
    //需要更新base表的数据
    val updateBase = mutable.Map[String, Int]()
    //查找激活表是否有记录
    val activeRes = findActiveExist(connection, data)
    if (activeRes.isEmpty) {
      insertActive(connection, data)
      updateBase += ("active_count" -> 1)
    } else {
      //计算留存天数
      active_date = activeRes(0)(9)
      retentionDays = diffDays(active_date, TODAY) + 1
      //println(retentionDays)
      if (retentionDays >= 2) {
        moreThan2Days = true
      }
    }
    val todayLaunchRes = findTodayLaunchExist(connection, data)
    if (todayLaunchRes.isEmpty) {
      updateBase += ("vibrant_count" -> 1)
      todayNoLaunch = true
    }
    insertLaunch(connection, data)
    updateBase += ("launch_count" -> 1)

    //println(updateBase)
    //根据updatBase key-value来拼接sql并更新base表
    val setClauses = updateBase.map { case (col, inc) => s"$col = $col + ?" }.mkString(", ")
    val sql = s"UPDATE lionu_statistics_base SET $setClauses WHERE app_id = ? AND media_creativeid=? AND stat_date=?"
    val params = (updateBase.values.toSeq ++ Seq(data("app_id").toString, data("media_creativeid").toString, TODAY)).toArray
    JDBCutil.executeUpdate(connection, sql, params)

    //根据moreThan2Days 和 todayNoLaunch都为true则更新留存表
    if (moreThan2Days && todayNoLaunch) {
      ifInsertRetention(connection, data, retentionDays, active_date)
    }
    connection.close()
  }

  /**
   * 查找激活记录是否存在 lionu_log_web_active 表中
   * @param connection
   * @param data
   * @return
   */
  private def findActiveExist(connection:Connection, data:Map[String,Any]) = {
    val activeExistSql = "SELECT * FROM lionu_log_web_active WHERE media_clickid=? ORDER BY active_time LIMIT 0,1"
    val activeRes = JDBCutil.executeQuery(connection, activeExistSql, Array(data("media_clickid")))
    activeRes
  }

  /**
   * 写入active表一条记录
   * @param connection
   * @param data
   * @return
   */
  private def insertActive(connection:Connection, data:Map[String,Any]) = {
    val insertActiveSql = "INSERT INTO lionu_log_web_active(media_accountid, media_campaignid, media_advgroupid, media_creativeid , media_clickid, external_ip, app_id, plan_id, channel_id, active_date, active_time) " +
      "VALUES(?,?,?,?,?,?,?,?,?,?)"
    val newInsertActive = Array(data("media_accountid"), data("media_campaignid"), data("media_advgroupid"), data("media_creativeid"),
      data("media_clickid"), data("external_ip"), data("app_id").toString, data("plan_id").toString,
      data("channel_id").toString, this.TODYA, data("active_time"),
    )
    val insertRes = JDBCutil.executeUpdate(connection, insertActiveSql, newInsertActive)
    insertRes
  }

  /**
   * 写入launch表一条记录
   * @param connection
   * @param data
   */
  private def insertLaunch(connection:Connection, data:Map[String,Any]): Unit = {
    val TODAY = this.getTODAY
    val insertLaunchSql = "INSERT INTO lionu_log_web_launch(media_accountid, media_campaignid, media_advgroupid, media_creativeid , media_clickid, external_ip, app_id, plan_id, channel_id, launch_date, launch_time) " +
      "VALUES(?,?,?,?,?,?,?,?,?,?)"
    val newInsertLaunch = Array(data("media_accountid"), data("media_campaignid"), data("media_advgroupid"), data("media_creativeid"),
      data("media_clickid"), data("external_ip"), data("app_id").toString, data("plan_id").toString,
      data("channel_id").toString, TODAY, data("active_time")
    )
    JDBCutil.executeUpdate(connection, insertLaunchSql, newInsertLaunch)
  }

  /**
   * 查找登录表是否有当天的记录
   * @param connection
   * @param data
   */
  private def findTodayLaunchExist(connection:Connection, data:Map[String,Any]) = {
    val TODAY = this.getTODAY
    val launchExistSql = "SELECT * FROM lionu_log_web_launch WHERE media_clickid=? AND launch_date=? ORDER BY launch_time LIMIT 0,1"
    val launchRes = JDBCutil.executeQuery(connection, launchExistSql, Array(data("media_clickid"), TODAY))
    launchRes
  }

  /**
   * 判断是否需要向留存表中写入一条记录
   * @param connection
   * @param data
   * @param retention_days
   * @param active_date
   */
  private def ifInsertRetention(connection:Connection, data:Map[String,Any], retention_days:Int, active_date:String): Unit = {
    val retentionExistSql = "SELECT * FROM lionu_statistics_retention WHERE app_id=? AND media_creativeid=? AND retention_days=?"
    val retentionExistRes = JDBCutil.executeQuery(connection, retentionExistSql, Array(data("app_id").toString, data("media_creativeid").toString, retention_days))
    if(retentionExistRes.isEmpty){
      //没有记录写入一条空白记录
      val insertBaseSql = "INSERT INTO lionu_statistics_retention(media_accountid, media_campaignid, media_advgroupid, media_creativeid, app_id, plan_id, channel_id, retention_count, retention_days, active_date) VALUES(?,?,?,?,?,?,?,?,?,?)"
      JDBCutil.executeUpdate(connection, insertBaseSql, Array(data("media_accountid").toString, data("media_campaignid").toString, data("media_advgroupid").toString, data("media_creativeid").toString, data("app_id").toString, data("plan_id").toString, data("channel_id").toString, 1, retention_days, active_date))
    } else {
      //有记录则 retention_count+1
      val insertBaseSql = "UPDATE lionu_statistics_retention SET retention_count=retention_count+? WHERE app_id=? AND media_creativeid=? AND retention_days=? AND active_date=?"
      JDBCutil.executeUpdate(connection, insertBaseSql, Array(1, data("app_id").toString, data("media_creativeid").toString, retention_days, active_date))
    }
  }

  /**
   * 计算两个日期跨度的天数
   * startDate 起始日期
   * endDate  结束日期
   */
  private def diffDays(startDate:String,endDate:String):Int = {
    val dft = new SimpleDateFormat("yyyy-MM-dd")
    val start = dft.parse(startDate)
    val end = dft.parse(endDate)
    val starTime = start.getTime
    val endTime = end.getTime
    val num = ((endTime - starTime)/1000).toInt  //时间戳相差的毫秒数
    //System.out.println("相差天数为：" + num / 24 / 60 / 60 / 1000) //除以一天的毫秒数
    num / 24 / 60 / 60
  }
}
