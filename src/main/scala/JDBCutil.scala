import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

import java.io.{File, FileInputStream, InputStream}

object JDBCutil {
  //初始化连接池
  private val dataSource: DataSource = init()

  //初始化连接池方法
  private def init() = {
    //读取配置文件
    val prop = new Properties()
    //方式一 将properties文件打入jar包 使用ClassLoader加载properties配置文件生成对应的输入流
    /*val in = JDBCutil.getClass.getClassLoader.getResourceAsStream("application.properties")
    // 使用properties对象加载输入流
    prop.load(in)
    val properties = new Properties()
    properties.setProperty("driverClassName", prop.getProperty("mysql.drive"))
    properties.setProperty("url", "jdbc:mysql://" + prop.getProperty("mysql.hostname") + ":" + prop.getProperty("mysql.port") + "/" + prop.getProperty("mysql.database"))
    properties.setProperty("username", prop.getProperty("mysql.user"))
    properties.setProperty("password", prop.getProperty("mysql.password"))
    properties.setProperty("maxActive", "50")*/

    //方式二 properties配置不打进jar包 使用外部传入的方式 需要使用parseFile方法 使用load无效 而且执行目录是shell所在的目录 不是 jar 包所在的目录
    val config = ConfigFactory.parseFile(new File("../common_config/application.properties"))
    val properties = new Properties()
    properties.setProperty("driverClassName", config.getString("mysql.drive"))
    properties.setProperty("url", "jdbc:mysql://" + config.getString("mysql.hostname") + ":" + config.getString("mysql.port") + "/" + config.getString("mysql.database"))
    properties.setProperty("username", config.getString("mysql.user"))
    properties.setProperty("password", config.getString("mysql.password"))
    properties.setProperty("maxActive", "50")

    //println(properties.getProperty("url"))
    DruidDataSourceFactory.createDataSource(properties)

  }

  //获取mysql连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行SQL语句，单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    val logger = LoggerFactory.getLogger(this.getClass)
    try {
      connection.setAutoCommit(false)
      val pstmt = connection.prepareStatement(sql)
      params.zipWithIndex.foreach { case (param, i) => pstmt.setObject(i + 1, param) }
      //拼接打印sql语句
      //val sqlWithParams = sql.split("\\?").zipAll(params.map(_.toString), "", "").map { case (sqlPart, param) => sqlPart + "'" + param + "'" }.mkString("")
      //println(s"Executing SQL: $sqlWithParams")
      val rtn = pstmt.executeUpdate()
      connection.commit()
      rtn
    } catch {
      case e: SQLException =>
        logger.error("SQL Exception occurred", e)
        connection.rollback() // 回滚事务
        0
      case e: Exception =>
        logger.error("Exception occurred", e)
        connection.rollback() // 回滚事务
        0
    } finally {
      connection.setAutoCommit(true) // 恢复自动提交
    }
  }

  /**
   * 查找记录
   * 查询到 以数组形式返回第一行记录
   * 未查询到返回 null
   */

  def executeQuery(connection: Connection, sql: String, params: Array[Any]): ArrayBuffer[ArrayBuffer[String]] = {
    var rs: ResultSet = null
    val queryResult:ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      //拼接打印sql语句
      //val sqlWithParams = sql.split("\\?").zipAll(params.map(_.toString), "", "").map { case (sqlPart, param) => sqlPart + "'" + param + "'" }.mkString("")
      //println(s"Executing SQL: $sqlWithParams")
      rs = pstmt.executeQuery()
      val fieldNumber = rs.getMetaData.getColumnCount
      while (rs.next()) {
        val temp = ArrayBuffer[String]()
        for(i <- 1 to fieldNumber){
          temp.append(rs.getString(i))
        }
        queryResult.append(temp)
      }
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    queryResult
  }

  //判断记录是否存在
//  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
//    var flag: Boolean = false
//    var pstmt: PreparedStatement = null
//    try {
//      pstmt = connection.prepareStatement(sql)
//      for (i <- params.indices) {
//        pstmt.setObject(i + 1, params(i))
//      }
//      flag = pstmt.executeQuery().next()
//      pstmt.close()
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//    flag
//  }

}
