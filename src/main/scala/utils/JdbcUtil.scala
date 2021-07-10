package utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSourceFactory
/**
 * @Classname JdbcUtil
 * @Description jdbc连接池
 * @Date 2021-07-09 16:51
 * @Created by 忘尘
 */
object JdbcUtil {

  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", config.getProperty("jdbc.driverClassName"))
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive",
      config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  def main(args: Array[String]): Unit = {
    val conn = this.getConnection
    val stmt = conn.createStatement
    val con = stmt.executeQuery("select * from kafkaoutmsgs")
    while(con.next()){
      println(JsonUtil.xml2jsonString(con.getString("KAFKAOUTMSGS_CLOB_MSG")))
    }
  }
}
