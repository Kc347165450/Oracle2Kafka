package send

import utils.{JdbcUtil, JsonUtil, PropertiesUtil}
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.sql.ResultSet
import java.text.SimpleDateFormat

/**
 * @Classname Oracle2Kafka
 * @Description 推送Oracle表数据到kafka topic
 * @Date 2021-07-10 11:56
 * @Created by 忘尘
 */
object Oracle2Kafka {
  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建 Kafka 生产者
    new KafkaProducer[String, String](prop)
  }
  def main(args: Array[String]): Unit = {
    // 获取配置文件 config.properties 中的 Kafka 配置参数
    val config: Properties = PropertiesUtil.load("config.properties")
    val broker: String = config.getProperty("kafka.broker.list")
    val topic = "kctest"
    // 获取配置文件 config.properties 中的 Oracle表名

    val table = config.getProperty("table.name")
    val table_col = config.getProperty("table.col")
    val table_filter = config.getProperty("table.filter")
    val table_update = config.getProperty("table.update")


    // 创建 Kafka 消费者
    val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer(broker)
    // 获取Oracle连接
    val ocon = JdbcUtil.getConnection
    val stmt = ocon.createStatement
    var data : ResultSet = null
    //
    var count = 0
    while (true) {
      count = 0
      // 随机产生实时数据并通过 Kafka 生产者发送到 Kafka 集群中
      data = null
      data = stmt.executeQuery(s"select * from ${table} where ${table_filter} IS NULL")
      var xmlstr = ""
      var jsonstr = ""
      var ids = ""
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = dateFormat.format(now)
      println(s"${date}：开始发送消息")
      while(data.next()) {
        xmlstr = data.getString(table_col)
        ids += data.getString(table_update) + ","
        jsonstr = JsonUtil.xml2jsonString(xmlstr)
        kafkaProducer.send(new ProducerRecord[String, String](topic, jsonstr))
        println(date + "-" + jsonstr)
        count += 1
      }
      if(ids != ""){
        ids = ids.substring(0,ids.length-1)
        val sql = s"update ${table} set ${table_filter} = to_date('${date}','yyyy-MM-dd HH24:mi:ss') where ${table_update} IN (${ids})"
        stmt.executeUpdate(sql)
      }
      println(s"本次发送消息数量：${count}")
      Thread.sleep(60*1000)
    }
  }
}
