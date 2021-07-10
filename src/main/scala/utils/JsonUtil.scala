package utils
import org.json.{JSONObject,XML}
/**
 * @Classname JsonUtil
 * @Description JSON工具类
 * @Date 2021-07-10 11:35
 * @Created by 忘尘
 */
object JsonUtil {
  /**
   * 将XML字符串转换为JSON字符串
   * @param xmlStr
   * @return
   */
  def xml2jsonString(xmlStr:String):String= {
      XML.toJSONObject(xmlStr).toString
  }

}
