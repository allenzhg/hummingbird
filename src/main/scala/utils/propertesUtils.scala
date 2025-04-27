package utils

import java.io.FileInputStream
import java.util.Properties

object propertesUtils {
  val props = new Properties()

  def getParameter(s: String): String = {
    props.load(new FileInputStream("config.properties"))
    props.getProperty(s)
    //防止读取出的中文出现乱码
    //new String( props.getProperty(s).getBytes("ISO-8859-1"), "utf-8")
  }

}
