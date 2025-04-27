package com.zenitera

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.xerial.snappy.Snappy
import utils.propertesUtils.getParameter
import utils.toolsUtils.combineData

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, PreparedStatement}
import scala.collection.JavaConverters.asScalaSetConverter

object hbase_2doris {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)
    // HBase configuration
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", getParameter("hbase_zookeeper_quorum"))
    conf.set("hbase.client.scanner.timeout.period", getParameter("client_scanner_timeout"))
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.master.kerberos.principal", getParameter("hbase_master_kerberos_principal"))
    conf.set("hbase.regionserver.kerberos.principal", getParameter("hbase_regionserver_kerberos_principal"))
    conf.set("hbase.security.authentication", "kerberos")
    conf.set("HADOOP_USER_NAME", getParameter("hadoop_user_name"))
    conf.set("user.name", getParameter("user_name"))
    conf.addResource(new Path(getParameter("hbase_site_xml")))
    conf.addResource(new Path(getParameter("core_site_xml")))

    // Kerberos configuration
    System.setProperty("java.security.krb5.conf", getParameter("krb5_conf_path"))
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(getParameter("keytab_name"), getParameter("keytab_file_path"))

    //    op_hbase_2local.handleData(conf)
    println("help: example: java -cp xxx.jar com.classname")
    val vars_columns_str = getParameter("vars_columns")
    val vars_columns = vars_columns_str.split(",")
    val num_columns_int = getParameter("num_columns")

    val numVariables = num_columns_int.toInt
    val jsonObject = new JSONObject()
    for (i <- 0 until numVariables) {
      val variableName = s"variable_${i + 1}" // 按顺序生成变量
      val variableValue = vars_columns.apply(i) // 给对应的变量赋值
      jsonObject.put(variableName, variableValue)
    }

    hbase_2mysql.handleData(conf, jsonObject)
  }

  def handleData(conf: Configuration, jsonObject: JSONObject): Unit = {
    // Create HBase connection
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val tableName: TableName = TableName.valueOf(getParameter("table_name"))
    val table: Table = connection.getTable(tableName)
    val jsonContent = new JSONObject()
    val error_rowkeys_path = Paths.get(getParameter("error_rowkey_path"))
    val error_rowkeys_path_str = getParameter("error_rowkey_path")
    val decompress_enable = getParameter("decompress_enable")
    val compress_type = getParameter("compress_type")
    val compress_field: List[String] = getParameter("compress_field").split(",").toList
    // Mysql JDBC URL
    val jdbc_host = getParameter("jdbc_host")
    val jdbc_port = getParameter("jdbc_port")
    val database = getParameter("database")
    val username = getParameter("username")
    val password = getParameter("password")
    val jdbcUrl = s"jdbc:mysql://${jdbc_host}:${jdbc_port}/${database}?useServerPrepStmts=true&useLocalSessionState=true&rewriteBatchedStatements=true&cachePrepStmts=true&prepStmtCacheSqlLimit=99999&prepStmtCacheSize=50&sessionVariables=group_commit=async_mode"
    val conn = DriverManager.getConnection(jdbcUrl, username, password)
    var line_cnt = 0
    val sql = getParameter("sql")
    val statement: PreparedStatement = conn.prepareStatement(sql)
    // 创建一个新的Scan实例
    val scan = new Scan().setTimeRange(getParameter("start_time").toLong, getParameter("end_time").toLong)

    // 执行Scan操作
    val scanner = table.getScanner(scan)
    try {
      val keySet = jsonObject.keySet()
      // 定义正则表达式，用于匹配字符串中的数字部分
      val regex = """\d+""".r
      val sortedKeys = keySet.asScala.toSet.toList.sorted
      // 对字符串列表进行排序
      val sortedStrings = sortedKeys.sortBy { str =>
        // 从字符串中提取数字部分
        regex.findFirstIn(str).map(_.toInt).getOrElse(0)
      }
      val rowIterator = scanner.iterator()
      while (rowIterator.hasNext) {
        val result = rowIterator.next()
        // 获取行键
        val rowKey = result.getRow
        val id = Bytes.toString(rowKey)
        jsonContent.put("id", id)
        if (decompress_enable == "true") {
          for (key <- sortedStrings) {
            val value = jsonObject.get(key).toString
            try {
              var data = result.getValue(Bytes.toBytes(getParameter("column_family")), Bytes.toBytes(s"${value}"))
              if (data.isEmpty) {
                data = Bytes.toBytes("NULL")
              }
              if (compress_type == "snappy") {
                if (compress_field.contains(value)) {
                  var data_fix: Array[Byte] = Bytes.toBytes("NULL")
                  try {
                    if (data.isEmpty) {
                      data_fix = Bytes.toBytes("NULL")
                    } else {
                      data_fix = Snappy.uncompress(data)
                    }
                  } catch {
                    case e: Exception => Files.write(error_rowkeys_path, combineData(error_rowkeys_path_str, Bytes.toString(rowKey)).getBytes(StandardCharsets.UTF_8))
                      //这里可以记录更详细的错误日志，便于排查问题，比如使用日志框架记录异常信息
                      println(s"Error decompressing data: ${e.getMessage}")
                  }
                  jsonContent.put(value, Bytes.toString(data_fix).replaceAll("[\u0000-\u000F]", "").replaceAll("\uFFFD", ""))
                } else {
                  jsonContent.put(value, Bytes.toString(data).replaceAll("[\u0000-\u000F]", "").replaceAll("\uFFFD", ""))
                }
              } else if (compress_type == "zip") {
                if (compress_field.contains(value)) {
                  var data_fix: Array[Byte] = Bytes.toBytes("NULL")
                  try {
                    if (data.isEmpty) {
                      data_fix = Bytes.toBytes("NULL")
                    } else {
                      data_fix = utils.decompressUtils.ZipDecompress(data, id)
                    }
                  } catch {
                    case e: Exception => Files.write(error_rowkeys_path, combineData(error_rowkeys_path_str, Bytes.toString(rowKey)).getBytes(StandardCharsets.UTF_8))
                      //这里可以记录更详细的错误日志，便于排查问题，比如使用日志框架记录异常信息
                      println(s"Error decompressing data: ${e.getMessage}")
                  }
                  jsonContent.put(value, Bytes.toString(data_fix).replaceAll("[\u0000-\u000F]", "").replaceAll("\uFFFD", ""))
                } else {
                  jsonContent.put(value, Bytes.toString(data).replaceAll("[\u0000-\u000F]", "").replaceAll("\uFFFD", ""))
                }
              } else {
                jsonContent.put(value, Bytes.toString(data).replaceAll("[\u0000-\u000F]", "").replaceAll("\uFFFD", ""))
              }
            } catch {
              case e: Exception => null
            }

          }

        }
        try {
          var index = 0
          index += 1
          statement.setString(index, id)
          for (key <- sortedStrings) {
            index += 1
            val col_nm = jsonObject.get(key)
            val col_data = jsonContent.getString(col_nm.toString)
            statement.setString(index, col_data)
          }
          statement.addBatch()
          line_cnt += 1
          if (line_cnt == getParameter("batch_size").toInt) {
            utils.toolsUtils.saveMysql(statement)
            jsonContent.clear()
            line_cnt = 0
          }
        } catch {
          case e: Exception => Files.write(error_rowkeys_path, combineData(error_rowkeys_path_str, Bytes.toString(rowKey)).getBytes(StandardCharsets.UTF_8))
        }
      }
      if (line_cnt != 0) {
        utils.toolsUtils.saveMysql(statement)
      }


    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      // Close resources
      if (table != null) table.close()
      if (connection != null) connection.close()
      if (statement != null) statement.close()
      if (conn != null) conn.close()

    }
  }

}
