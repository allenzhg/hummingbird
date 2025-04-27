package utils

import io.minio.errors.MinioException
import io.minio.{MinioClient, PutObjectArgs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import utils.propertesUtils.getParameter

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.PreparedStatement
import scala.io.Source

object toolsUtils {
  def saveToMinio(id: String, data: String, minio_client: MinioClient, bucket_name: String, minio_dir: String): Unit = {
    try {
      val byteArrayInputStream: ByteArrayInputStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))
      // 将字节流写入Minio对象存储
      minio_client.putObject(PutObjectArgs.builder.bucket(s"${bucket_name}")
        .`object`(s"${minio_dir}/${id}.json")
        .stream(byteArrayInputStream, byteArrayInputStream.available, -(1))
        .build)
      byteArrayInputStream.close()
    } catch {
      case (e: MinioException) =>
        println("Error occurred: " + e);
        println("HTTP trace: " + e.httpTrace())
    }

  }

  def saveImpala(statement: PreparedStatement): Unit = {
    Logger.getLogger("com.cloudera.impala").setLevel(Level.DEBUG)
    System.setProperty("java.security.krb5.conf", getParameter("krb5_conf_path"))
    val driverClass = "com.cloudera.impala.impala.common.ImpalaJDBC42Driver"
    val conf: Configuration = new Configuration()
    conf.set("hadoop.security.authentication", "Kerberos")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(getParameter("keytab_name"), getParameter("keytab_file_path"))

    try {
      // 加载JDBC驱动
      Class.forName(driverClass)
      statement.executeBatch()

    } catch {
      case e: Exception =>
        println("error..")
        e.printStackTrace()
    }
  }

  def saveHive(statement: PreparedStatement): Unit = {
    Logger.getLogger("org.apache.hive").setLevel(Level.DEBUG)
    System.setProperty("java.security.krb5.conf", getParameter("krb5_conf_path"))
    val driverClass = "org.apache.hive.jdbc.HiveDriver"
    val conf: Configuration = new Configuration()
    conf.set("hadoop.security.authentication", "Kerberos")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(getParameter("keytab_name"), getParameter("keytab_file_path"))

    try {
      // 加载JDBC驱动
      Class.forName(driverClass)
      statement.executeBatch()

    } catch {
      case e: Exception =>
        println("error..")
        e.printStackTrace()
    }
  }

  def saveMysql(statement: PreparedStatement): Unit = {
    try {
      // 加载JDBC驱动
      val driverClass = "com.mysql.cj.jdbc.Driver"
      Class.forName(driverClass)
      statement.executeBatch()
    } catch {
      case e: Exception =>
        println("error..")
        e.printStackTrace()
    }
  }

  def combineData(filePath: String, dataAppend: String): String = {
    try {
      val existingContent = Source.fromFile(filePath).mkString
      val content = existingContent.replaceAll("[\u0000-\u000F]", "") + "," + dataAppend
      content
    } catch {
      case e: java.io.FileNotFoundException =>
        println("File not found: " + e.getMessage)
        dataAppend
      case e: java.io.IOException =>
        println("An error occurred using a mix of Scala and Java: " + e.getMessage)
        dataAppend
    } finally {

    }

  }

}
