package com.huyong.bigdata.spark.excel

import java.io.{File, FileInputStream}
import java.util

import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yonghu on 2020/7/7.
  */
object ExcelReaderScala {

  private val conf: SparkConf = new SparkConf().setAppName("Task1").setMaster("local")
  private val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  val peopleSchema = StructType(Array(
    StructField("Name", StringType, nullable = false),
    StructField("Age", DoubleType, nullable = false),
    StructField("Occupation", StringType, nullable = false),
    StructField("Date of birth", StringType, nullable = false)))

  def main(args: Array[String]): Unit = {

    val myFile = new File("/Users/yonghu/Downloads/工资表.xlsx")
    val fis = new FileInputStream(myFile)

    val myWorkbook = new XSSFWorkbook(fis)

    val mySheet = myWorkbook.getSheetAt(0)

    val lastNum=mySheet.getLastRowNum
    val schemaList  =  ArrayBuffer[String]()
    val dataList =  ArrayBuffer[org.apache.spark.sql.Row]();
    val a = 4;

      val row=mySheet.getRow(a-1)
      val iterator = row.cellIterator()
      while(iterator.hasNext){
        val  currentCell = iterator.next()
        if(currentCell.getCellTypeEnum() == CellType.STRING) schemaList.append(currentCell.getStringCellValue().trim());
        //数值
        if(currentCell.getCellTypeEnum() == CellType.NUMERIC)  schemaList.append((currentCell.getNumericCellValue()+"").trim());

      }

      for (a<- 3 to lastNum){
        //一行数据做成一个List
        val rowDataList = new ArrayBuffer[String]
        //获取一行数据
        val r = mySheet.getRow(a);
        if (r != null) {
          //根据字段数遍历当前行的单元格
          for (cn <- 0 to schemaList.length) {
            val c = r.getCell(cn)
            if (c == null) rowDataList.append("0"); //空值简单补零
            if (c != null && c.getCellTypeEnum() == CellType.STRING) rowDataList.append(c.getStringCellValue().trim()); //字符串
            if (c != null && c.getCellTypeEnum() == CellType.NUMERIC) {
                val value = c.getNumericCellValue();
                rowDataList.append(value + ""); //不保留小数点

            }
          }
        }
        //dataList数据集添加一行
        //dataList.append(rowDataList.toArray);
      }



    }


}