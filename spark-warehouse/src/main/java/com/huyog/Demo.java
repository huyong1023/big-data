package com.huyog;


import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
/**
 * Created by yonghu on 2020/7/7.
 */

public class Demo {

    /**
    cp /Users/yonghu/.m2/repository/org/apache/poi/poi/3.17/poi-3.17.jar ./Downloads
    cp /Users/yonghu/.m2/repository/org/apache/poi/poi-ooxml/3.17/poi-ooxml-3.17.jar ./Downloads
    cp /Users/yonghu/.m2/repository/org/apache/poi/poi-ooxml/3.17/poi-ooxml-3.17.jar ./Downloads
    cp /Users/yonghu/.m2/repository/org/apache/xmlbeans/xmlbeans/3.1.0/xmlbeans-3.1.0.jar ./Downloads
    cp /Users/yonghu/.m2/repository/org/apache/commons/commons-collections4/4.1/commons-collections4-4.1.jar ./Downloads/spark-2.4.5-bin-hadoop2.7/jars/
    cp /Users/yonghu/.m2/repository/org/apache/poi/poi-ooxml-schemas/3.17/poi-ooxml-schemas-3.17.jar ./Downloads/spark-2.4.5-bin-hadoop2.7/jars
**/

    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private static final Pattern p = Pattern.compile("\\d+.0$");
    public static final SparkSession spark;
    DecimalFormat format = new DecimalFormat();


    //初始化SparkSession
    static {
        //System.setProperty("hadoop.home.dir", HADOOP_HOME);
        spark = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", SPARK_HOME)
                .config("spark.sql.parquet.binaryAsString", "true")
                .getOrCreate();
    }

    public static void main(String[] args) throws Exception {
        //需要查询的excel路径
        String xlsxPath = "/Users/yonghu/Downloads/人工成本项目分摊表.xlsx";
        String xlsxPath2 = "/Users/yonghu/Downloads/工资表.xlsx";
        //定义表名
        String tableName1 = "timeSheet";
        String tableName2 = "salary";
        Demo demo = new Demo();
        ArrayList<CellDef> schemaList = new ArrayList<CellDef>();
        CellDef def = new CellDef("项目号", 1);
        schemaList.add(def);
        demo.readExcel(xlsxPath, tableName1, schemaList, 1, 3, 4);
        demo.readExcel(xlsxPath2, tableName2, null, 3, 0, 4);

        Dataset salary  = spark.sql("select * from " + tableName2 + "Detail ");

        Map<String, String> slMap = new HashMap<String, String>();
        List<Row> ls = salary.collectAsList();
        for (Row row : ls) {
            slMap.put(row.getString(3), row.getString(15));
        }
        Dataset teenagerDF = spark.sql("select * from " + tableName1 + "Detail ");

/*
        StructType structType = teenagerDF.schema();
        String[] names = structType.fieldNames();
        StringBuffer sb = new StringBuffer("select ").append("`").append(names[0]).append("`, ").append("stack(").append(names.length - 1);
        for (int i = 1; i < names.length; i++){
            String name = names[i];
            sb.append(", '").append(name).append("'").append(", `").append(name).append("`");
        }
        sb.append(") from ").append(tableName1).append("Detail ");;
        System.out.println(sb.toString());


        spark.sql(sb.toString()).show();
        */


        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();
        //将RDD  中的数据进行映射转换成为Student
        JavaRDD<Row> result = teenagerRDD.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                ArrayList<String> rowDataList = new ArrayList<String>();

                StructType structType = row.schema();
                String[] names = structType.fieldNames();
                StringBuffer sb = new StringBuffer();
                BigDecimal count = new BigDecimal(0);
                for (int i = 1 ; i < names.length; i++){
                    String sal = slMap.get(names[i]) ;
                    String workload = row.getString(i);
                    if(null != sal ){
                        BigDecimal salB = new BigDecimal(sal);

                        BigDecimal workloadB = new BigDecimal(workload);
                        if (workloadB.compareTo(new BigDecimal(0)) > 0 ){
                            count.add(salB.multiply(workloadB));
                        }
                    } else {
                        logger.info(names[i] + "salary is null");
                    }
                }
                rowDataList.add(row.get(0).toString());
                //rowDataList.add("");
                rowDataList.add(count.toString());
//                dept.setName(row.get(0).toString());
//                dept.setValue("");
//                dept.setTotal(count.toString());
                return RowFactory.create(rowDataList.toArray());
                //return dept;
            }
        });


        StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
        //StructField ageField = DataTypes.createStructField("value", DataTypes.StringType, true);

// 创建 location的结构
        StructField cityField = DataTypes.createStructField("total", DataTypes.StringType, true);

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(nameField);
        //fields.add(ageField);
        fields.add(cityField);
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.createDataFrame(result, schema);
        //spark.createDataFrame(result, schema).createOrReplaceTempView(tableName + dataTypeSheet.getSheetName());

        df.coalesce(1).write().mode(SaveMode.Append).option("header", "true").csv("/Users/yonghu/Downloads/result");


        /*List<StructField> schemaFields = new ArrayList<StructField>();
        schemaFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("totel", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(schemaFields);Zg干

        Dataset<Dept> df = spark.createDataFrame((List<Row>) result, schema);

        result.select("year", "model").save("newcars.csv", "com.databricks.spark.csv");

        result.saveAsTextFile("/Users/yonghu/Downloads/result");*/
        /*List<String> dataList = new ArrayList<String>();
        result.foreach(new VoidFunction<Dept>() {
            @Override
            public void call(Dept student) throws Exception {
                dataList.add(student.toString());
                exportCsv(new File("/Users/yonghu/Downloads/result/res.csv"), dataList);
            }
        });*/


    }

    public void readExcel(String filePath, String tableName, ArrayList<CellDef> schemaList, int headRow, int headCell, int startCell) throws IOException {

        File file = new File(filePath);
        InputStream inputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        Workbook workbook = null;
        if (file.getName().contains("xlsx"))
            workbook = new XSSFWorkbook(bufferedInputStream);
        if (file.getName().contains("xls") && !file.getName().contains("xlsx"))
            workbook = new HSSFWorkbook(bufferedInputStream);
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        Iterator<Sheet> dataTypeSheets = workbook.sheetIterator();
        while (dataTypeSheets.hasNext()) {
            if (null == schemaList) {
                schemaList = new ArrayList<CellDef>();
            }
            ArrayList<org.apache.spark.sql.Row> dataList = new ArrayList<org.apache.spark.sql.Row>();
            List<StructField> fields = new ArrayList<>();

            //获取当前sheet
            Sheet dataTypeSheet = dataTypeSheets.next();
            Iterator<org.apache.poi.ss.usermodel.Row> iterator = dataTypeSheet.iterator();
            if (!iterator.hasNext()) continue;
            Iterator<Cell> firstRowCellIterator = dataTypeSheet.getRow(headRow).iterator();
            int temp = 1;
            while (firstRowCellIterator.hasNext()) {
                Cell currentCell = firstRowCellIterator.next();
                if (temp > headCell) {
                    CellDef def = null;
                    if (currentCell.getCellTypeEnum() == CellType.STRING) {
                        def = new CellDef(currentCell.getStringCellValue().trim(), currentCell.getColumnIndex());
                    } else if (currentCell.getCellTypeEnum() == CellType.NUMERIC) {
                        def = new CellDef(getNUMERIC(currentCell), currentCell.getColumnIndex());
                    }
                    if (null != def) {
                        schemaList.add(def);
                    }
                }

                temp++;
            }


            for (CellDef def : schemaList) {
                StructField field = DataTypes.createStructField(def.getName(), DataTypes.StringType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);
            int len = schemaList.size();
            int rowEnd = dataTypeSheet.getLastRowNum();
            for (int rowNum = startCell; rowNum <= rowEnd; rowNum++) {
                ArrayList<String> rowDataList = new ArrayList<String>();
                org.apache.poi.ss.usermodel.Row r = dataTypeSheet.getRow(rowNum);
                if (r != null) {
                    for (int cn = 0; cn < len; cn++) {
                        CellDef def = schemaList.get(cn);
                        Cell c = r.getCell(def.getCellNumber(), org.apache.poi.ss.usermodel.Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                        if (c == null) {
                            rowDataList.add("0");//空值简单补零
                        } else if (c != null && c.getCellTypeEnum() == CellType.STRING) {
                            rowDataList.add(c.getStringCellValue().trim());
                        } else if (c != null && c.getCellTypeEnum() == CellType.NUMERIC) {
                            rowDataList.add(getNUMERIC(c));
                        } else if (c != null && c.getCellTypeEnum() == CellType.BOOLEAN) {
                            c.getBooleanCellValue();
                        } else if (c != null && c.getCellTypeEnum() == CellType.FORMULA) {
                            String str = getFORMULA(c, evaluator);
                            rowDataList.add(str);
                        } else {
                            rowDataList.add("0");//空值简单补零
                        }

                    }
                }
                dataList.add(RowFactory.create(rowDataList.toArray()));
            }
            spark.createDataFrame(dataList, schema).createOrReplaceTempView(tableName + dataTypeSheet.getSheetName());
        }
    }


    public String getFORMULA(Cell cell, FormulaEvaluator evaluator) {
        String res = "0";
        CellValue cellValue = null;
        try {
            cellValue = evaluator.evaluate(cell);
        } catch (Exception e) {
            logger.warn("cell" + cell, e.getMessage());
        }
        try {
            if (null == cellValue || cellValue.getCellTypeEnum() == CellType.NUMERIC) {
                return getNUMERIC(cell);
            }
        } catch (Exception e) {
            logger.warn("get numeric for cell" + cell, e.getMessage());
        }
        try {
            if (null == cellValue || cellValue.getCellTypeEnum() == CellType.STRING) {
                return cell.getStringCellValue().trim();

            }
        } catch (Exception e) {
            logger.warn("get string for cell " + cell, e.getMessage());
        }


        return res;
    }

    private String getNUMERIC(Cell cell) {
        return getNUMERIC(cell, "");
    }

    private String getNUMERIC(Cell cell, String formatStr) {
        if (null != formatStr && !formatStr.trim().equals("")) {
            format.applyPattern(formatStr);
        } else {
            format.applyPattern("#");
        }


        double value = cell.getNumericCellValue();
        if (p.matcher(value + "").matches())
            return format.format(value);//不保留小数点
        if (!p.matcher(value + "").matches())
            return value + "";//保留小数点
        return "0";

    }
}

class CellDef {
    private String name;
    private int cellNumber;


    public CellDef(String name, int cellNumber) {
        this.name = name;
        this.cellNumber = cellNumber;
    }



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCellNumber() {
        return cellNumber;
    }

    public void setCellNumber(int cellNumber) {
        this.cellNumber = cellNumber;
    }
}