package com.ymmihw;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaApp {

  public static void main(String[] args) throws AnalysisException {
    SparkConf sparkConf = new SparkConf().setAppName("AggregateAction").setMaster("local[*]");
    SparkSession spark =
        SparkSession.builder().appName("Simple Application").config(sparkConf).getOrCreate();
    JavaRDD<String> employee = spark.sparkContext()
        .textFile(JavaApp.class.getClassLoader().getResource("employee.txt").toString(), 1)
        .toJavaRDD();

    JavaRDD<Row> rdd = employee.map(e -> e.split(",")).map(
        e -> RowFactory.create(Integer.parseInt(e[0].trim()), e[1], Integer.parseInt(e[2].trim())));

    List<StructField> schemaFields = new ArrayList<>();
    schemaFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
    schemaFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    schemaFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
    StructType schema = DataTypes.createStructType(schemaFields);

    Dataset<Row> dfs = spark.createDataFrame(rdd, schema);
    dfs.createTempView("employee");

    Dataset<Row> allRecords = spark.sql("SELECT * FROM employee");
    System.out.println("=========allRecords=========");
    allRecords.show();

    Dataset<Row> ageFilter = spark.sql("SELECT * FROM employee WHERE age >= 20 AND age <= 35");
    System.out.println("=========ageFilter=========");
    ageFilter.show();

    System.out
        .println("=========Fetch ID values from agefilter DataFrame using column index=========");
    ageFilter.toJavaRDD().map(t -> "ID: " + t.getAs(0)).collect().forEach(System.out::println);
  }
}
