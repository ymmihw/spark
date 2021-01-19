package com.ymmihw;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaApp {

  public static void main(String[] args) throws AnalysisException {
    SparkConf sparkConf = new SparkConf().setAppName("AggregateAction").setMaster("local[*]");
    SparkSession spark =
        SparkSession.builder().appName("Simple Application").config(sparkConf).getOrCreate();
    JavaRDD<String> javaRDD = spark.sparkContext()
        .textFile(JavaApp.class.getClassLoader().getResource("employee.txt").toString(), 1)
        .toJavaRDD();
    JavaRDD<JavaEmployee> javaEmployees = javaRDD.map(e -> e.split(",")).map(
        e -> new JavaEmployee(Integer.parseInt(e[0].trim()), e[1], Integer.parseInt(e[2].trim())));

    Dataset<Row> dfs = spark.createDataFrame(javaEmployees, JavaEmployee.class);
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
