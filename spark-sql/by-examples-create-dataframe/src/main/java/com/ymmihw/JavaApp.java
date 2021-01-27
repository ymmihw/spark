package com.ymmihw;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.guava.collect.Lists;
import scala.Function1;

public class JavaApp {
  public static void main(String[] args) {
    SparkSession sparkSession =
        SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
    SparkContext sparkContext = sparkSession.sparkContext();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkContext);
    List<List<String>> data = Lists.newArrayList(Lists.newArrayList("Java", "20000"),
        Lists.newArrayList("Python", "100000"), Lists.newArrayList("Scala", "3000"));

    JavaRDD<List<String>> javaRddStrings = jsc.parallelize(data);

    JavaRDD<Row> javaRdd = javaRddStrings.map(e -> RowFactory.create(e.get(0), e.get(1)));

    List<StructField> schemaFields = new ArrayList<>();
    schemaFields.add(DataTypes.createStructField("language", DataTypes.StringType, true));
    schemaFields.add(DataTypes.createStructField("users_count", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(schemaFields);

    RDD<Row> rdd = JavaRDD.toRDD(javaRdd);


    Dataset<Row> dfFromRDD1 = sparkSession.createDataFrame(rdd, schema);

    dfFromRDD1.printSchema();

    Dataset<Row> dfFromJavaRDD = sparkSession.createDataFrame(javaRdd, schema);

  }
}
