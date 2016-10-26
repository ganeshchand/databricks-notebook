// Databricks notebook source exported at Wed, 26 Oct 2016 23:35:59 UTC
val ds = spark.range(1,10)

// COMMAND ----------

val newds = ds.map( x => (x, Array.fill(5)("This is a test")))

// COMMAND ----------

newds.collect

// COMMAND ----------

newds.map(x => x._2.length).collect

// COMMAND ----------

newds.flatMap(x => x._2.mkString(" ").split(" ")).collect

// COMMAND ----------

val empsRdd = spark.sparkContext.parallelize(
      """{"id": 1, "employee":[{ "name":"joe", "age":21}, { "name":"Jane", "age":21}]}""" :: Nil
    )

// COMMAND ----------

val empsDF = spark.read.json(empsRdd)
empsDF.printSchema() 

// COMMAND ----------

   import org.apache.spark.sql.types._
    val empsSchema = StructType(StructField(
        "team", StructType(
          Seq(StructField(
            "employee", ArrayType(StructType
            (Array
            (StructField("name", StringType, true),
              StructField("age", IntegerType, true)
            ))))))) :: Nil)

// COMMAND ----------

spark.createDataset(empsDF, Encoders.ST)

// COMMAND ----------

case class Employee(age: Long, name: String)
case class Employees(id: Long, employees: Array[Employee])

// COMMAND ----------

case class Employee(age: Long, name: String)
case class Employees(id: Long, employees: Array[Employee])

val empsRdd = spark.sparkContext.parallelize(
      """{"id": 1, "employee":[{ "name":"joe", "age":21}, { "name":"Jane", "age":21}]}""" :: Nil
    )
val empDS = Seq(Employees(1, Array(Employee(25, "andy chan"), Employee(26, "bill smith")))).toDS()

// COMMAND ----------

empDS.printSchema

// COMMAND ----------

display(empDS)

// COMMAND ----------

(display(empDS.flatMap(_.employees)))

// COMMAND ----------

empDS.select("employees.name").flatMap(name => name.toString.split(" "))

// COMMAND ----------

empDS.select("employees.age").show

// COMMAND ----------

empsDF.select("team.employee")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Using Explode and Flatmap with Dataset

// COMMAND ----------

case class Book(title: String, words: String)

// COMMAND ----------

val bookDS = spark.sparkContext.parallelize(Seq(Book("java", "Programming with Java8"), Book("scala", "Programming with Scala"))).toDS()

// COMMAND ----------

display(bookDS)

// COMMAND ----------

display(bookDS.flatMap(x => x.words.split(" "))) // flatMap

// COMMAND ----------

import org.apache.spark.sql.functions._
// display(bookDS.select('title, explode(split('words, " ")).as("word")))
display(bookDS.select(explode(split('words, " ")).as("word")))

// COMMAND ----------

val flattendBookDF = bookDS.select('title, explode(split('words, " ")).as("words"))

// COMMAND ----------

val flattendBookDs = flattendBookDF.as[Book]

// COMMAND ----------

bookDS.flatMap(x => x.words.split(" "))

// COMMAND ----------

