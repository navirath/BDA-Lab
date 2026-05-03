import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create Spark Session
val spark = SparkSession.builder
  .appName("TextCleaningStream")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Read streaming data from socket
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

// Convert to Dataset[String]
val words = lines.as[String]

// Define simple stop words
val stopWords = Seq("is", "the", "a", "an", "and", "to")

// Cleaning function
val cleaned = words.map { line =>
  // Lowercase
  val lower = line.toLowerCase

  // Remove extra spaces
  val noSpace = lower.trim.replaceAll("\\s+", " ")

  // Remove punctuation
  val noPunct = noSpace.replaceAll("[^a-zA-Z ]", "")

  // Remove stop words
  val filtered = noPunct.split(" ")
    .filter(word => !stopWords.contains(word))

  // Simple lemmatization (basic example)
  val lemmatized = filtered.map {
    case w if w.endsWith("ing") => w.dropRight(3)
    case w if w.endsWith("ed")  => w.dropRight(2)
    case w => w
  }

  lemmatized.mkString(" ")
}

// Output to console
val query = cleaned.writeStream
  .outputMode("append")
  .format("console")
  .start()

query.awaitTermination()
