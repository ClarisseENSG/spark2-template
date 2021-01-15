package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val francePopulationDataFrame = spark.read.json("data/input/demographie_par_commune.json")
    francePopulationDataFrame.show

    // 1
    francePopulationDataFrame.select(sum($"Population")).show

    // 2
    val departPopulation = francePopulationDataFrame
      .groupBy($"Departement")
      .agg(sum($"Population").as("populations"))
      .orderBy($"populations".desc)

    departPopulation.show

    // 3
    val departement = spark.read
      .csv("data/input/departements.txt")
      .toDF("name", "Departement")

    departPopulation.join(departement, "Departement").show

  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    // 1
    toursDF.groupBy($"tourDifficulty").count().show

    // 2
    toursDF.agg(min($"tourPrice")).show()
    toursDF.agg(max($"tourPrice")).show()
    toursDF.agg(avg($"tourPrice")).show()

    // 3
    val minTourDifficulty = toursDF.groupBy($"tourDifficulty").agg(min($"tourPrice"))
    val maxTourDifficulty = toursDF.groupBy($"tourDifficulty").agg(max($"tourPrice"))
    val avgTourDifficulty = toursDF.groupBy($"tourDifficulty").agg(avg($"tourPrice"))

    minTourDifficulty.show()
    maxTourDifficulty.show()
    avgTourDifficulty.show()

    // 4
    minTourDifficulty
      .join(toursDF.groupBy($"tourDifficulty").agg(min($"tourLength")), "tourDifficulty").show()
    maxTourDifficulty
      .join(toursDF.groupBy($"tourDifficulty").agg(max($"tourLength")), "tourDifficulty").show()
    avgTourDifficulty
      .join(toursDF.groupBy($"tourDifficulty").agg(avg($"tourLength")), "tourDifficulty").show()

    // 5
    val exploded = toursDF.withColumn("tourTags", explode($"tourTags"))
    exploded.groupBy($"tourTags").count().orderBy($"count".desc).show(10)

    // 6
    exploded.groupBy($"tourTags", $"tourDifficulty").count().orderBy($"count".desc).show(10)

    // 7
    exploded.groupBy($"tourTags", $"tourDifficulty").agg(min($"tourPrice")).orderBy("min(tourPrice)").show(1)
    exploded.groupBy($"tourTags", $"tourDifficulty").agg(max($"tourPrice")).orderBy($"max(tourPrice)".desc).show(1)
    exploded.groupBy($"tourTags", $"tourDifficulty").agg(avg($"tourPrice")).orderBy($"avg(tourPrice)").show()

  }
}
