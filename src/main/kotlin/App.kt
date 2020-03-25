import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import java.util.*

fun main() = SparkSession.Builder()
    .appName("Analyse temperature")
    .master("local[4]") // local spark master
    .orCreate
    .use {
        MyJob(it).start()
    }


class MyJob(session: SparkSession) {

    // In my case I use local PostgreSQL database
    private val jdbcUrl = "jdbc:postgresql://localhost:5432/jetdb"
    private val connectionProperties = Properties().also {
        it["user"] = "postgres"
        it["password"] = "some_password"
        it["driver"] = "org.postgresql.Driver"
    }

    // Loading csv into dataframe
    private val df = session.read()
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(javaClass.getResource("GlobalLandTemperaturesByMajorCity.csv").path)
        .filter(col("AverageTemperature").isNotNull)


    fun start() {
        // Cities
        cityTempByYear()
        cityTempByCentury()
        cityTempOverall()

        // Countries
        countryTempByYear()
        countryTempByCentury()
        countryTempOverall()
    }

    private fun cityTempByYear() {
        df.groupBy(
                col("City"),
                year(col("dt")).alias("Year")
            )
            .agg(
                mean("AverageTemperature").alias("MeanAverageTemperature"),
                min("AverageTemperature").alias("MinAverageTemperature"),
                max("AverageTemperature").alias("MaxAverageTemperature")
            )
            .sort("City", "Year")
            .write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "CityTempByYear", connectionProperties)
    }

    private fun cityTempByCentury() {
        df.groupBy(
                col("City"),
                floor(year(col("dt")).divide(100)).plus(1).alias("Century")
            )
            .agg(
                mean("AverageTemperature").alias("MeanAverageTemperature"),
                min("AverageTemperature").alias("MinAverageTemperature"),
                max("AverageTemperature").alias("MaxAverageTemperature")
            )
            .sort("City", "Century")
            .write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "CityTempByCenturies", connectionProperties)
    }

    private fun cityTempOverall() {
        df.groupBy("City")
            .agg(
                mean("AverageTemperature").alias("MeanAverageTemperature"),
                min("AverageTemperature").alias("MinAverageTemperature"),
                max("AverageTemperature").alias("MaxAverageTemperature")
            )
            .sort("City")
            .write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "CityTempOverall", connectionProperties)
    }

    private fun countryTempByYear() {
        df.groupBy(
                col("Country"),
                year(col("dt")).alias("Year")
            )
            .agg(
                mean("AverageTemperature").alias("MeanAverageTemperature"),
                min("AverageTemperature").alias("MinAverageTemperature"),
                max("AverageTemperature").alias("MaxAverageTemperature")
            )
            .sort("Country", "Year")
            .write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "CountryTempByYear", connectionProperties)
    }

    private fun countryTempByCentury() {
        df.groupBy(
                col("Country"),
                floor(year(col("dt")).divide(100)).plus(1).alias("Century")
            )
            .agg(
                mean("AverageTemperature").alias("MeanAverageTemperature"),
                min("AverageTemperature").alias("MinAverageTemperature"),
                max("AverageTemperature").alias("MaxAverageTemperature")
            )
            .sort("Country", "Century")
            .write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "CountryTempByCenturies", connectionProperties)
    }

    private fun countryTempOverall() {
        df.groupBy("Country")
            .agg(
                mean("AverageTemperature").alias("MeanAverageTemperature"),
                min("AverageTemperature").alias("MinAverageTemperature"),
                max("AverageTemperature").alias("MaxAverageTemperature")
            )
            .sort("Country")
            .write()
            .mode(SaveMode.Append)
            .jdbc(jdbcUrl, "CountryTempOverall", connectionProperties)
    }

}