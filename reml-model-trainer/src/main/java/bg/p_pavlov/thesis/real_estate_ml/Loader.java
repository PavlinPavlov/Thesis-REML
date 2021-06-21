package bg.p_pavlov.thesis.real_estate_ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Loader {

    public Dataset<Row> loadRealEstateData(String master, String filePath) {
        SparkSession session = SparkSession.builder()
                .appName("Spark Real Estate Model Builder")
                .master(master)
                .getOrCreate();

        Dataset<Row> realEstateDF = session.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load(filePath);

        return realEstateDF
                .drop("id", "date", "waterfront", "view", "yr_renovated", "lat", "long")
                .withColumnRenamed("price", "label");
    }
}
