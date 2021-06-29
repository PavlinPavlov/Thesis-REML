package bg.p_pavlov.thesis.real_estate_ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.DATE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.LATITUDE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.LONGITUDE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.PRICE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.VIEW;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.WATERFRONT;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.YEAR_RENOVATED;

public class DataLoader {

    public Dataset<Row> loadRealEstateData(String master, String filePath) {
        SparkSession session = SparkSession.builder()
                .appName("Spark Real Estate Price Predictor")
                .master(master)
                .getOrCreate();

        Dataset<Row> realEstateDF = session.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load(filePath);

        return realEstateDF
                .drop(
                        DATE,
                        WATERFRONT,
                        VIEW,
                        YEAR_RENOVATED,
                        LATITUDE,
                        LONGITUDE
                )
                .withColumnRenamed(PRICE, "label");
    }
}
