package bg.p_pavlov.thesis.real_estate_ml;

import bg.p_pavlov.thesis.real_estate_ml.common.schema.SchemaHolder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.DATE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.LATITUDE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.LONGITUDE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.PRICE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.VIEW;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.WATERFRONT;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.YEAR_RENOVATED;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class DataFrameLoader {

    public Dataset<Row> loadRealEstateData() {
        SparkSession session = SparkSession.builder()
                .appName("Spark Real Estate Processing App")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> kafkaDataFrame = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "real-estate-entries")
                .load();

        kafkaDataFrame = kafkaDataFrame
                .select(
                        from_json(
                                col("value").cast(DataTypes.StringType),
                                SchemaHolder.getSchema()
                        ).as("tmp_real_estate_data")
                )
                .select("tmp_real_estate_data.*");

        return kafkaDataFrame
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
