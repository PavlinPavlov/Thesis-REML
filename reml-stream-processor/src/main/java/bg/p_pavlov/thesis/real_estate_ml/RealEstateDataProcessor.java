package bg.p_pavlov.thesis.real_estate_ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;

public class RealEstateDataProcessor {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        Dataset<Row> baseDataFrame = new DataLoader().loadRealEstateData("local[*]", "src/main/resources/ml-data/data_to_estimate.csv");

        baseDataFrame = new ModelLoader()
                .load()
                .transform(baseDataFrame);

        baseDataFrame
                .select(col("label"), round(col("price")).as("price"))
                .show();
    }

}
