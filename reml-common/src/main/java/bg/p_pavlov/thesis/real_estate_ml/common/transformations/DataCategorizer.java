package bg.p_pavlov.thesis.real_estate_ml;

import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.CONDITION;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.GRADE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.ZIPCODE;

public class DataCategorizer {

    public Dataset<Row> encodeColumns(Dataset<Row> originalDataFrame) {

        StringIndexer gradeIndexer = getIndexerForColumn(GRADE);
        originalDataFrame = gradeIndexer
                .fit(originalDataFrame)
                .transform(originalDataFrame);

        StringIndexer conditionIndexer = getIndexerForColumn(CONDITION);
        originalDataFrame = conditionIndexer
                .fit(originalDataFrame)
                .transform(originalDataFrame);

        StringIndexer zipcodeIndexer = getIndexerForColumn(ZIPCODE);
        originalDataFrame = zipcodeIndexer
                .fit(originalDataFrame)
                .transform(originalDataFrame);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCols(new String[]{"grade_index", "condition_index", "zipcode_index"})
                .setOutputCols(new String[]{"grade_vector", "condition_vector", "zipcode_vector"});

        return encoder.fit(originalDataFrame).transform(originalDataFrame);
    }

    private static StringIndexer getIndexerForColumn(String col) {
        return new StringIndexer()
                .setInputCol(col)
                .setOutputCol(col + "_index");
    }
}
