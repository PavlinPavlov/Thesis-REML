package bg.p_pavlov.thesis.real_estate_ml.common.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.BATHROOMS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.BEDROOMS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.CONDITION;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.DATE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.FLOORS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.GRADE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.ID;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.LATITUDE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.LONGITUDE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.PRICE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_ABOVE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_BASEMENT;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_LIVING;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_LIVING_2015;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_LOT;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_LOT_2015;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.VIEW;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.WATERFRONT;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.YEAR_BUILD;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.YEAR_RENOVATED;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.ZIPCODE;

public class SchemaHolder {

    public static StructType getSchema() {

        //@formatter:off
        return new StructType()
                .add(                     ID, DataTypes.StringType )
                .add(                   DATE, DataTypes.DateType   )
                .add(                  PRICE, DataTypes.IntegerType)
                .add(               BEDROOMS, DataTypes.IntegerType)
                .add(              BATHROOMS, DataTypes.DoubleType )
                .add(     SQUARE_FOOT_LIVING, DataTypes.IntegerType)
                .add(        SQUARE_FOOT_LOT, DataTypes.IntegerType)
                .add(                 FLOORS, DataTypes.DoubleType )
                .add(             WATERFRONT, DataTypes.IntegerType)
                .add(                   VIEW, DataTypes.IntegerType)
                .add(              CONDITION, DataTypes.IntegerType)
                .add(                  GRADE, DataTypes.IntegerType)
                .add(      SQUARE_FOOT_ABOVE, DataTypes.IntegerType)
                .add(   SQUARE_FOOT_BASEMENT, DataTypes.IntegerType)
                .add(             YEAR_BUILD, DataTypes.IntegerType)
                .add(         YEAR_RENOVATED, DataTypes.IntegerType)
                .add(                ZIPCODE, DataTypes.IntegerType)
                .add(               LATITUDE, DataTypes.DoubleType )
                .add(              LONGITUDE, DataTypes.DoubleType )
                .add(SQUARE_FOOT_LIVING_2015, DataTypes.IntegerType)
                .add(   SQUARE_FOOT_LOT_2015, DataTypes.IntegerType);
        //@formatter:on
    }
}
