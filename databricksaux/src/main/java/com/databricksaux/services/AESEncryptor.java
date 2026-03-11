package com.databricksaux.services;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

@Builder
@AllArgsConstructor
public class AESEncryptor {

    @Setter
    @Builder.Default
    private String[] columnList = new String[]{};
    private final String key;

    public Dataset<Row> aesEncrypt(Dataset<Row> inputDf) {
        for (String attribute: columnList) {
            inputDf = inputDf.withColumn(
                    attribute,
                    base64(aes_encrypt(col(attribute), lit(key), lit("GCM"), lit("DEFAULT")))
            );
        }
        return inputDf;
    }

    public Dataset<Row> aesDecrypt(Dataset<Row> inputDf) {
        for (String attribute: columnList) {
            inputDf = inputDf.withColumn(
                    attribute,
                    aes_decrypt(unbase64(col(attribute)), lit(key), lit("GCM"), lit("DEFAULT")).cast(DataTypes.StringType)
            );
        }
        return inputDf;
    }
}
