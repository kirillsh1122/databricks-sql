package com.databricksaux;

import com.databricksaux.services.AESEncryptor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.scala.dbutils.DBUtils;

public class BronzeLoader {

    public static void main(String... args) throws Exception {

        DBUtils dbutils = DBUtils.getDBUtils(new DatabricksConfig());

        // Get storage account name and SPN data
        String storageAccountName = dbutils.secrets().get("key-vault", "storage-acc-name");
        String clientId = dbutils.secrets().get("key-vault", "client-id");
        String clientSecret = dbutils.secrets().get("key-vault", "client-secret");
        String tenantId = dbutils.secrets().get("key-vault", "tenant-id");
        String AESKey = dbutils.secrets().get("key-vault", "aes-encryption-key");


        // Build source data location paths
        String expediaSourcePath ="abfss://data@"+storageAccountName+".dfs.core.windows.net/expedia";
        String hotelWeatherSourcePath ="abfss://data@"+storageAccountName+".dfs.core.windows.net/hotel-weather";

        //Target delta tables
        String expediaBronzeTable = "bronze.expedia_raw";
        String hotelWeatherBronzeTable = "bronze.hotel_weather_raw";

        // Build Spark Session
        SparkSession spark = SparkSession.builder()
                .config("fs.azure.account.auth.type."+storageAccountName+".dfs.core.windows.net", "OAuth")
                .config("fs.azure.account.oauth.provider.type."+storageAccountName+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                .config("fs.azure.account.oauth2.client.id."+storageAccountName+".dfs.core.windows.net", clientId)
                .config("fs.azure.account.oauth2.client.secret."+storageAccountName+".dfs.core.windows.net", clientSecret)
                .config("fs.azure.account.oauth2.client.endpoint."+storageAccountName+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantId+"/oauth2/token")
                .getOrCreate();

        // Load source data into DataFrames
        Dataset<Row> expediaDF = spark.read().format("avro").load(expediaSourcePath);
        Dataset<Row> hotelWeatherDF = spark.read().format("parquet").load(hotelWeatherSourcePath);

        // Encrypt address with AES
        AESEncryptor encryptor = AESEncryptor.builder().key(AESKey).build();
        encryptor.setColumnList(new String[]{"name", "address"});
        hotelWeatherDF = hotelWeatherDF.transform(encryptor::aesEncrypt);

        // Persist raw encrypted data in bronze tables
        expediaDF.write().mode("overwrite").format("delta").saveAsTable(expediaBronzeTable);
        hotelWeatherDF.write().mode("overwrite").format("delta").partitionBy("year", "month", "day").saveAsTable(hotelWeatherBronzeTable);
    }
}
