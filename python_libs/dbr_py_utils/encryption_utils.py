from pyspark.sql.functions import aes_encrypt, aes_decrypt, base64, unbase64, col, lit
from pyspark.sql import DataFrame
from typing import List

from pyspark.sql.types import StringType


class AESEncryptor:

    def __init__(self, key: str, attribute_list: List[str]):
        self.key = key
        self.attribute_list = attribute_list

    def aesEncrypt(self, df: DataFrame) -> DataFrame:
        for attribute in self.attribute_list:
            df = df.withColumn(
                attribute,
                base64(aes_encrypt(col(attribute), lit(self.key), lit("GCM"), lit("DEFAULT")))
            )
        return df

    def aesDecrypt(self, df: DataFrame) -> DataFrame:
        for attribute in self.attribute_list:
            df = df.withColumn(
                attribute,
                aes_decrypt(unbase64(col(attribute)), lit(self.key), lit("GCM"), lit("DEFAULT")).cast(StringType())
            )
        return df
