package com.bosch.test

import java.io.Serializable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._



/**
  * Created by saschavetter on 06/07/16.
  */

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {
    def splitUDF: UserDefinedFunction = {
      val splitIsbnStrToList = (s:String) => {
        val reg = """^(?:ISBN: )(\d{3})-(\d{2})(\d{4})(\d{3})(?:\d{1})$""".r
        s match {
          case reg(ean, group, publisher, title) => List(s, s"ISBN-EAN: $ean", s"ISBN-GROUP: $group", s"ISBN-PUBLISHER: $publisher", s"ISBN-TITLE: $title")
          case _ => List(s)
        }

      }
      udf(splitIsbnStrToList)
    }

    df.withColumn("isbn", explode(splitUDF(col("isbn"))))
  }

}
