package com.atlas.db

import java.sql.Timestamp

import org.joda.time.DateTime
import org.squeryl.PrimitiveTypeMode
import org.squeryl.dsl._

object AtlasPrimitiveTypeMode extends PrimitiveTypeMode {

  // optionally define custom types :
  implicit val jodaTimeTEF = new NonPrimitiveJdbcMapper[Timestamp, DateTime, TTimestamp](timestampTEF, this) {
    /**
      * Here we implement functions fo convert to and from the native JDBC type
      */
    def convertFromJdbc(t: Timestamp) = new DateTime(t)

    def convertToJdbc(t: DateTime) = new Timestamp(t.getMillis())
  }
  /**
    * We define this one here to allow working with Option of our new type, this allso
    * allows the 'nvl' function to work
    */
  implicit val optionJodaTimeTEF =
    new TypedExpressionFactory[Option[DateTime], TOptionTimestamp]
      with DeOptionizer[Timestamp, DateTime, TTimestamp, Option[DateTime], TOptionTimestamp] {
      val deOptionizer = jodaTimeTEF
    }

  /**
    * the following are necessary for the AST lifting
    */
  implicit def jodaTimeToTE(s: DateTime) = jodaTimeTEF.create(s)

  implicit def optionJodaTimeToTE(s: Option[DateTime]) = optionJodaTimeTEF.create(s)

}
