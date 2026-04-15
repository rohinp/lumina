package lumina.plan

enum DataType:
  case Int32,
    Int64,
    Float64,
    BooleanType,
    StringType,
    /** Calendar date (year, month, day).  Stored as `java.time.LocalDate`. */
    DateType,
    /** Date and time without timezone.  Stored as `java.time.LocalDateTime`. */
    TimestampType,
    Unknown

final case class Column(name: String, dataType: DataType, nullable: Boolean = true)

final case class Schema(columns: Vector[Column]):
  lazy val columnNames: Vector[String] = columns.map(_.name)
