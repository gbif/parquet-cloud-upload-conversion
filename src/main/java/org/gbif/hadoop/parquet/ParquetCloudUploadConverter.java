package org.gbif.hadoop.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.common.parsers.date.TemporalAccessorUtils;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;

/**
 * Converts a SIMPLE_PARQUET GBIF download into the format accepted by Google BigQuery and others.
 *
 * 1. Convert timestamps from an int96 to an int64
 * 2. Convert bags into arrays
 * 3. Convert the event date (interval string) into an int64 of the earliest date
 */
public class ParquetCloudUploadConverter extends Mapper<LongWritable, Group, LongWritable, Group> {
  enum CountersEnum {RECORDS}

  private Configuration conf;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
  }

  @Override
  public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
    Group outRecord = new SimpleGroup(value.getType());
    cloneRecord(value, outRecord);

    Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.RECORDS.toString());
    counter.increment(1);
    context.write(key, outRecord);
  }

  protected static String updateSchema(MessageType schema) {
    String stringSchema = schema.toString();

    // Replace int96 timestamp → int64 timestamp (TIMESTAMP_MILLIS) for all int96 types.
    Pattern dates = Pattern.compile("int96 ([a-z]+);");
    stringSchema = dates.matcher(stringSchema).replaceAll("int64 $1 (TIMESTAMP_MILLIS);");

    // Replace bags with arrays. I'm not sure why we do this; I don't see any reference to it in my email.
    stringSchema = stringSchema.replaceAll("group bag", "group array");

    // Replace string eventdate with int64 timestamp.
    stringSchema = stringSchema.replaceAll("binary eventdate .UTF8.;", "int64 eventdate (TIMESTAMP_MILLIS);");
    //System.out.println("New schema: "+stringSchema);

    return stringSchema;
  }

  /**
   * Duplicate a Parquet record, including complex types. Convert fields as required.
   */
  protected static void cloneRecord(Group in, Group out) {
    for (int field = 0; field < in.getType().getFieldCount(); field++) {
      Type t = in.getType().getType(field);
      for (int rep = 0; rep < in.getFieldRepetitionCount(field); rep++) {
        if (t.isPrimitive()) {
          switch (t.asPrimitiveType().getPrimitiveTypeName()) {
            case FLOAT:
              out.add(field, in.getFloat(field, rep));
              break;
            case INT32:
              out.add(field, in.getInteger(field, rep));
              break;
            case INT64:
              out.add(field, in.getLong(field, rep));
              break;
            case INT96:
              Instant inst = toInstant(in.getInt96(field, rep));
              out.add(field, inst.toEpochMilli());
              break;
            case BINARY:
              if ("eventdate".equals(t.getName())) {
                Binary date = in.getBinary(field, rep);
                if (date != null && date.length() > 0) {
                  String stringDate = date.toStringUsingUTF8();
                  try {
                    IsoDateInterval interval = IsoDateInterval.fromString(stringDate);
                    LocalDateTime earliestLDT = TemporalAccessorUtils.toEarliestLocalDateTime(interval.getFrom(), true);
                    out.add(field, earliestLDT.toInstant(ZoneOffset.UTC).toEpochMilli());
                  } catch (ParseException e) {
                    throw new RuntimeException("Problem understanding an eventdate "+stringDate, e);
                  }
                }
              } else {
                out.add(field, in.getBinary(field, rep));
              }
              break;
            case DOUBLE:
              out.add(field, in.getDouble(field, rep));
              break;
            case BOOLEAN:
              out.add(field, in.getBoolean(field, rep));
              break;
            case FIXED_LEN_BYTE_ARRAY:
              throw new RuntimeException("Unimplemented FIXED_LEN_BYTE_ARRAY");
          }
        } else {
          Group g = out.addGroup(field);
          cloneRecord(in.getGroup(field, rep), g);
        }
      }
    }
  }

  public static final Instant REDUCED_JD = ZonedDateTime.of(1858, 11, 16, 12, 0, 0, 0, ZoneOffset.UTC).toInstant();
  public static final Instant JULIAN_DATE = REDUCED_JD.minus(2400000, ChronoUnit.DAYS).minus(1, ChronoUnit.HALF_DAYS);

  public static final Instant EARLIEST_SQL = ZonedDateTime.of(1, 1, 1, 0, 0, 0, 1_000_000, ZoneOffset.UTC).toInstant();
  public static final Instant LATEST_SQL = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000, ZoneOffset.UTC).toInstant();

  public static Instant toInstant(Binary binary) {
    if (binary == null) {
      return null;
    } else {
      return toInstant(NanoTime.fromBinary(Binary.fromConstantByteArray(binary.getBytes())));
    }
  }

  public static Instant toInstant(NanoTime nt) {
    Instant i = JULIAN_DATE
      .plus(nt.getJulianDay(), ChronoUnit.DAYS)
      .plusNanos(nt.getTimeOfDayNanos());
    if (i.isBefore(EARLIEST_SQL)) {
      return EARLIEST_SQL;
    } else if (i.isAfter(LATEST_SQL)) {
      return LATEST_SQL;
    } else {
      return i;
    }
  }
}
