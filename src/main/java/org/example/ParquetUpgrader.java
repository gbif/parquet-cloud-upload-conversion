package org.example;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.DelegatingWriteSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.apache.parquet.hadoop.example.GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA;

public class ParquetUpgrader extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetUpgrader.class);

  public static final class MyReadSupport extends DelegatingReadSupport<Group> {
    public MyReadSupport() {
      super(new GroupReadSupport());
    }

    @Override
    public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
      return super.init(context);
    }
  }

  public static final class MyWriteSupport extends DelegatingWriteSupport<Group> {
    public MyWriteSupport() {
      super(new GroupWriteSupport());
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return super.init(configuration);
    }
  }

  public static class ParquetTimeUpgrader extends Mapper<LongWritable, Group, LongWritable, Group> {
    enum CountersEnum {RECORDS}

    private Configuration conf;
    private BufferedReader fis;

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

    private static void cloneRecord(Group in, Group out) {
      for (int i = 0; i < in.getType().getFieldCount(); i++) {
        if (in.getFieldRepetitionCount(i) > 0) {
          Type t = in.getType().getType(i);

          if (t.isPrimitive()) {
            switch (t.asPrimitiveType().getPrimitiveTypeName()) {
              case FLOAT:
                out.add(i, in.getFloat(i, 0));
                break;
              case INT32:
                out.add(i, in.getInteger(i, 0));
                break;
              case INT64:
                out.add(i, in.getLong(i, 0));
                break;
              case INT96:
                Instant inst = toInstant(in.getInt96(i, 0));
                out.add(i, inst.toEpochMilli());
                break;
              case BINARY:
                out.add(i, in.getBinary(i, 0));
                break;
              case DOUBLE:
                out.add(i, in.getDouble(i, 0));
                break;
              case BOOLEAN:
                out.add(i, in.getBoolean(i, 0));
                break;
              case FIXED_LEN_BYTE_ARRAY:
                LOG.error("What?"); // TODO
                break;
            }
          } else {
            t.asGroupType();
            Group g = out.addGroup(i);
            cloneRecord(in.getGroup(i, 0), g);
          }
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    LOG.error("1 Arguments: {}, {}", args.length, args);
    GenericOptionsParser optionParser = new GenericOptionsParser(getConf(), args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 3)) {
      System.err.println("Usage: parquetupgrader <in> <out>");
      LOG.error("Arguments: {}, {}", args.length, args);
      System.exit(2);
    }
    String inputFile = args[1];
    String outputFile = args[2];

    getConf().set("mapreduce.map.memory.mb", "8192");

    Path parquetFilePath = null;
    // Find a file in case a directory was passed
    RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputFile), true);
    while (it.hasNext()) {
      FileStatus fs = it.next();
      if (fs.isFile()) {
        parquetFilePath = fs.getPath();
        break;
      }
    }
    if (parquetFilePath == null) {
      LOG.error("No file found for " + inputFile);
      return 1;
    }
    LOG.info("Getting schema from " + parquetFilePath);
//    ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(), parquetFilePath);
//    MessageType schema = readFooter.getFileMetaData().getSchema();
//    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/org/example/simple.avsc"));
    String schema = "message hive_schema {\n" +
      "  optional int64 gbifid;\n" +
      "  optional binary datasetkey (UTF8);\n" +
      "  optional binary occurrenceid (UTF8);\n" +
      "  optional binary kingdom (UTF8);\n" +
      "  optional binary phylum (UTF8);\n" +
      "  optional binary class (UTF8);\n" +
      "  optional binary order (UTF8);\n" +
      "  optional binary family (UTF8);\n" +
      "  optional binary genus (UTF8);\n" +
      "  optional binary species (UTF8);\n" +
      "  optional binary infraspecificepithet (UTF8);\n" +
      "  optional binary taxonrank (UTF8);\n" +
      "  optional binary scientificname (UTF8);\n" +
      "  optional binary verbatimscientificname (UTF8);\n" +
      "  optional binary verbatimscientificnameauthorship (UTF8);\n" +
      "  optional binary countrycode (UTF8);\n" +
      "  optional binary locality (UTF8);\n" +
      "  optional binary stateprovince (UTF8);\n" +
      "  optional binary occurrencestatus (UTF8);\n" +
      "  optional int32 individualcount;\n" +
      "  optional binary publishingorgkey (UTF8);\n" +
      "  optional double decimallatitude;\n" +
      "  optional double decimallongitude;\n" +
      "  optional double coordinateuncertaintyinmeters;\n" +
      "  optional double coordinateprecision;\n" +
      "  optional double elevation;\n" +
      "  optional double elevationaccuracy;\n" +
      "  optional double depth;\n" +
      "  optional double depthaccuracy;\n" +
      "  optional int96 eventdate;\n" +
      "  optional int32 day;\n" +
      "  optional int32 month;\n" +
      "  optional int32 year;\n" +
      "  optional int32 taxonkey;\n" +
      "  optional int32 specieskey;\n" +
      "  optional binary basisofrecord (UTF8);\n" +
      "  optional binary institutioncode (UTF8);\n" +
      "  optional binary collectioncode (UTF8);\n" +
      "  optional binary catalognumber (UTF8);\n" +
      "  optional binary recordnumber (UTF8);\n" +
      "  optional group identifiedby (LIST) {\n" +
      "    repeated group bag {\n" +
      "      optional binary array_element (UTF8);\n" +
      "    }\n" +
      "  }\n" +
      "  optional int96 dateidentified;\n" +
      "  optional binary license (UTF8);\n" +
      "  optional binary rightsholder (UTF8);\n" +
      "  optional group recordedby (LIST) {\n" +
      "    repeated group bag {\n" +
      "      optional binary array_element (UTF8);\n" +
      "    }\n" +
      "  }\n" +
      "  optional group typestatus (LIST) {\n" +
      "    repeated group bag {\n" +
      "      optional binary array_element (UTF8);\n" +
      "    }\n" +
      "  }\n" +
      "  optional binary establishmentmeans (UTF8);\n" +
      "  optional int96 lastinterpreted;\n" +
      "  optional group mediatype (LIST) {\n" +
      "    repeated group bag {\n" +
      "      optional binary array_element (UTF8);\n" +
      "    }\n" +
      "  }\n" +
      "  optional group issue (LIST) {\n" +
      "    repeated group bag {\n" +
      "      optional binary array_element (UTF8);\n" +
      "    }\n" +
      "  }\n" +
      "}";
    LOG.info("Schema: {}", schema);

    Pattern dates = Pattern.compile("int96 ([a-z]+);");
    schema = dates.matcher(schema).replaceAll("int64 $1 (TIMESTAMP_MILLIS);");
    LOG.info("Replacement schema: {}", schema);

    getConf().set(PARQUET_EXAMPLE_SCHEMA, schema.toString());
//    GroupWriteSupport.setSchema(schema, getConf());
//    GroupReadSupport.;

    Job job = Job.getInstance(getConf(), "parquet upgrader");
    job.setJarByClass(ParquetUpgrader.class);
    job.setMapperClass(ParquetTimeUpgrader.class);
    job.setNumReduceTasks(0);
//    job.setOutputKeyClass(LongWritable.class);
//    job.setOutputValueClass(Group.class);
    job.setInputFormatClass(ExampleInputFormat.class);
    job.setOutputFormatClass(ExampleOutputFormat.class);
    job.getConfiguration().set("mapreduce.map.memory.mb", new Integer(16*1024).toString());

//    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    ParquetInputFormat.addInputPath(job, new Path(inputFile));
    ParquetInputFormat.setReadSupportClass(job, MyReadSupport.class);

    ParquetOutputFormat.setOutputPath(job, new Path(outputFile));
    ParquetOutputFormat.setWriteSupportClass(job, MyWriteSupport.class);

    CompressionCodecName codec = CompressionCodecName.SNAPPY;
  	LOG.info("Output compression: " + codec);
    ParquetOutputFormat.setCompression(job, codec);

    job.waitForCompletion(true);

    LOG.info("Wrote to {}", outputFile);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    try {
      int res = ToolRunner.run(new Configuration(), new ParquetUpgrader(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
    }
  }

  public static final Instant REDUCED_JD = ZonedDateTime.of(1858, 11, 16, 12, 0, 0, 0, ZoneOffset.UTC).toInstant();
  public static final Instant MODIFIED_JD = ZonedDateTime.of(1858, 11, 17, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
  public static final Instant JULIAN_DATE = REDUCED_JD.minus(2400000, ChronoUnit.DAYS).minus(1, ChronoUnit.HALF_DAYS);

  public static final Instant EARLIST_SQL = ZonedDateTime.of(1, 1, 1, 0, 0, 0, 1_000_000, ZoneOffset.UTC).toInstant();
  public static final Instant LATEST_SQL = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000, ZoneOffset.UTC).toInstant();

  public static Instant toInstant(Binary binary) {
    if (binary == null) {
      return null;
    } else {
      return toInstant(NanoTime.fromBinary(Binary.fromConstantByteArray(binary.getBytes())));
    }
  }

  public static Instant toInstant(GenericData.Fixed timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return toInstant(NanoTime.fromBinary(Binary.fromConstantByteArray(timestamp.bytes())));
    }
  }

  public static Instant toInstant(NanoTime nt) {
    Instant i = JULIAN_DATE
      .plus(nt.getJulianDay(), ChronoUnit.DAYS)
      .plusNanos(nt.getTimeOfDayNanos());
    if (i.isBefore(EARLIST_SQL)) {
      return EARLIST_SQL;
    } else if (i.isAfter(LATEST_SQL)) {
      return LATEST_SQL;
    } else {
      return i;
    }
  }

}
