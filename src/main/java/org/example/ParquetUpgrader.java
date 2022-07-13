package org.example;

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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
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
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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

    /**
     * Duplicate a Parquet record, including (some?) complex types.
     */
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
                throw new RuntimeException("Unimplemented FIXED_LEN_BYTE_ARRAY");
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
    LOG.info("----------------------------------------------------------------------------------");
    LOG.info("Starting Parquet schema update with arguments {}", args);
    LOG.info("----------------------------------------------------------------------------------");

    GenericOptionsParser optionParser = new GenericOptionsParser(getConf(), args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 3)) {
      System.err.println("Usage: parquetupgrader <in> <out>");
      System.exit(2);
    }
    Path inputFile = new Path(args[1]);
    Path outputFile = new Path(args[2]);

    // Read schema from one of the source files
    Path parquetFilePath = null;
    RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(inputFile, true);
    while (it.hasNext()) {
      FileStatus fs = it.next();
      if (fs.isFile()) {
        parquetFilePath = fs.getPath();
        break;
      }
    }
    if (parquetFilePath == null) {
      LOG.error("No file found for {}", inputFile);
      return 1;
    }

    LOG.info("Getting schema from " + parquetFilePath);
    ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(), parquetFilePath);
    String schema = readFooter.getFileMetaData().getSchema().toString();
    LOG.info("Read schema: {}", schema);

    // Replace int96 eventdate → int64 eventdate (TIMESTAMP_MILLIS) for all int96 types.
    Pattern dates = Pattern.compile("int96 ([a-z]+);");
    schema = dates.matcher(schema).replaceAll("int64 $1 (TIMESTAMP_MILLIS);");
    schema = schema.replaceAll("group bag", "group array");
    LOG.info("Replacement schema: {}", schema);
    getConf().set(PARQUET_EXAMPLE_SCHEMA, schema);

    Job job = Job.getInstance(getConf(), "Parquet upgrade of "+outputFile.getName());
    job.setJarByClass(ParquetUpgrader.class);
    job.setMapperClass(ParquetTimeUpgrader.class);
    job.setNumReduceTasks(0);
    //job.setOutputKeyClass(LongWritable.class);
    //job.setOutputValueClass(Group.class);
    job.setInputFormatClass(ExampleInputFormat.class);
    job.setOutputFormatClass(ExampleOutputFormat.class);
    // Increase memory as the monthly downloads are pretty large.
    job.getConfiguration().set("mapreduce.map.memory.mb", Integer.toString(16 * 1024));

    ParquetInputFormat.addInputPath(job, inputFile);
    ParquetInputFormat.setReadSupportClass(job, MyReadSupport.class);

    ParquetOutputFormat.setOutputPath(job, outputFile);
    ParquetOutputFormat.setWriteSupportClass(job, MyWriteSupport.class);

    CompressionCodecName codec = CompressionCodecName.SNAPPY;
  	LOG.info("Output compression: " + codec);
    ParquetOutputFormat.setCompression(job, codec);

    job.waitForCompletion(true);

    // Tidy up output files
    LOG.info("Tidying up resulting files");
    it = FileSystem.get(getConf()).listFiles(outputFile, true);
    while (it.hasNext()) {
      FileStatus fs = it.next();
      if (fs.isFile()) {
        Path from = fs.getPath();
        if (from.getName().endsWith("parquet")) {
          Path to = Path.mergePaths(from.getParent(), new Path("/" + from.getName().replace("part-m-", "0").replace(".snappy.parquet", "")));
          FileSystem.get(getConf()).rename(from, to);
          LOG.info("{} renamed to {}", from, to);
        } else {
          FileSystem.get(getConf()).delete(from, false);
          LOG.info("{} deleted", from);
        }
      }
    }

    LOG.info("Wrote to {}", outputFile);

    return 0;
  }

  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new Configuration(), new ParquetUpgrader(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
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
