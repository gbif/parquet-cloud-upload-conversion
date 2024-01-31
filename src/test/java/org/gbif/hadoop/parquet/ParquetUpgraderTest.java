package org.gbif.hadoop.parquet;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITE_SUPPORT_CLASS;
import static org.apache.parquet.hadoop.example.GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA;
import static org.gbif.hadoop.parquet.ParquetCloudUploadConverter.toInstant;

public class ParquetUpgraderTest {

  final int recordLimit = Integer.MAX_VALUE;
  final String INPUT_FILE = "src/test/resources/input-sample-after-eventdate-change.parquet";
  final String OUTPUT_FILE = "target/after-eventdate-change-updated";

  @Test
  public void parquetUpgradeTest() throws Exception {
    Path inputFile = new Path(INPUT_FILE);
    Path outputFile = new Path(OUTPUT_FILE);
    if (new File(OUTPUT_FILE).exists()) {
      new File(OUTPUT_FILE).delete();
    }

    Configuration conf = new Configuration();

    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, inputFile);
    MessageType schema = readFooter.getFileMetaData().getSchema();
    //System.out.println("Schema: "+schema);
    //System.out.println("Columns "+ schema.getColumns());

    // Replace int96 eventdate → int64 eventdate (TIMESTAMP_MILLIS) for all int96 types.
    String newSchema = ParquetCloudUploadConverter.updateSchema(schema);
    conf.set(PARQUET_EXAMPLE_SCHEMA, newSchema);

    // Destination file
    conf.set(WRITE_SUPPORT_CLASS, ParquetUpgrader.MyWriteSupport.class.getName());
    ParquetOutputFormat<Group> writeSupport = new ParquetOutputFormat<>();
    RecordWriter<Void, Group> recordWriter = writeSupport.getRecordWriter(conf, outputFile, CompressionCodecName.SNAPPY);

    try (ParquetFileReader reader = ParquetFileReader.open(conf, inputFile)) {
      PageReadStore pages;
      while (null != (pages = reader.readNextRowGroup())) {
        final long rows = pages.getRowCount();
        System.out.println("Number of rows: " + rows);

        final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

        final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

        int record;
        for (record = 0; record < rows && record < recordLimit; record++) {
          if (recordLimit < Integer.MAX_VALUE) {
            System.out.println("Reading record " + record);
          } else if (record % 50000 == 0) {
            System.out.println("Reading record " + record);
          }
          final Group in = (Group) recordReader.read();
          printGroup(in);
          if (recordLimit < Integer.MAX_VALUE) System.err.println("-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-");
          final Group out = new SimpleGroup(schema);
          ParquetCloudUploadConverter.cloneRecord(in, out);
          printGroup(out);
          recordWriter.write(null, out);
        }
        System.out.println("Wrote " + record + " records");
      }

    } finally {
      recordWriter.close(null);
    }
  }

  private void printGroup(Group in) {
    //System.out.println("Fields " + in.getType().getFields().size());

    for (int field = 0; field < in.getType().getFieldCount(); field++) {
      Type t = in.getType().getType(field);

      if (t.isPrimitive()) {
        try {
          for (int rep = 0; rep < in.getFieldRepetitionCount(field); rep++) {
            switch (t.asPrimitiveType().getPrimitiveTypeName()) {
              case FLOAT:
                printIt(t, field, in.getFloat(field, rep));
                break;
              case INT32:
                printIt(t, field, in.getInteger(field, rep));
                break;
              case INT64:
                printIt(t, field, in.getLong(field, rep));
                break;
              case INT96:
                //printIt(t, field, in.getInt96(field, rep));
                Instant inst = toInstant(in.getInt96(field, rep));
                printIt(t, field, inst.toEpochMilli());
                break;
              case BINARY:
                if (in.getBinary(field, rep) != null) {
                  if ("eventdate".equals(t.getName())) {
                    Binary date = in.getBinary(field, rep);
                    if (date != null && date.length() > 0) {
                      String stringDate = date.toStringUsingUTF8();
                      try {
                        IsoDateInterval interval = IsoDateInterval.fromString(stringDate);
                        LocalDateTime earliestLDT = TemporalAccessorUtils.toEarliestLocalDateTime(interval.getFrom(), true);
                        printIt(t, field, earliestLDT.toInstant(ZoneOffset.UTC).toEpochMilli());
                      } catch (ParseException e) {
                        throw new RuntimeException("Problem understanding an eventdate "+stringDate, e);
                      }
                    }
                  } else {
                    printIt(t, field, in.getBinary(field, rep).toStringUsingUTF8());
                  }
                }
                break;
              case DOUBLE:
                printIt(t, field, in.getDouble(field, rep));
                break;
              case BOOLEAN:
                printIt(t, field, in.getBoolean(field, rep));
                break;
              case FIXED_LEN_BYTE_ARRAY:
                throw new RuntimeException("Unimplemented FIXED_LEN_BYTE_ARRAY");
            }
          }
        } catch (RuntimeException e) {
          if (recordLimit < Integer.MAX_VALUE) System.out.println(t + " " + field + ": NULL");
        }
      } else {
        try {
          for (int x = 0; x < in.getFieldRepetitionCount(field); x++) {
            printItGroup(t, field, in.getGroup(field, x));
          }
        } catch (RuntimeException e) {
          if (recordLimit < Integer.MAX_VALUE) System.out.println(t + " " + field + ": NULL");
        }
      }
    }
  }

  int indent = 0;

  private void printIt(Type t, int i, Object v) {
    if (recordLimit < Integer.MAX_VALUE) System.out.println(Strings.repeat("  ", indent) + t + " " + i + ": " + v.toString());
  }

  private void printItGroup(Type t, int i, Group v) {
    GroupType gt = t.asGroupType();
    if (recordLimit < Integer.MAX_VALUE) System.out.println(Strings.repeat("  ", indent) + gt.toString().replaceAll("\n *", " ") + " " + i + ": ");
    indent++;
    printGroup(v);
    indent--;
  }
}
