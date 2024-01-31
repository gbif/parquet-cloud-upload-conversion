# Parquet Cloud Upload Conversion

This tool converts files from a SIMPLE_PARQUET GBIF download into the form uploaded to Google, Amazon and Microsoft's public data.

We should coordinate changes to the output schema with the three providers.

The conversion from INT96 to INT64 timestamps is due to our processing cluster using an older version of Parquet.

See https://www.gbif.org/occurrence-snapshots
