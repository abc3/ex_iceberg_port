package io.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Scanner;

public class AppS3 {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            Configuration conf = new Configuration();
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            conf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"));
            conf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"));
            conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.connection.ssl.enabled", "true");

            ObjectMapper objectMapper = new ObjectMapper();

            String warehousePath = "s3a://iceberg-warehouse-bucket/warehouse-path";
            HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

            while (true) {
                System.out.println("wait for input");

                String input = scanner.nextLine();
                if (input.trim().isEmpty()) {
                    System.out.println("Empty input detected. Exiting...");
                    break;
                }

                InputRules inputRules = objectMapper.readValue(input, InputRules.class);
                System.out.println("Input Rules: " + inputRules);

                Schema icebergSchema = createSchema(inputRules.getCols());
                System.out.println("Generated Iceberg Schema: " + icebergSchema);

                TableIdentifier tableIdentifier = TableIdentifier.of(inputRules.getName()); // No namespace

                Table table;
                if (!catalog.tableExists(tableIdentifier)) {
                    table = catalog.createTable(tableIdentifier, icebergSchema);
                    System.out.println("Created table at: " + table.location());
                } else {
                    table = catalog.loadTable(tableIdentifier);
                    System.out.println("Loaded table at: " + table.location());
                }

                String dataFilePath = table.location() + "/data/iceberg_data_" + System.currentTimeMillis() + ".parquet";
                System.out.println("Writing data to Parquet file: " + dataFilePath);

                OutputFile outputFile = table.io().newOutputFile(dataFilePath);
                var appender = Parquet.write(outputFile)
                        .schema(icebergSchema)
                        .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::buildWriter)
                        .build();

                for (List<Object> row : inputRules.getRows()) {
                    Record record = GenericRecord.create(icebergSchema);
                    for (int i = 0; i < inputRules.getCols().size(); i++) {
                        record.setField(inputRules.getCols().get(i).getName(), row.get(i));
                    }
                    appender.add(record);
                }
                appender.close();
                System.out.println("Finished writing Parquet file: " + dataFilePath);

                DataFile dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                        .withPath(dataFilePath)
                        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                        .withRecordCount(inputRules.getRows().size())
                        .withFileSizeInBytes(table.io().newInputFile(dataFilePath).getLength())
                        .build();

                AppendFiles append = table.newAppend();
                append.appendFile(dataFile);
                append.commit();
                System.out.println("Data committed to table.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Schema createSchema(List<InputRules.Column> columns) {
        Types.NestedField[] icebergFields = columns.stream()
                .map(column -> {
                    String name = column.getName();
                    String type = column.getType();
                    switch (type.toLowerCase()) {
                        case "int":
                            return Types.NestedField.required(1, name, Types.IntegerType.get());
                        case "long":
                            return Types.NestedField.required(1, name, Types.LongType.get());
                        case "string":
                            return Types.NestedField.optional(2, name, Types.StringType.get());
                        default:
                            throw new IllegalArgumentException("Unsupported field type: " + type);
                    }
                })
                .toArray(Types.NestedField[]::new);

        return new Schema(icebergFields);
    }
}
