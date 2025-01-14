package io.experiment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class App {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "file:///");

            ObjectMapper objectMapper = new ObjectMapper();

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

                String warehouseDir = "./iceberg_warehouse";
                String tablePath = warehouseDir + "/" + inputRules.getName();

                File warehouseDirFile = new File(warehouseDir);
                if (!warehouseDirFile.exists() && !warehouseDirFile.mkdirs()) {
                    throw new RuntimeException("Failed to create warehouse directory: " + warehouseDir);
                }

                HadoopTables tables = new HadoopTables(conf);
                Table table;
                if (!new File(tablePath).exists()) {
                    table = tables.create(icebergSchema, tablePath);
                    System.out.println("Created table at: " + table.location());
                } else {
                    table = tables.load(tablePath);
                    System.out.println("Loaded table at: " + table.location());
                }

                String dataFilePath = tablePath + "/data/iceberg_data_" + System.currentTimeMillis() + ".parquet";
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

                File parquetFile = new File(dataFilePath);
                DataFile dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                        .withPath(parquetFile.getAbsolutePath())
                        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                        .withRecordCount(inputRules.getRows().size())
                        .withFileSizeInBytes(parquetFile.length())
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
