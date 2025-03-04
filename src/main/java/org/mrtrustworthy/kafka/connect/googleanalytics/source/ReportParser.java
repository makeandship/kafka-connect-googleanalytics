package org.mrtrustworthy.kafka.connect.googleanalytics.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.mrtrustworthy.kafka.connect.googleanalytics.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.analyticsreporting.v4.model.MetricHeaderEntry;
import com.google.api.services.analyticsreporting.v4.model.Report;
import com.google.api.services.analyticsreporting.v4.model.ReportRow;

public class ReportParser {

    private ConnectSchema valueSchema;
    private ConnectSchema keySchema;
    private int currentSchemaVersion;
    private static final Logger log = LoggerFactory.getLogger(ReportParser.class);

    ReportParser() {
        this.currentSchemaVersion = 1;
    }

    public ConnectSchema getValueSchema() {
        return valueSchema;
    }

    public ConnectSchema getKeySchema() {
        return keySchema;
    }

    /**
     * This is the only public endpoint. It's used by the SourceTask to parse a
     * report into a Struct for kafka ingestion
     *
     * @param report
     *            the Google Analytics report
     * @param topicName
     *            the topic name - this is used at schema creation to give the
     *            schema a name
     * @return All Structs that need to be sent to kafka
     */
    public Map<Struct, Struct> parseReport(Report report, String topicName) {
        this.maybeUpdateSchema(report, topicName);
        return this.createStructsOffReport(report);
    }

    /**
     * Updates self.schema if needed based on the current report
     *
     * @param report
     *            the Google Analytics report
     * @param topicName
     *            the topic name - this is used at schema creation to give the
     *            schema a name
     */
    protected void maybeUpdateSchema(Report report, String topicName) {
        ConnectSchema newValueSchema = (ConnectSchema) this.createSchemaOffReport(topicName, report);
        ConnectSchema newKeySchema = (ConnectSchema) this.createKeySchema(topicName, report);
        if (this.valueSchema == null) {
            // This is the case in the initial run - just use the newly created schema
            this.valueSchema = newValueSchema;
            this.keySchema = newKeySchema;
        } else if (newValueSchema.equals(this.valueSchema)) {
            // effectively do nothing if the schema has not changed
            log.info("Schema has not changed, continuing to use version " + this.currentSchemaVersion);
        } else {
            this.currentSchemaVersion++;
            log.info("Schema has changed, need to use new schema with version " + this.currentSchemaVersion);
            // Need to re-create schema to include the version bump
            this.valueSchema = (ConnectSchema) this.createSchemaOffReport(topicName, report);
            this.keySchema = (ConnectSchema) this.createKeySchema(topicName, report);
        }
    }

    /**
     * Will fetch all record names and values and zip them into various records
     * Assumes the correct schema has been set before calling
     *
     * @param report
     *            the Google Analytics report
     * @return all structs that need to be submitted to kafka
     */
    protected Map<Struct, Struct> createStructsOffReport(Report report) {

        assert this.valueSchema != null : "Schema must not be null!";

        Map<Struct, Struct> structs = new HashMap<>();
        List<String> recordNames = this.getRecordNamesInOrder(report);
        List<List<String>> recordValueList = this.getRecordValuesInOrder(report);

        for (List<String> recordValues : recordValueList) {
            assert recordNames.size() == recordValues.size() : "Those sizes should be the same";
            Struct value = new Struct(this.valueSchema);
            Struct key = new Struct(this.keySchema);
            for (int i = 0; i < recordNames.size(); i++) {
                String recordName = recordNames.get(i);
                this.putValueInStruct(value, recordNames.get(i), recordValues.get(i));
                if ("pagePath".equals(recordName)) {
                    String url = recordValues.get(i);
                    this.putValueInStruct(value, "urlHash", Utils.md5(url));
                    this.putValueInStruct(key, "urlHash", Utils.md5(url));
                } else if ("dateHourMinute".equals(recordName)) {
                    this.putValueInStruct(key, "dateHourMinute", recordValues.get(i));
                }
            }
            structs.put(key, value);
        }

        return structs;
    }

    /**
     * This function puts a value into the struct, parsing it to the correct value
     * underway
     *
     * @param struct
     *            the individual data set to submit to kafka
     * @param name
     *            key for the struct value, ex. "pageName"
     * @param value
     *            struct value, ex. "/home""
     */
    private void putValueInStruct(Struct struct, String name, String value) {

        String schemaName = this.valueSchema.field(name).schema().type().getName().toUpperCase();

        if (schemaName.startsWith("INT64")) {
            struct.put(name, Long.parseLong(value));
        } else if (schemaName.startsWith("INT")) {
            struct.put(name, Integer.parseInt(value));
        } else if (schemaName.startsWith("FLOAT")) {
            struct.put(name, Float.parseFloat(value));
        } else if (schemaName.startsWith("DOUBLE")) {
            struct.put(name, Double.parseDouble(value));
        } else if (schemaName.startsWith("BOOL")) {
            struct.put(name, Boolean.parseBoolean(value));
        } else if (schemaName.startsWith("STRING")) {
            struct.put(name, value);
        } else {
            log.warn("Can't find matching type for {}, putting {} into {} as string", schemaName, value, name);
            struct.put(name, value);
        }
    }

    /**
     * @param report
     *            the Google Analytics report
     * @return all record names in order, dimensions first then metrics
     */
    private List<String> getRecordNamesInOrder(Report report) {
        List<String> names = new ArrayList<>();
        names.addAll(report.getColumnHeader().getDimensions());
        names.addAll(report.getColumnHeader().getMetricHeader().getMetricHeaderEntries().stream()
                .map(MetricHeaderEntry::getName).collect(Collectors.toList()));
        return names.stream().map(this::sanitize).collect(Collectors.toList());
    }

    /**
     * @param report
     *            the Google Analytics report
     * @return all record values in order, dimensions first then metrics
     */
    private List<List<String>> getRecordValuesInOrder(Report report) {
        List<List<String>> listOfRecordValues = new ArrayList<>();
        if (report.getData().getRows() == null)
            throw new KafkaException("No data available for this timeframe");
        for (ReportRow row : report.getData().getRows()) {
            List<String> rowValues = new ArrayList<>(row.getDimensions());
            row.getMetrics().forEach(drv -> rowValues.addAll(drv.getValues()));
            listOfRecordValues.add(rowValues);
        }
        return listOfRecordValues;
    }

    /**
     * Creates a schema based on the metadata in the report object
     *
     * @param name
     *            name of the schema, typically the topic name
     * @param report
     *            the Google Analytics report
     * @return a valid schema that can encode the report
     */
    private Schema createSchemaOffReport(String name, Report report) {

        SchemaBuilder schema = SchemaBuilder.struct().name(name).version(this.currentSchemaVersion);
        report.getColumnHeader().getDimensions().forEach((s) -> schema.field(this.sanitize(s), Schema.STRING_SCHEMA));
        report.getColumnHeader().getMetricHeader().getMetricHeaderEntries()
                .forEach(mhe -> schema.field(this.sanitize(mhe.getName()), ReportParser.getSchemaOfMetric(mhe)));
        schema.field("urlHash", Schema.STRING_SCHEMA);
        return schema.build();
    }

    /**
     * Creates The key schema
     *
     */
    private Schema createKeySchema(String name, Report report) {

        SchemaBuilder schema = SchemaBuilder.struct().name(name).version(this.currentSchemaVersion);
        schema.field("dateHourMinute", Schema.STRING_SCHEMA);
        schema.field("urlHash", Schema.STRING_SCHEMA);
        return schema.build();
    }

    /**
     * @param mhe
     *            a single entry describing the metadata for a metric "column"
     * @return a primitive schema
     */
    private static Schema getSchemaOfMetric(MetricHeaderEntry mhe) {
        switch (mhe.getType().toUpperCase()) {
        case "INTEGER":
            return Schema.INT64_SCHEMA;
        case "FLOAT":
        case "DECIMAL":
            return Schema.FLOAT64_SCHEMA;
        case "BOOLEAN":
            return Schema.BOOLEAN_SCHEMA;
        default:
            log.warn("Schema for MetricHeaderEntry defaulted to String because it was " + mhe.getType());
            return Schema.STRING_SCHEMA;
        }
    }

    /**
     * Sanitize column names to confirm with AVRO standard See
     * https://avro.apache.org/docs/current/spec.html#names
     * 
     * @param s
     *            input string
     * @return avro-conforming string
     */
    private String sanitize(String s) {
        return s.replace("ga:", "").replace(".", "_");
    }
}
