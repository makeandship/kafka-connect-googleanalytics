package org.mrtrustworthy.kafka.connect.googleanalytics.source;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import com.google.api.services.analyticsreporting.v4.AnalyticsReporting;
import com.google.api.services.analyticsreporting.v4.model.DateRange;
import com.google.api.services.analyticsreporting.v4.model.Report;

class GAReportFetcherTest {

    GAConnectorConfig getSampleConfig() {
        Properties prop = new Properties();
        ClassLoader loader = GAConnectorConfig.class.getClassLoader();
        InputStream stream = loader.getResourceAsStream("test-conf.properties");
        try {
            prop.load(stream);
        } catch (IOException e) {
            assertTrue(false, "This should not throw - is the file there?");
        }
        Map<String, String> map = prop.entrySet().stream()
                .collect(Collectors.toMap(es -> es.getKey().toString(), es -> es.getValue().toString()));

        GAConnectorConfig conf = GAConnectorConfig.fromConfigMap(map, GAConnectorConfig.ConfigType.TASK_CONFIG);
        return conf;
    }

    @Test
    void testAnalyticsReportingInitialization() throws IOException, GeneralSecurityException {
        GAConnectorConfig conf = getSampleConfig();
        GAReportFetcher gafetcher = new GAReportFetcher(conf);
        AnalyticsReporting rep = gafetcher.getAnalyticsService();
        assertNotNull(rep);
    }

    @Test
    void testStructAssembly() {
        try {
            GAConnectorConfig conf = getSampleConfig();
            GAReportFetcher gafetcher = new GAReportFetcher(conf);
            GASourceTask task = new GASourceTask();
            ReportParser repParser = new ReportParser();
            task.setConfig(conf);
            task.setFetcher(gafetcher);
            task.setReportParser(repParser);

            gafetcher.maybeInitializeAnalyticsReporting();
            DateRange dateRange = new DateRange();
            dateRange.setStartDate("2DaysAgo");
            dateRange.setEndDate("yesterday");
            Report report = gafetcher.getReport(dateRange, "0");
            assertNotNull(report);
            System.out.println("Report: " + report);

            repParser.maybeUpdateSchema(report, conf.getTopicName());
            System.out.println("Schema: " + Objects.toString(repParser.getSchema()));

            List<Struct> structs = repParser.createStructsOffReport(report);
            System.out.println("Structs: " + Objects.toString(structs));

        } catch (IOException e) {
            assertTrue(false);
        }
    }
}
