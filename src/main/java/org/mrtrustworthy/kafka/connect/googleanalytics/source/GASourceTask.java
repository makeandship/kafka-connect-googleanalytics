package org.mrtrustworthy.kafka.connect.googleanalytics.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.mrtrustworthy.kafka.connect.googleanalytics.GASourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.analyticsreporting.v4.model.DateRange;
import com.google.api.services.analyticsreporting.v4.model.Report;

public class GASourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(GASourceConnector.class);
    private static final String FETCH_END_DATE = "yesterday";
    private static final String DEFAULT_START_DATE = "1DaysAgo";
    private static final String PROCESS_FROM = "30DaysAgo";
    private static final long DAY_IN_MS = 86400000;

    private GAReportFetcher fetcher;
    private GAConnectorConfig config;
    private ReportParser reportParser;

    // https://kafka.apache.org/documentation/#connect_resuming
    private String pageToken;
    private DateRange dateRange;
    private Date lastProcessedDate;

    public void setFetcher(GAReportFetcher fetcher) {
        this.fetcher = fetcher;
    }

    public void setConfig(GAConnectorConfig config) {
        this.config = config;
    }

    public void setReportParser(ReportParser reportParser) {
        this.reportParser = reportParser;
    }

    @Override
    public void initialize(SourceTaskContext context) {
        init();
        this.context = context;
    }

    private void init() {
        DateRange range = new DateRange();
        range.setStartDate(PROCESS_FROM);
        range.setEndDate(FETCH_END_DATE);

        this.pageToken = "0";
        this.dateRange = range;
        this.lastProcessedDate = new Date();
    }

    /**
     * This should be the only place where the topic name is assembled
     * 
     * @return the topic name
     */
    private String buildTopicName() {
        return this.config.getTopicName();
    }

    @Override
    public String version() {
        return "1.0.0-rc1";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = GAConnectorConfig.fromConfigMap(props, GAConnectorConfig.ConfigType.TASK_CONFIG);
        this.fetcher = new GAReportFetcher(this.config);
        this.reportParser = new ReportParser();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final ArrayList<SourceRecord> records = new ArrayList<>();
        this.fetcher.maybeInitializeAnalyticsReporting();

        long diff = new Date().getTime() - this.lastProcessedDate.getTime();

        log.info("Last recorded offset is == " + this.pageToken);

        if (diff > DAY_IN_MS || PROCESS_FROM.equals(this.dateRange.getStartDate())) {
            this.pageToken = "0";
        } else if ("0".equals(this.pageToken)) {
            // do nothing
            log.info("sleeping, don't wake me up.");
            Thread.sleep(this.config.getPollingFrequency());
            return null;
        }

        List<Report> reports = fetchPaginatedReports();

        reports.forEach(report -> {
            List<Struct> structs = this.reportParser.parseReport(report, this.buildTopicName());

            for (Struct struct : structs) {
                records.add(this.buildSourceRecord(struct));
            }
        });

        // Thread.sleep(this.config.getPollingFrequency());

        return records;
    }

    /**
     * Retrieve paginated reports
     * 
     * @return
     * @throws IOException
     */
    private List<Report> fetchPaginatedReports() {
        List<Report> paginatedReports = new ArrayList<Report>();
        try {
            log.info("Page token is == " + this.pageToken);
            Report report = this.fetcher.getReport(dateRange, this.pageToken);
            paginatedReports.add(report);

            int total = report.getData().getRowCount();
            log.info("the total records is: " + total);

            while (report.getNextPageToken() != null) {
                int pageNumber = Integer.valueOf(report.getNextPageToken());
                this.pageToken = String.valueOf(pageNumber);

                log.info("polling from " + this.pageToken + " of " + total);
                report = this.fetcher.getReport(dateRange, this.pageToken);
                paginatedReports.add(report);
                log.info("new pageToken is: " + this.pageToken);
            }
        } catch (IOException e) {
            log.error("Got an IO exception when fetching paginated reports: " + e.getMessage());
            return paginatedReports;
        }

        log.info("set the start date to: " + DEFAULT_START_DATE);
        this.dateRange.setStartDate(DEFAULT_START_DATE);
        this.pageToken = "0";

        return paginatedReports;
    }

    public SourceRecord buildSourceRecord(Struct struct) {
        Map<String, String> sourcePartition = Collections.singletonMap("startDate", this.dateRange.getStartDate());
        Map<String, String> sourceOffset = Collections.singletonMap("pageNumber", this.pageToken);
        return new SourceRecord(sourcePartition, sourceOffset, this.buildTopicName(), this.reportParser.getSchema(),
                struct);
    }

    @Override
    public synchronized void stop() {

    }
}
