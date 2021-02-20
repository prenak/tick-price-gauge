package com.idx.tick.service;

import com.idx.tick.exception.TickOlderThanAllowedDurationException;
import com.idx.tick.model.Stat;
import com.idx.tick.model.Tick;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;


@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {PriceAggregationService.class})
@TestPropertySource("classpath:application-test.properties")
public class PriceAggregationServiceTests {

    @Autowired
    private PriceAggregationService priceAggregationService;


    @Before
    public void clearData(){
        priceAggregationService.getTicks().clear();
    }


    @Test
    public void test_ProcessTick_ForIllegalArguments(){
        Throwable thrown = catchThrowable(() -> {
            priceAggregationService.processTick(null);
        });
        assertThat(thrown)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Tick passed is null");
    }

    @Test
    public void test_ProcessTick_ForTicksOlderThanAllowedDuration(){
        Throwable thrown = catchThrowable(() -> {
            priceAggregationService.processTick(new Tick("ABC", 120.0, System.currentTimeMillis()-1001));
        });
        assertThat(thrown)
                .isInstanceOf(TickOlderThanAllowedDurationException.class)
                .hasMessageContaining("Tick(instrument=ABC, price=120.0, timestamp=")
                .hasMessageContaining(") is older than allowed duration of 1000 milliseconds");
    }


    @Test
    public void test_PriceAggregation_WhenTicksAreAddedContinuously() throws IOException {
        File file = ResourceUtils.getFile("classpath:ticks.csv");

        // We use apache.commons-csv to parse the CSV easily
        List<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new FileReader(file))
                .getRecords();

        records.forEach(r -> {
            try {
                priceAggregationService.processTick(new Tick(r.get(0), Double.valueOf(r.get(1)), System.currentTimeMillis()));
            } catch (TickOlderThanAllowedDurationException e) {
                e.printStackTrace();
            }
        });

        Optional<Stat> optionalOverallStat = priceAggregationService.getOverallStat();
        assertThat(optionalOverallStat.isPresent()).isTrue();
        Stat overallStat = optionalOverallStat.get();
        assertThat(overallStat.getAvg()).isEqualTo(Double.valueOf(281.6666666666667));
        assertThat(overallStat.getMax()).isEqualTo(Double.valueOf(415.0));
        assertThat(overallStat.getMin()).isEqualTo(Double.valueOf(118.0));
        assertThat(overallStat.getCount()).isEqualTo(Long.valueOf(30));

        Optional<Stat> optionalIbmStat = priceAggregationService.getStatForInstrument("IBM");
        assertThat(optionalIbmStat.isPresent()).isTrue();
        Stat ibmStat = optionalIbmStat.get();
        assertThat(ibmStat.getAvg()).isEqualTo(Double.valueOf(120.63636363636364));
        assertThat(ibmStat.getMax()).isEqualTo(Double.valueOf(123.0));
        assertThat(ibmStat.getMin()).isEqualTo(Double.valueOf(118.0));
        assertThat(ibmStat.getCount()).isEqualTo(Long.valueOf(11));

        Optional<Stat> optionalRocStat = priceAggregationService.getStatForInstrument("ROC");
        assertThat(optionalRocStat.isPresent()).isTrue();
        Stat rocStat = optionalRocStat.get();
        assertThat(rocStat.getAvg()).isEqualTo(Double.valueOf(340.0));
        assertThat(rocStat.getMax()).isEqualTo(Double.valueOf(380.0));
        assertThat(rocStat.getMin()).isEqualTo(Double.valueOf(300.0));
        assertThat(rocStat.getCount()).isEqualTo(Long.valueOf(9));

        Optional<Stat> optionalAbcStat = priceAggregationService.getStatForInstrument("ABC");
        assertThat(optionalAbcStat.isPresent()).isTrue();
        Stat abcStat = optionalAbcStat.get();
        assertThat(abcStat.getAvg()).isEqualTo(Double.valueOf(406.3));
        assertThat(abcStat.getMax()).isEqualTo(Double.valueOf(415.0));
        assertThat(abcStat.getMin()).isEqualTo(Double.valueOf(400.0));
        assertThat(abcStat.getCount()).isEqualTo(Long.valueOf(10));
    }


    @Test
    public void test_PriceAggregation_WhenTicksAreAddedConcurrently() throws IOException {
        File file = ResourceUtils.getFile("classpath:ticks.csv");

        // We use apache.commons-csv to parse the CSV easily
        List<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new FileReader(file))
                .getRecords();

        records.forEach(r -> {
            new Thread(() -> {
                try {
                    priceAggregationService.processTick(new Tick(r.get(0), Double.valueOf(r.get(1)), System.currentTimeMillis()));
                } catch (TickOlderThanAllowedDurationException e) {
                    e.printStackTrace();
                }
            }).start();
        });
        // Let the threads complete execution
        sleepFor(100);

        Optional<Stat> optionalOverallStat = priceAggregationService.getOverallStat();
        assertThat(optionalOverallStat.isPresent()).isTrue();
        Stat overallStat = optionalOverallStat.get();
        assertThat(overallStat.getAvg()).isEqualTo(Double.valueOf(281.6666666666667));
        assertThat(overallStat.getMax()).isEqualTo(Double.valueOf(415.0));
        assertThat(overallStat.getMin()).isEqualTo(Double.valueOf(118.0));
        assertThat(overallStat.getCount()).isEqualTo(Long.valueOf(30));

        Optional<Stat> optionalIbmStat = priceAggregationService.getStatForInstrument("IBM");
        assertThat(optionalIbmStat.isPresent()).isTrue();
        Stat ibmStat = optionalIbmStat.get();
        assertThat(ibmStat.getAvg()).isEqualTo(Double.valueOf(120.63636363636364));
        assertThat(ibmStat.getMax()).isEqualTo(Double.valueOf(123.0));
        assertThat(ibmStat.getMin()).isEqualTo(Double.valueOf(118.0));
        assertThat(ibmStat.getCount()).isEqualTo(Long.valueOf(11));

        Optional<Stat> optionalRocStat = priceAggregationService.getStatForInstrument("ROC");
        assertThat(optionalRocStat.isPresent()).isTrue();
        Stat rocStat = optionalRocStat.get();
        assertThat(rocStat.getAvg()).isEqualTo(Double.valueOf(340.0));
        assertThat(rocStat.getMax()).isEqualTo(Double.valueOf(380.0));
        assertThat(rocStat.getMin()).isEqualTo(Double.valueOf(300.0));
        assertThat(rocStat.getCount()).isEqualTo(Long.valueOf(9));

        Optional<Stat> optionalAbcStat = priceAggregationService.getStatForInstrument("ABC");
        assertThat(optionalAbcStat.isPresent()).isTrue();
        Stat abcStat = optionalAbcStat.get();
        assertThat(abcStat.getAvg()).isEqualTo(Double.valueOf(406.3));
        assertThat(abcStat.getMax()).isEqualTo(Double.valueOf(415.0));
        assertThat(abcStat.getMin()).isEqualTo(Double.valueOf(400.0));
        assertThat(abcStat.getCount()).isEqualTo(Long.valueOf(10));
    }

    @Test
    public void test_PriceAggregation_WhenTicksAreAddedConcurrently_InIntervalsOf1000Ms() throws IOException {
        File file = ResourceUtils.getFile("classpath:ticks.csv");

        // We use apache.commons-csv to parse the CSV easily
        List<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new FileReader(file))
                .getRecords();

        int i;
        for (i=0; i<10; i++){
            int n = i;
            new Thread(() -> {
                try {
                    priceAggregationService.processTick(new Tick(records.get(n).get(0), Double.valueOf(records.get(n).get(1)), System.currentTimeMillis()));
                } catch (TickOlderThanAllowedDurationException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        // Let the threads complete execution
        sleepFor(100);

        Optional<Stat> optionalOverallStat = priceAggregationService.getOverallStat();
        assertThat(optionalOverallStat.isPresent()).isTrue();
        Stat overallStat = optionalOverallStat.get();
        assertThat(overallStat.getAvg()).isEqualTo(Double.valueOf(263.1));
        assertThat(overallStat.getMax()).isEqualTo(Double.valueOf(410.0));
        assertThat(overallStat.getMin()).isEqualTo(Double.valueOf(120.0));
        assertThat(overallStat.getCount()).isEqualTo(Long.valueOf(10));

        Optional<Stat> optionalIbmStat = priceAggregationService.getStatForInstrument("IBM");
        assertThat(optionalIbmStat.isPresent()).isTrue();
        Stat ibmStat = optionalIbmStat.get();
        assertThat(ibmStat.getAvg()).isEqualTo(Double.valueOf(121.5));
        assertThat(ibmStat.getMax()).isEqualTo(Double.valueOf(123.0));
        assertThat(ibmStat.getMin()).isEqualTo(Double.valueOf(120.0));
        assertThat(ibmStat.getCount()).isEqualTo(Long.valueOf(4));

        Optional<Stat> optionalRocStat = priceAggregationService.getStatForInstrument("ROC");
        assertThat(optionalRocStat.isPresent()).isTrue();
        Stat rocStat = optionalRocStat.get();
        assertThat(rocStat.getAvg()).isEqualTo(Double.valueOf(310.0));
        assertThat(rocStat.getMax()).isEqualTo(Double.valueOf(320.0));
        assertThat(rocStat.getMin()).isEqualTo(Double.valueOf(300.0));
        assertThat(rocStat.getCount()).isEqualTo(Long.valueOf(3));

        Optional<Stat> optionalAbcStat = priceAggregationService.getStatForInstrument("ABC");
        assertThat(optionalAbcStat.isPresent()).isTrue();
        Stat abcStat = optionalAbcStat.get();
        assertThat(abcStat.getAvg()).isEqualTo(Double.valueOf(405.0));
        assertThat(abcStat.getMax()).isEqualTo(Double.valueOf(410.0));
        assertThat(abcStat.getMin()).isEqualTo(Double.valueOf(400.0));
        assertThat(abcStat.getCount()).isEqualTo(Long.valueOf(3));

        sleepFor(1000);

        for (i=10; i<25; i++){
            int n = i;
            new Thread(() -> {
                try {
                    priceAggregationService.processTick(new Tick(records.get(n).get(0), Double.valueOf(records.get(n).get(1)), System.currentTimeMillis()));
                } catch (TickOlderThanAllowedDurationException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        // Let the threads complete execution
        sleepFor(100);

        optionalOverallStat = priceAggregationService.getOverallStat();
        assertThat(optionalOverallStat.isPresent()).isTrue();
        overallStat = optionalOverallStat.get();
        assertThat(overallStat.getAvg()).isEqualTo(Double.valueOf(276.26666666666665));
        assertThat(overallStat.getMax()).isEqualTo(Double.valueOf(415.0));
        assertThat(overallStat.getMin()).isEqualTo(Double.valueOf(118.0));
        assertThat(overallStat.getCount()).isEqualTo(Long.valueOf(15));

        optionalIbmStat = priceAggregationService.getStatForInstrument("IBM");
        assertThat(optionalIbmStat.isPresent()).isTrue();
        ibmStat = optionalIbmStat.get();
        assertThat(ibmStat.getAvg()).isEqualTo(Double.valueOf(120.0));
        assertThat(ibmStat.getMax()).isEqualTo(Double.valueOf(123.0));
        assertThat(ibmStat.getMin()).isEqualTo(Double.valueOf(118.0));
        assertThat(ibmStat.getCount()).isEqualTo(Long.valueOf(6));

        optionalRocStat = priceAggregationService.getStatForInstrument("ROC");
        assertThat(optionalRocStat.isPresent()).isTrue();
        rocStat = optionalRocStat.get();
        assertThat(rocStat.getAvg()).isEqualTo(Double.valueOf(345.0));
        assertThat(rocStat.getMax()).isEqualTo(Double.valueOf(360.0));
        assertThat(rocStat.getMin()).isEqualTo(Double.valueOf(330.0));
        assertThat(rocStat.getCount()).isEqualTo(Long.valueOf(4));

        optionalAbcStat = priceAggregationService.getStatForInstrument("ABC");
        assertThat(optionalAbcStat.isPresent()).isTrue();
        abcStat = optionalAbcStat.get();
        assertThat(abcStat.getAvg()).isEqualTo(Double.valueOf(408.8));
        assertThat(abcStat.getMax()).isEqualTo(Double.valueOf(415.0));
        assertThat(abcStat.getMin()).isEqualTo(Double.valueOf(402.0));
        assertThat(abcStat.getCount()).isEqualTo(Long.valueOf(5));

        sleepFor(1000);

        for (i=25; i<30; i++){
            int n = i;
            new Thread(() -> {
                try {
                    priceAggregationService.processTick(new Tick(records.get(n).get(0), Double.valueOf(records.get(n).get(1)), System.currentTimeMillis()));
                } catch (TickOlderThanAllowedDurationException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        // Let the threads complete execution
        sleepFor(100);

        optionalOverallStat = priceAggregationService.getOverallStat();
        assertThat(optionalOverallStat.isPresent()).isTrue();
        overallStat = optionalOverallStat.get();
        assertThat(overallStat.getAvg()).isEqualTo(Double.valueOf(335.0));
        assertThat(overallStat.getMax()).isEqualTo(Double.valueOf(403.0));
        assertThat(overallStat.getMin()).isEqualTo(Double.valueOf(121.0));
        assertThat(overallStat.getCount()).isEqualTo(Long.valueOf(5));

        optionalIbmStat = priceAggregationService.getStatForInstrument("IBM");
        assertThat(optionalIbmStat.isPresent()).isTrue();
        ibmStat = optionalIbmStat.get();
        assertThat(ibmStat.getAvg()).isEqualTo(Double.valueOf(121.0));
        assertThat(ibmStat.getMax()).isEqualTo(Double.valueOf(121.0));
        assertThat(ibmStat.getMin()).isEqualTo(Double.valueOf(121.0));
        assertThat(ibmStat.getCount()).isEqualTo(Long.valueOf(1));

        optionalRocStat = priceAggregationService.getStatForInstrument("ROC");
        assertThat(optionalRocStat.isPresent()).isTrue();
        rocStat = optionalRocStat.get();
        assertThat(rocStat.getAvg()).isEqualTo(Double.valueOf(375.0));
        assertThat(rocStat.getMax()).isEqualTo(Double.valueOf(380.0));
        assertThat(rocStat.getMin()).isEqualTo(Double.valueOf(370.0));
        assertThat(rocStat.getCount()).isEqualTo(Long.valueOf(2));

        optionalAbcStat = priceAggregationService.getStatForInstrument("ABC");
        assertThat(optionalAbcStat.isPresent()).isTrue();
        abcStat = optionalAbcStat.get();
        assertThat(abcStat.getAvg()).isEqualTo(Double.valueOf(402.0));
        assertThat(abcStat.getMax()).isEqualTo(Double.valueOf(403.0));
        assertThat(abcStat.getMin()).isEqualTo(Double.valueOf(401.0));
        assertThat(abcStat.getCount()).isEqualTo(Long.valueOf(2));
    }


    private void sleepFor(long ms){
        try { Thread.sleep(ms);   } catch (InterruptedException e) {   e.printStackTrace(); }
    }
}
