package com.idx.tick.service;

import com.idx.tick.exception.TickOlderThanAllowedDurationException;
import com.idx.tick.model.Stat;
import com.idx.tick.model.Tick;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class PriceAggregationService {

    @Getter
    private final List<Tick> ticks = new CopyOnWriteArrayList<>();
    private final Stat overallStat = new Stat();
    private final Map<String, AtomicReference<Stat>> instrumentStatMap = new ConcurrentHashMap<>();


    @Value("#{T(java.lang.Long).valueOf('${sliding.interval}')}")
    private long slidingIntervalInMs;


    /**
     * Adds the tick to the list if valid and does the price aggregations
     * @param tick Tick
     * @throws TickOlderThanAllowedDurationException if tick is older than the allowed time duration
     */
    public void processTick(Tick tick) throws TickOlderThanAllowedDurationException {
        Assert.notNull(tick, "Tick passed is null");

        if (! didTickHappenInPastOf(System.currentTimeMillis(), tick.getTimestamp())) {
            throw new TickOlderThanAllowedDurationException(tick, slidingIntervalInMs);
        }

        ticks.add(tick);
        log.debug("Added tick {} at {}", tick, System.currentTimeMillis());

        String instrument = tick.getInstrument();
        Stat newInstrumentStat = aggregateTickStats(instrument);
        updateStatForInstrument(instrument, newInstrumentStat);
    }

    /**
     * Fetches the overall price stats across all the instruments based on the ticks added in the current sliding time interval
     * @return Optional of price statistics if it was last updated in the current sliding time interval. Else empty
     */
    public Optional<Stat> getOverallStat() {
        if (ObjectUtils.isEmpty(overallStat.getLastUpdatedTs())) return Optional.empty();

        long currentTimestamp = System.currentTimeMillis();
        return (currentTimestamp - slidingIntervalInMs) <= overallStat.getLastUpdatedTs() ? Optional.of(overallStat) : Optional.empty();
    }


    /**
     * Fetches the price stats for a specific instrument based on the ticks that were added in the current sliding time interval
     * @return Optional of price statistics if it was last updated in the current sliding time interval. Else empty
     */
    public Optional<Stat> getStatForInstrument(String instrument) {
        Assert.hasText(instrument, "Instrument passed is either null or empty");

        Stat instrumentStat = instrumentStatMap.getOrDefault(instrument, new AtomicReference<>()).get();
        if (ObjectUtils.isEmpty(instrumentStat) || ObjectUtils.isEmpty(instrumentStat.getLastUpdatedTs())) return Optional.empty();

        long currentTimestamp = System.currentTimeMillis();
        return (currentTimestamp - slidingIntervalInMs) <= instrumentStat.getLastUpdatedTs() ? Optional.of(instrumentStat) : Optional.empty();
    }


    @Scheduled(cron = "*/10 * * * * *")
    public void regularPriceAggregationCleanUpJob(){
        instrumentStatMap.keySet().forEach(instrument -> {
            // if (System.currentTimeMillis() - instrumentStatMap.get(instrument).get().getLastUpdatedTs() > slidingIntervalInMs)
                updateStatForInstrument(instrument, aggregateTickStats(instrument));
        });
    }


    private synchronized Stat aggregateTickStats(String instrument) {
        Assert.hasText(instrument, "Instrument passed is either null or empty");
        log.debug("Starting the price aggregation for instrument: {}", instrument);
        long currentTimestamp = System.currentTimeMillis();

        DoubleSummaryStatistics dss = ticks.stream()
                .filter(tick -> didTickHappenInPastOf(currentTimestamp, tick.getTimestamp()))
                .mapToDouble(Tick::getPrice)
                .summaryStatistics();
        overallStat.update(dss.getAverage(), dss.getMax(), dss.getMin(), dss.getCount(), currentTimestamp);

        dss = ticks.stream()
                .filter(tick -> instrument.equals(tick.getInstrument())  &&  didTickHappenInPastOf(currentTimestamp, tick.getTimestamp()))
                .mapToDouble(Tick::getPrice)
                .summaryStatistics();
        return new Stat(dss.getAverage(), dss.getMax(), dss.getMin(), dss.getCount(), currentTimestamp);
    }


    private void updateStatForInstrument(String instrument, Stat newInstrumentStat) {
        Assert.hasText(instrument, "Instrument passed is either null or empty");
        Assert.notNull(newInstrumentStat, "NewInstrumentStat passed is null");

        Stat oldInstrumentStat = instrumentStatMap.getOrDefault(instrument, new AtomicReference<>()).get();
        if (instrumentStatMap.containsKey(instrument)){
            instrumentStatMap.get(instrument).compareAndSet(oldInstrumentStat, newInstrumentStat);

        } else {
            if (instrumentStatMap.putIfAbsent(instrument, new AtomicReference<>(newInstrumentStat)) != null) {
                instrumentStatMap.get(instrument).compareAndSet(oldInstrumentStat, newInstrumentStat);
            }
        }
    }


    private boolean didTickHappenInPastOf(long timestampToCompare, long tickTimestamp) {
        return (timestampToCompare - slidingIntervalInMs) <= tickTimestamp;
    }
}
