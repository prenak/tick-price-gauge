package com.idx.tick.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor
public class Stat {

    private Double avg;
    private Double max;
    private Double min;
    private Long count;
    private volatile Long lastUpdatedTs;


    public Stat(Double avg, Double max, Double min, Long count, Long updateTimestamp) {
        this.avg = avg;
        this.max = (count == 0) ? 0.0 : max;
        this.min = (count == 0) ? 0.0 : min;
        this.count = count;
        this.lastUpdatedTs = updateTimestamp != null ? updateTimestamp : System.currentTimeMillis();
    }


    public void update(Double average, Double maxPrice, Double minPrice, Long tickCount, Long updateTimestamp) {
        this.avg = average;
        this.max = (tickCount == 0) ? 0.0 : maxPrice;
        this.min = (tickCount == 0) ? 0.0 : minPrice;
        this.count = tickCount;
        this.lastUpdatedTs = updateTimestamp != null ? updateTimestamp : System.currentTimeMillis();
    }
}
