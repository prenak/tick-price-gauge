package com.idx.tick.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Tick {

    private String instrument;
    private Double price;
    private Long timestamp;
}


