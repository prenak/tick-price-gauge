package com.idx.tick.model.dto;

import lombok.Data;

@Data
public class StatDto {

    private Double avg = 0.0;
    private Double max = 0.0;
    private Double min = 0.0;
    private Long count = 0L;
}
