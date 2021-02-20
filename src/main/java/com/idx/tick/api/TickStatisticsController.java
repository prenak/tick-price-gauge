package com.idx.tick.api;


import com.idx.tick.exception.TickOlderThanAllowedDurationException;
import com.idx.tick.model.Stat;
import com.idx.tick.model.Tick;
import com.idx.tick.model.dto.StatDto;
import com.idx.tick.service.PriceAggregationService;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("")
public class TickStatisticsController {

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private PriceAggregationService priceAggregationService;


    @PostMapping(value = "/ticks", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public void publishTick(@RequestBody Tick tick) {
        log.debug("Received request to publish a tick: {}", tick);
        try {
            priceAggregationService.processTick(tick);

        } catch (IllegalArgumentException iae) {
            log.error("IllegalArgumentException - {}", iae.getMessage(), iae);
            ResponseStatusException responseStatusException = new ResponseStatusException(HttpStatus.BAD_REQUEST, iae.getMessage());
            log.info("Returning ResponseStatusException: ", responseStatusException);
            throw responseStatusException;

        } catch (TickOlderThanAllowedDurationException totade) {
            log.error("TickOlderThanAllowedDurationException - {}", totade.getMessage(), totade);
            ResponseStatusException responseStatusException = new ResponseStatusException(HttpStatus.NO_CONTENT, totade.getMessage());
            log.info("Returning ResponseStatusException: ", responseStatusException);
            throw responseStatusException;
        }
    }


    @GetMapping("/statistics")
    @ResponseStatus(HttpStatus.FOUND)
    public StatDto getOverallStatistics() {
        log.debug("Received a request to fetch overall stat");
        StatDto statDtoResponse = null;
        try {
            Optional<Stat> optionalStat = priceAggregationService.getOverallStat();
            if (optionalStat.isPresent()){
                statDtoResponse = modelMapper.map(optionalStat.get(), StatDto.class);

            } else {
                statDtoResponse = new StatDto();
            }

        } catch (Exception ex){
            log.error("Exception - {}", ex.getMessage(), ex);
            ResponseStatusException responseStatusException = new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
            log.info("Returning ResponseStatusException: ", responseStatusException);
            throw responseStatusException;
        }
        log.debug("Returning {}", statDtoResponse);
        return statDtoResponse;
    }


    @GetMapping("/statistics/{instrument_identifier}")
    @ResponseStatus(HttpStatus.FOUND)
    public StatDto getStatisticsForInstrument(@PathVariable("instrument_identifier") String instrumentIdentifier) {
        log.debug("Received a request to fetch stat for instrument identifier {}", instrumentIdentifier);
        StatDto statDtoResponse = null;
        try {
            Optional<Stat> optionalStat = priceAggregationService.getStatForInstrument(instrumentIdentifier);
            if (optionalStat.isPresent()){
                statDtoResponse = modelMapper.map(optionalStat.get(), StatDto.class);

            } else {
                statDtoResponse = new StatDto();
            }

        } catch (IllegalArgumentException iae) {
            log.error("IllegalArgumentException - {}", iae.getMessage(), iae);
            ResponseStatusException responseStatusException = new ResponseStatusException(HttpStatus.BAD_REQUEST, iae.getMessage());
            log.info("Returning ResponseStatusException: ", responseStatusException);
            throw responseStatusException;

        } catch (Exception ex){
            log.error("Exception - {}", ex.getMessage(), ex);
            ResponseStatusException responseStatusException = new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
            log.info("Returning ResponseStatusException: ", responseStatusException);
            throw responseStatusException;
        }
        log.debug("Returning {}", statDtoResponse);
        return statDtoResponse;
    }
}
