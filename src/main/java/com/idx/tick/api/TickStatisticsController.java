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

/**
 *  TickStatisticsController : REST API that provides the following functionalities -
 *  1. Publish ticks which are not older than allowed duration
 *  2. Fetch the overall price statistics based on the ticks of all instruments in the sliding time interval
 *  3. Fetch the price statistics based on the ticks of one instrument in the sliding time interval
 */


@Slf4j
@RestController
@RequestMapping("")
public class TickStatisticsController {

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private PriceAggregationService priceAggregationService;


    /**
     * Publish ticks which are not older than allowed time duration.
     * Returns  201 status if successfully published.
     *          204 status if the tick is older than predefined allowed time duration
     * @param tick : pojo holding information about the instrument
     */
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


    /**
     * Fetches aggregated statistics for all ticks across all instruments that happened in last sliding time interval.
     * @return  If success, returns 302 status with aggregated statistics for all ticks across all instruments.
     *          Returns status 500 in case of any unexpected internal errors.
     */
    @GetMapping(value = "/statistics", produces = MediaType.APPLICATION_JSON_VALUE)
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


    /**
     * Fetches aggregated statistics for all ticks for a specific instrument that happened in last sliding time interval.
     * @param instrumentIdentifier identifier for the instrument
     * @return If success, returns 302 status with aggregated statistics for the given instrument.
     *         Returns status 500 in case of any unexpected internal errors.
     */
    @GetMapping(value = "/statistics/{instrument_identifier}", produces = MediaType.APPLICATION_JSON_VALUE)
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
