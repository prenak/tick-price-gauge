package com.idx.tick.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.idx.tick.config.CommonBeanConfiguration;
import com.idx.tick.model.Tick;
import com.idx.tick.service.PriceAggregationService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.util.ResourceUtils;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(TickStatisticsController.class)
@ContextConfiguration(classes = {TickStatisticsController.class, CommonBeanConfiguration.class, PriceAggregationService.class})
@TestPropertySource("classpath:application-test.properties")
public class TickStatisticsControllerTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private PriceAggregationService priceAggregationService;

    private List<CSVRecord> records;


    @Before
    public void setUp() throws IOException {
        priceAggregationService.getTicks().clear();

        File file = ResourceUtils.getFile("classpath:ticks.csv");
        // We use apache.commons-csv to parse the CSV easily
        records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new FileReader(file))
                .getRecords();
    }


    @Test
    public void test_PublishTick_WhenServiceThrows_TickOlderThanAllowedDurationException() throws Exception {
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders
            .post("/ticks")
                .content(asJsonString(new Tick("ABC", 120.0, System.currentTimeMillis()-1001)))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNoContent())
                .andReturn();

        assertThat(mvcResult.getResolvedException())
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("Tick(instrument=ABC, price=120.0, timestamp=")
                .hasMessageContaining(") is older than allowed duration of 1000 milliseconds");
    }


    @Test
    public void test_GetStatisticsForInstrument_ForIllegalArguments() throws Exception {
        MvcResult mvcResult = mockMvc.perform(get("/statistics/ "))
                .andExpect(status().isBadRequest())
                .andReturn();

        assertThat(mvcResult.getResolvedException())
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("Instrument passed is either null or empty");
    }


    @Test
    public void test_PublishTick_AndCheckStats() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .post("/ticks")
                .content(asJsonString(new Tick("ABC", 120.0, System.currentTimeMillis())))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andReturn();

        MvcResult mvcResult = mockMvc.perform(get("/statistics"))
                .andExpect(status().isFound())
                .andReturn();

        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":120.0,\"max\":120.0,\"min\":120.0,\"count\":1}");
    }


    @Test
    public void test_PublishTickContinually_AndCheckStats() throws Exception {
        for (CSVRecord r : records) {
            mockMvc.perform(MockMvcRequestBuilders
                    .post("/ticks")
                    .content(asJsonString(new Tick(r.get(0), Double.valueOf(r.get(1)), System.currentTimeMillis())))
                    .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isCreated())
                    .andReturn();
        }

        MvcResult mvcResult = mockMvc.perform(get("/statistics"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":281.6666666666667,\"max\":415.0,\"min\":118.0,\"count\":30}");

        mvcResult = mockMvc.perform(get("/statistics/ABC"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":406.3,\"max\":415.0,\"min\":400.0,\"count\":10}");

        mvcResult = mockMvc.perform(get("/statistics/IBM"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":120.63636363636364,\"max\":123.0,\"min\":118.0,\"count\":11}");

        mvcResult = mockMvc.perform(get("/statistics/ROC"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":340.0,\"max\":380.0,\"min\":300.0,\"count\":9}");

        sleepFor(1000);

        mvcResult = mockMvc.perform(get("/statistics"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":0.0,\"max\":0.0,\"min\":0.0,\"count\":0}");

        mvcResult = mockMvc.perform(get("/statistics/ABC"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":0.0,\"max\":0.0,\"min\":0.0,\"count\":0}");

        mvcResult = mockMvc.perform(get("/statistics/IBM"))
                .andExpect(status().isFound())
                .andReturn();
        System.out.println(mvcResult.getResponse().getContentAsString());
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":0.0,\"max\":0.0,\"min\":0.0,\"count\":0}");

        mvcResult = mockMvc.perform(get("/statistics/ROC"))
                .andExpect(status().isFound())
                .andReturn();
        System.out.println(mvcResult.getResponse().getContentAsString());
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":0.0,\"max\":0.0,\"min\":0.0,\"count\":0}");
    }


    @Test
    public void test_PublishTickWithAGapOf400Ms_AndCheckStats() throws Exception {
        for (CSVRecord r : records) {
            mockMvc.perform(MockMvcRequestBuilders
                    .post("/ticks")
                    .content(asJsonString(new Tick(r.get(0), Double.valueOf(r.get(1)), System.currentTimeMillis())))
                    .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isCreated())
                    .andReturn();

            sleepFor(400);
        }
        priceAggregationService.regularPriceAggregationCleanUpJob();

        MvcResult mvcResult = mockMvc.perform(get("/statistics"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":390.5,\"max\":401.0,\"min\":380.0,\"count\":2}");

        mvcResult = mockMvc.perform(get("/statistics/ABC"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":401.0,\"max\":401.0,\"min\":401.0,\"count\":1}");

        mvcResult = mockMvc.perform(get("/statistics/IBM"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":0.0,\"max\":0.0,\"min\":0.0,\"count\":0}");

        mvcResult = mockMvc.perform(get("/statistics/ROC"))
                .andExpect(status().isFound())
                .andReturn();
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("{\"avg\":380.0,\"max\":380.0,\"min\":380.0,\"count\":1}");
    }



    private static String asJsonString(final Object obj) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final String jsonContent = mapper.writeValueAsString(obj);
            return jsonContent;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sleepFor(long ms){
        try { Thread.sleep(ms);   } catch (InterruptedException e) {   e.printStackTrace(); }
    }
}
