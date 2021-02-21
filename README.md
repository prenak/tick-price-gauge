# Tick Price Gauge
#### Tick-Price-Gauge is a Spring Boot application that provides REST interfaces to process ticks and perform price aggregation. It supports the following functionalities:
- Publish and process ticks which are not older than allowed time duration.
- Fetch the overall price statistics across all the instruments based on the ticks added in the current sliding time interval.
- Fetch the price statistics for a specific instrument based on the ticks that were added in the current sliding time interval.

## How to  Run

### Starting the application:
Run the below commands to bring up the application.
```bash
cd tick-price-gauge
mvn spring-boot:run
```
The application would be up and running on 8084 port.

### Application monitoring:
Application health and info can be monitored via actuator management context.
- http://localhost:8084/actuator/health
- http://localhost:8084/actuator/info  


## REST Endpoints

### 1.	POST  /ticks
Publish and process ticks which are not older than allowed time duration.  
Returns:  
- 201 Status if processed successfully.  
- 204 Status if tick is older than allowed time interval.  

Example: http://localhost:8084/ticks  
```bash
{
	"instrument": "IBM.N",
	"price": 143.82,
	"timestamp": 1478192204000
}
```

### 2.	GET  /statistics
Fetches aggregated statistics for all ticks across all instruments that happened in last sliding time interval.   
Example: http://localhost:8084/statistics  
```bash
{
	"avg": 100,
	"max": 200,
	"min": 50,
	"count": 10
}
```

### 3.	GET  /statistics/{instrument_identifier}
Fetches aggregated statistics for all ticks for a specific instrument that happened in last sliding time interval.     
Example: http://localhost:8084/statistics/ABC  
```bash
{
	"avg": 100,
	"max": 200,
	"min": 50,
	"count": 10
}
```