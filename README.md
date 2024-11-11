# VWAP Calculation

This project calculates the **Volume Weighted Average Price (VWAP)** for multiple currency pairs based on an incoming stream of price and volume data. The VWAP is computed over an hourly window for each unique currency pair, using Kafka Streams with KStream, Spring Boot, and Gradle for dependency management, and Docker for containerization.

## Solution Outline

This project leverages the following technologies:

- **Kafka Streams (KStream API)**: To manage and process streaming data for each currency pair.
- **Spring Boot**: For setting up the application, managing configurations, and handling dependencies.
- **Gradle**: For dependency management and build automation.
- **Docker**: For containerizing the application, ensuring environment consistency across development, testing, and production.

## Project Setup

### Prerequisites

- **Java 17**
- **Docker**: To run the application in a containerized environment
- **Kafka**: The application expects a Kafka cluster for the stream data
- **Gradle**: For building the project

### Installation

1. **Clone the Repository**
   ```sh
   git clone <repository-url>
   cd vwap-calculation
   ```
2. **Build the Project**
   ```sh
   ./gradlew build
   ```
   
3. **Run with Docker**
- **Ensure Docker is running.**
- **Run the application in a Docker container**
  ```sh
   docker-compose up --build 
  ```

   
## Configuration
Application configuration can be found in the `application.yml` file. Key configurations include:

- **Kafka Broker URL**: `spring.kafka.bootstrap-servers` - Address of your Kafka broker.
- **Stream Configurations**: Window duration and Kafka topic details for input and output streams.

## Usage
The VWAP calculation application listens to a Kafka topic for incoming currency pair data and outputs the computed VWAP for each currency pair on an hourly basis to an output Kafka topic.

### Data Format

Each message in the input Kafka topic should be structured as follows:
```json
{
    "timestamp": "ISO-8601 timestamp",
    "currencyPair": "String representing currency pair (e.g., AUD/USD)",
    "price": "Float representing the price",
    "volume": "Integer representing the volume"
}
```
The output Kafka topic will receive VWAP calculations structured as:
```json
{
    "currencyPair": "String",
    "timestamp": "ISO-8601 timestamp for the end of the hourly window",
    "vwap": "Calculated VWAP for the hour"
}
```

## Testing
The project includes unit and integration tests for the core VWAP calculation logic, Kafka stream handling, and application configuration.
To run tests:
```sh
./gradlew test
```