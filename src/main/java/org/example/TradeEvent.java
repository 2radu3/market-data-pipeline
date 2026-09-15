package org.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true) //Drops unused fields from the Binance payload
public record TradeEvent(
        @JsonProperty("t") long tradeId,
        @JsonProperty("s") String symbol,
        @JsonProperty("p") BigDecimal price, //BigDecimal preserves exact monetary precision without floating-point rounding numbers
        @JsonProperty("q") BigDecimal quantity,
        @JsonProperty("T") long tradeTimeMs,
        @JsonProperty("m") boolean isBuyerMaker
) {
    public Instant getTradeInstant() {
        return Instant.ofEpochMilli(tradeTimeMs);
    }
}
