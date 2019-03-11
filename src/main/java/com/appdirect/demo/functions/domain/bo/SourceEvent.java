package com.appdirect.demo.functions.domain.bo;

import java.util.Map;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;

@Data
public class SourceEvent {

  @NonNull
  private String referenceId;
  private Long processingTimeMillis;

  @NonNull
  private String eventStr;

  @NonNull
  private String configId;

  public RawEvent mapToRawEvent(Function<String, Map<String, String>> splitter) {
    return RawEvent.builder()
        .referenceId(referenceId)
        .processingTimeMillis(processingTimeMillis)
        .fields(splitter.apply(eventStr))
        .build();
  }
}
