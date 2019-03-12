package com.appdirect.demo.functions.domain.bo;

import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class SourceEvent {

  @NonNull
  private String referenceId;

  @NonNull
  private String eventStr;

  public RawEvent mapToRawEvent(Function<String, Map<String, String>> splitter) {
    return RawEvent.builder()
        .referenceId(referenceId)
        .fields(splitter.apply(eventStr))
        .build();
  }
}
