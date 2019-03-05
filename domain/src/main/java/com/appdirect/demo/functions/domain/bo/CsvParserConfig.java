package com.appdirect.demo.functions.domain.bo;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CsvParserConfig {

  private String delimiter;
  private List<Metadata> metadata;

  @Data
  @NoArgsConstructor
  public static class Metadata {

    private int index;
    private String id;
  }

  public char delimiter() {
    return delimiter.charAt(0);
  }
}
