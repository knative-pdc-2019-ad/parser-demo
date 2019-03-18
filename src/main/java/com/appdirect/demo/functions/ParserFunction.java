package com.appdirect.demo.functions;

import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import com.appdirect.demo.functions.domain.bo.CloudEvent;
import com.appdirect.demo.functions.domain.bo.CsvParserConfig;
import com.appdirect.demo.functions.domain.bo.CsvParserConfig.Metadata;
import com.appdirect.demo.functions.domain.bo.RawEvent;
import com.appdirect.demo.functions.domain.bo.SourceEvent;
import com.google.gson.internal.LinkedTreeMap;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@SpringBootApplication
public class ParserFunction {

  private static final Logger LOGGER = getLogger(MethodHandles.lookup().lookupClass());

  @Bean
  public Function<CloudEvent, RawEvent> parse() {
    return cEvent -> {
      LOGGER.info("Received: {}", cEvent);
      SourceEvent srcEvent = sourceEvent(cEvent);
      CsvParserConfig config = parserConfig();

      return srcEvent.mapToRawEvent(parser(config));
    };
  }

  //......##### internal #####......//


  private Function<String, Map<String, String>> parser(CsvParserConfig parserConfig) {
    return str -> {
      String[] tokens = str.split(parserConfig.getDelimiter());
      return parserConfig.getMetadata().stream()
          .collect(toMap(Metadata::getId, meta -> tokens[meta.getIndex()]));
    };
  }

  private SourceEvent sourceEvent(CloudEvent cEvent) {
    LinkedTreeMap<String, Object> rawEvent = cEvent.getData();

    return SourceEvent.builder()
        .referenceId((String) rawEvent.get("referenceId"))
        .eventStr((String) rawEvent.get("eventStr"))
        .build();
  }

  private CsvParserConfig parserConfig() {
    InputStream is = getClass().getClassLoader()
        .getResourceAsStream("schema/parser.yaml");
    Yaml yaml = new Yaml(new Constructor(CsvParserConfig.class));
    return yaml.load(is);
  }

  public static void main(String[] args) {
    SpringApplication.run(ParserFunction.class, args);
  }
}
