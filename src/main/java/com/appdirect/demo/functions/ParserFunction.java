package com.appdirect.demo.functions;

import static java.lang.String.format;
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
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@SpringBootApplication
public class ParserFunction implements Function<CloudEvent, RawEvent> {

  private static final Logger LOGGER = getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public RawEvent apply(CloudEvent cEvent) {

    LOGGER.info("Received: {}", cEvent);

    SourceEvent srcEvent = sourceEvent(cEvent);
    CsvParserConfig config = parserConfig(srcEvent.getConfigId());
    return srcEvent.mapToRawEvent(parser(config));
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
        .configId((String) rawEvent.get("configId"))
        .build();
  }

  private CsvParserConfig parserConfig(String configId) {
    InputStream is = getClass().getClassLoader()
        .getResourceAsStream(format("schema/%s.yaml", configId));
    Yaml yaml = new Yaml(new Constructor(CsvParserConfig.class));
    return yaml.load(is);
  }

  public static void main(String[] args) {
    SpringApplication.run(ParserFunction.class, args);
  }
}
