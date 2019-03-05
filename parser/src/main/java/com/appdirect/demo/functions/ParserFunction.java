package com.appdirect.demo.functions;

import static org.slf4j.LoggerFactory.getLogger;

import com.appdirect.demo.functions.domain.bo.CsvParserConfig;
import com.appdirect.demo.functions.domain.bo.RawEvent;
import com.appdirect.demo.functions.domain.bo.UploadNotification;
import com.appdirect.demo.functions.domain.exception.ParserException;
import com.appdirect.demo.functions.domain.exception.ParserIOException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.simpleflatmapper.csv.CsvParser;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@SpringBootApplication
public class ParserFunction implements Function<UploadNotification, RawEvent> {

  private static final Logger LOGGER = getLogger(MethodHandles.lookup().lookupClass());

  private Storage storage;

  @Autowired
  public ParserFunction(Storage storage) {
    this.storage = storage;
  }


  @Override
  public RawEvent apply(UploadNotification notif) {

    try {
      Blob gcsFile = gcsFile(notif.getBucketId(), notif.getObjectId());
      Path dest = downloadLocal(gcsFile);
      removeRemote(gcsFile);

      CsvParserConfig parserConfig = parserConfig();

      Stream<String[]> records = parse(dest, parserConfig.delimiter());
      records.map(r -> mapper(parserConfig()));

      // todo: stream the response and send each message to channel

    } catch (StorageException e) {
      throw new ParserException("Storage API error", e);
    } catch (IOException e) {
      throw new ParserIOException("IO error in parsing", e);
    }

    return null;
  }

  //......##### internal #####......//


  private Function<? extends String[], RawEvent> mapper(CsvParserConfig parserConfig) {
    return r -> RawEvent.builder().referenceId(UUID.randomUUID().toString()).build();
  }

  private Path downloadLocal(Blob gcsFile) throws StorageException {
    Path dest = Paths.get(System.getProperty("java.io.tmpdir"), gcsFile.getBlobId().getName());
    gcsFile.downloadTo(dest);
    return dest;
  }

  private void removeRemote(Blob gcsFile) throws StorageException {
    boolean deleted = gcsFile.delete();
    if (!deleted) {
      LOGGER.error("Unable to delete remote file in google storage");
    }
  }

  private Blob gcsFile(String bucketId, String objectId) throws StorageException {
    return storage.get(BlobId.of(bucketId, objectId));
  }

  private CsvParserConfig parserConfig() {
    InputStream is = getClass().getClassLoader().getResourceAsStream("schema/parser.yaml");
    Yaml yaml = new Yaml(new Constructor(CsvParserConfig.class));
    return yaml.load(is);
  }

  private Stream<String[]> parse(Path p, char delim) throws IOException {
    CsvParser.DSL parser = CsvParser.separator(delim).trimSpaces();
    return parser.stream(new FileReader(p.toFile()));
  }

  @SuppressWarnings("unused")
  private Stream<String[]> parse(String payload, char delim) throws IOException {
    CsvParser.DSL parser = CsvParser.separator(delim).trimSpaces();
    return parser.stream(payload);
  }

  public static void main(String[] args) {
    SpringApplication.run(ParserFunction.class, args);
  }
}
