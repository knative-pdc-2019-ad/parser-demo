package com.appdirect.demo.functions.domain.exception;

import java.io.IOException;

public final class ParserIOException extends ParserException {

  public ParserIOException(String message) {
    super(message);
  }

  public ParserIOException(String message, IOException cause) {
    super(message, cause);
  }
}
