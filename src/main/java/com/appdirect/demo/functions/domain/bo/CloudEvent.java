package com.appdirect.demo.functions.domain.bo;

import com.google.gson.internal.LinkedTreeMap;
import java.net.URI;
import lombok.Data;

@Data
public class CloudEvent {

  private String id;
  private String type;
  private String contentType;
  private LinkedTreeMap<String, Object> data;
  private URI source;
  private String specVersion;
}
