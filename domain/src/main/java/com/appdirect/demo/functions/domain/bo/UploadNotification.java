package com.appdirect.demo.functions.domain.bo;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
public class UploadNotification {

  private String eventType;
  private String payloadFormat;
  private String bucketId;
  private String objectId;
  private String objectGeneration;
}
