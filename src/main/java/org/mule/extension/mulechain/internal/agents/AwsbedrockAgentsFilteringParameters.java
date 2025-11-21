package org.mule.extension.mulechain.internal.agents;

import java.util.Map;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.sdk.api.annotation.param.Optional;

public class AwsbedrockAgentsFilteringParameters {

  public enum RetrievalMetadataFilterType {
    AND_ALL, OR_ALL
  }

  @Parameter
  @Optional
  private String knowledgeBaseId;

  @Parameter
  @Optional
  private Integer numberOfResults;

  @Parameter
  @Optional
  private String overrideSearchType;

  @Parameter
  @Optional
  private RetrievalMetadataFilterType retrievalMetadataFilterType;

  @Parameter
  @Optional
  private Map<String, String> metadataFilters;

  public String getKnowledgeBaseId() {
    return knowledgeBaseId;
  }

  public Integer getNumberOfResults() {
    return numberOfResults;
  }

  public String getOverrideSearchType() {
    return overrideSearchType;
  }

  public RetrievalMetadataFilterType getRetrievalMetadataFilterType() {
    return retrievalMetadataFilterType;
  }

  public Map<String, String> getMetadataFilters() {
    return metadataFilters;
  }
}
