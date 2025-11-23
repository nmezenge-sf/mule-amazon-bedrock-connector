package org.mule.extension.mulechain.internal.agents;

import java.util.Map;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

public class AwsbedrockAgentsFilteringParameters {

  public enum RetrievalMetadataFilterType {
    AND_ALL, OR_ALL
  }

  public enum SearchType {
    HYBRID, SEMANTIC
  }

  public static class KnowledgeBaseConfig {

    @Parameter
    @Optional
    private String knowledgeBaseId;

    @Parameter
    @Optional
    private Integer numberOfResults;

    @Parameter
    @Optional
    private SearchType overrideSearchType;

    @Parameter
    @Optional
    private RetrievalMetadataFilterType retrievalMetadataFilterType;

    @Parameter
    @Optional
    private Map<String, String> metadataFilters;

    public KnowledgeBaseConfig() {}

    public KnowledgeBaseConfig(String knowledgeBaseId, Integer numberOfResults, SearchType overrideSearchType,
                               RetrievalMetadataFilterType retrievalMetadataFilterType, Map<String, String> metadataFilters) {
      this.knowledgeBaseId = knowledgeBaseId;
      this.numberOfResults = numberOfResults;
      this.overrideSearchType = overrideSearchType;
      this.retrievalMetadataFilterType = retrievalMetadataFilterType;
      this.metadataFilters = metadataFilters;
    }

    public String getKnowledgeBaseId() {
      return knowledgeBaseId;
    }

    public Integer getNumberOfResults() {
      return numberOfResults;
    }

    public SearchType getOverrideSearchType() {
      return overrideSearchType;
    }

    public RetrievalMetadataFilterType getRetrievalMetadataFilterType() {
      return retrievalMetadataFilterType;
    }

    public Map<String, String> getMetadataFilters() {
      return metadataFilters;
    }
  }

  @Parameter
  @Optional
  private String knowledgeBaseId;

  @Parameter
  @Optional
  private Integer numberOfResults;

  @Parameter
  @Optional
  private SearchType overrideSearchType;

  @Parameter
  @Optional
  private RetrievalMetadataFilterType retrievalMetadataFilterType;

  @Parameter
  @Optional
  private Map<String, String> metadataFilters;

  public String getKnowledgeBaseId() {
    return knowledgeBaseId;
  }

  // Note: per-KB structured configs were moved to a dedicated parameter group.

  public Integer getNumberOfResults() {
    return numberOfResults;
  }

  public SearchType getOverrideSearchType() {
    return overrideSearchType;
  }

  public RetrievalMetadataFilterType getRetrievalMetadataFilterType() {
    return retrievalMetadataFilterType;
  }

  public Map<String, String> getMetadataFilters() {
    return metadataFilters;
  }
}
