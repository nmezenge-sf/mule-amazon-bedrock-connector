package org.mule.extension.mulechain.internal.agents;

import java.util.List;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

/**
 * Parameter group for passing multiple knowledge-base configurations (per-KB settings).
 */
public class AwsbedrockAgentsMultipleFilteringParameters {

  @Parameter
  @Optional
  private List<AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> knowledgeBases;

  public AwsbedrockAgentsMultipleFilteringParameters() {}

  public List<AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> getKnowledgeBases() {
    return knowledgeBases;
  }
}
