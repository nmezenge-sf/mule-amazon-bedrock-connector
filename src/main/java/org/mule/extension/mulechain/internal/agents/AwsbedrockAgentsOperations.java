package org.mule.extension.mulechain.internal.agents;

import static org.apache.commons.io.IOUtils.toInputStream;
import static org.mule.runtime.extension.api.annotation.param.MediaType.APPLICATION_JSON;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.mule.extension.mulechain.helpers.AwsbedrockAgentsPayloadHelper;
import org.mule.extension.mulechain.internal.AwsbedrockConfiguration;
import org.mule.extension.mulechain.internal.BedrockErrorsProvider;
import org.mule.runtime.api.meta.model.operation.ExecutionType;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.execution.Execution;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;

/**
 * This class is a container for operations, every public method in this class will be taken as an extension operation.
 */
public class AwsbedrockAgentsOperations {

  /**
   * Lists all agents for the configuration
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-list")
  public InputStream listAgents(@Config AwsbedrockConfiguration configuration,
                                @ParameterGroup(name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.ListAgents(configuration, awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Get agent by its Id
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Alias("AGENT-get-by-id")
  @Execution(ExecutionType.BLOCKING)
  public InputStream getAgentById(String agentId, @Config AwsbedrockConfiguration configuration,
                                  @ParameterGroup(
                                      name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.getAgentbyAgentId(agentId, configuration, awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Get agent by its Name
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-get-by-name")
  public InputStream getAgentByName(String agentName, @Config AwsbedrockConfiguration configuration,
                                    @ParameterGroup(
                                        name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.getAgentbyAgentName(agentName, configuration, awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Delete agent by its Id
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-delete-by-id")
  public InputStream deleteAgentById(String agentId, @Config AwsbedrockConfiguration configuration,
                                     @ParameterGroup(
                                         name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.deleteAgentByAgentId(agentId, configuration, awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Creates an agent with alias
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-create")
  public InputStream createAgentWithAlias(String agentName, String instructions,
                                          @Config AwsbedrockConfiguration configuration,
                                          @ParameterGroup(
                                              name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.createAgentOperation(agentName, instructions, configuration,
                                                                         awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Creates an agent alias
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-create-alias")
  public InputStream createAgentAlias(String agentAlias, String agentId, @Config AwsbedrockConfiguration configuration,
                                      @ParameterGroup(
                                          name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.createAgentAlias(agentAlias, agentId, configuration,
                                                                     awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Get agent alias by its Id
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-get-alias-by-agent-id")
  public InputStream getAgentAliasById(String agentId, @Config AwsbedrockConfiguration configuration,
                                       @ParameterGroup(
                                           name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.listAllAgentAliases(agentId, configuration, awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Get agent alias by its Id
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-delete-agent-aliases")
  public InputStream deleteAgentAlias(String agentId, String agentAliasName,
                                      @Config AwsbedrockConfiguration configuration,
                                      @ParameterGroup(
                                          name = "Additional properties") AwsbedrockAgentsParameters awsBedrockParameters) {
    String response = AwsbedrockAgentsPayloadHelper.deleteAgentAliasesByAgentId(agentId, agentAliasName, configuration,
                                                                                awsBedrockParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  /**
   * Chat with an agent
   */
  @MediaType(value = APPLICATION_JSON, strict = false)
  @Throws(BedrockErrorsProvider.class)
  @Execution(ExecutionType.BLOCKING)
  @Alias("AGENT-chat")
  public InputStream chatWithAgent(String agentId, String agentAliasId,
                                   String prompt,
                                   boolean enableTrace, boolean latencyOptimized,
                                   @Config AwsbedrockConfiguration configuration,
                                   @ParameterGroup(
                                       name = "Session properties") AwsbedrockAgentsSessionParameters awsBedrockSessionParameters,
                                   @ParameterGroup(
                                       name = "Knowledge Base Metadata Filtering (for single KB Id)") AwsbedrockAgentsFilteringParameters awsBedrockAgentsFilteringParameters,
                                   @ParameterGroup(
                                       name = "Knowledge Base Metadata Filtering (for multiple KB Ids)") AwsbedrockAgentsMultipleFilteringParameters awsBedrockAgentsMultipleFilteringParameters,
                                   @ParameterGroup(
                                       name = "Additional properties") AwsbedrockAgentsParameters awsBedrockAgentsParameters) {
    String response = AwsbedrockAgentsPayloadHelper.chatWithAgent(agentAliasId, agentId, prompt, enableTrace,
                                                                  latencyOptimized, configuration, awsBedrockSessionParameters,
                                                                  awsBedrockAgentsFilteringParameters,
                                                                  awsBedrockAgentsMultipleFilteringParameters,
                                                                  awsBedrockAgentsParameters);
    return toInputStream(response, StandardCharsets.UTF_8);
  }

  @MediaType(value = "text/event-stream", strict = false)
  @Alias("AGENT-chat-streaming-SSE")
  @DisplayName("Agent chat streaming (SSE)")
  public InputStream chatWithAgentSSEStream(String agentId, String agentAliasId,
                                            String prompt,
                                            boolean enableTrace, boolean latencyOptimized,
                                            @Config AwsbedrockConfiguration configuration,
                                            @ParameterGroup(
                                                name = "Session properties") AwsbedrockAgentsSessionParameters awsBedrockSessionParameters,
                                            @ParameterGroup(
                                                name = "Knowledge Base Metadata Filtering (for single KB Id)") AwsbedrockAgentsFilteringParameters awsBedrockAgentsFilteringParameters,
                                            @ParameterGroup(
                                                name = "Knowledge Base Metadata Filtering (for multiple KB Ids)") AwsbedrockAgentsMultipleFilteringParameters awsBedrockAgentsMultipleFilteringParameters,
                                            @ParameterGroup(
                                                name = "Additional properties") AwsbedrockAgentsParameters awsBedrockAgentsParameters) {
    return AwsbedrockAgentsPayloadHelper.chatWithAgentSSEStream(agentAliasId, agentId, prompt, enableTrace,
                                                                latencyOptimized, configuration, awsBedrockSessionParameters,
                                                                awsBedrockAgentsFilteringParameters,
                                                                awsBedrockAgentsMultipleFilteringParameters,
                                                                awsBedrockAgentsParameters);
  }

}
