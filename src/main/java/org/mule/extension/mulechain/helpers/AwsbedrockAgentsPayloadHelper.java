package org.mule.extension.mulechain.helpers;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mule.extension.mulechain.internal.AwsbedrockConfiguration;
import org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters;
import org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsMultipleFilteringParameters;
import org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsParameters;
import org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsSessionParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockagent.BedrockAgentClient;
import software.amazon.awssdk.services.bedrockagent.model.Agent;
import software.amazon.awssdk.services.bedrockagent.model.AgentAlias;
import software.amazon.awssdk.services.bedrockagent.model.AgentAliasSummary;
import software.amazon.awssdk.services.bedrockagent.model.AgentStatus;
import software.amazon.awssdk.services.bedrockagent.model.AgentSummary;
import software.amazon.awssdk.services.bedrockagent.model.CreateAgentAliasRequest;
import software.amazon.awssdk.services.bedrockagent.model.CreateAgentAliasResponse;
import software.amazon.awssdk.services.bedrockagent.model.CreateAgentRequest;
import software.amazon.awssdk.services.bedrockagent.model.CreateAgentResponse;
import software.amazon.awssdk.services.bedrockagent.model.DeleteAgentAliasRequest;
import software.amazon.awssdk.services.bedrockagent.model.DeleteAgentAliasResponse;
import software.amazon.awssdk.services.bedrockagent.model.DeleteAgentRequest;
import software.amazon.awssdk.services.bedrockagent.model.DeleteAgentResponse;
import software.amazon.awssdk.services.bedrockagent.model.GetAgentRequest;
import software.amazon.awssdk.services.bedrockagent.model.GetAgentResponse;
import software.amazon.awssdk.services.bedrockagent.model.ListAgentAliasesRequest;
import software.amazon.awssdk.services.bedrockagent.model.ListAgentAliasesResponse;
import software.amazon.awssdk.services.bedrockagent.model.ListAgentsRequest;
import software.amazon.awssdk.services.bedrockagent.model.ListAgentsResponse;
import software.amazon.awssdk.services.bedrockagent.model.PrepareAgentRequest;
import software.amazon.awssdk.services.bedrockagent.model.PrepareAgentResponse;
import software.amazon.awssdk.services.bedrockagentruntime.BedrockAgentRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockagentruntime.model.*;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.GetRoleRequest;
import software.amazon.awssdk.services.iam.model.GetRoleResponse;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.NoSuchEntityException;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.Role;

public class AwsbedrockAgentsPayloadHelper {

  private static final Logger logger = LoggerFactory.getLogger(AwsbedrockAgentsPayloadHelper.class);

  // JSON Keys
  private static final String AGENT_NAMES = "agentNames";
  private static final String AGENT_ID = "agentId";
  private static final String AGENT_NAME = "agentName";
  private static final String AGENT_ARN = "agentArn";
  private static final String AGENT_STATUS = "agentStatus";
  private static final String AGENT_RESOURCE_ROLE_ARN = "agentResourceRoleArn";
  private static final String CLIENT_TOKEN = "clientToken";
  private static final String CREATED_AT = "createdAt";
  private static final String DESCRIPTION = "description";
  private static final String FOUNDATION_MODEL = "foundationModel";
  private static final String IDLE_SESSION_TTL_IN_SECONDS = "idleSessionTTLInSeconds";
  private static final String INSTRUCTION = "instruction";
  private static final String PROMPT_OVERRIDE_CONFIGURATION = "promptOverrideConfiguration";
  private static final String UPDATED_AT = "updatedAt";
  private static final String AGENT_ALIAS_ID = "agentAliasId";
  private static final String AGENT_ALIAS_NAME = "agentAliasName";
  private static final String AGENT_ALIAS_ARN = "agentAliasArn";
  private static final String AGENT_ALIAS_SUMMARIES = "agentAliasSummaries";
  private static final String AGENT_ALIAS_STATUS = "agentAliasStatus";
  private static final String AGENT_VERSION = "agentVersion";
  private static final String PREPARED_AT = "preparedAt";
  private static final String SESSION_ID = "sessionId";
  private static final String AGENT_ALIAS = "agentAlias";
  private static final String PROMPT = "prompt";
  private static final String PROCESSED_AT = "processedAt";
  private static final String CHUNKS = "chunks";
  private static final String SUMMARY = "summary";
  private static final String TOTAL_CHUNKS = "totalChunks";
  private static final String FULL_RESPONSE = "fullResponse";
  private static final String TYPE = "type";
  private static final String CHUNK = "chunk";
  private static final String TIMESTAMP = "timestamp";
  private static final String TEXT = "text";
  private static final String CITATIONS = "citations";
  private static final String GENERATED_RESPONSE_PART = "generatedResponsePart";
  private static final String RETRIEVED_REFERENCES = "retrievedReferences";
  private static final String CONTENT = "content";
  private static final String LOCATION = "location";
  private static final String METADATA = "metadata";

  // Messages
  private static final String NO_AGENT_FOUND = "No Agent found!";

  // IAM and Agent Configuration
  private static final String AGENT_EXECUTION_ROLE_PREFIX = "AmazonBedrockExecutionRoleForAgents_";
  private static final String AGENT_ROLE_POLICY_NAME = "agent_permissions";
  private static final String AGENT_POSTFIX = "muc";
  private static final long AGENT_STATUS_CHECK_INTERVAL_MS = 2000;

  private static final AtomicInteger eventCounter = new AtomicInteger(0);

  public static String ListAgents(AwsbedrockConfiguration configuration,
                                  AwsbedrockAgentsParameters awsBedrockParameters) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameters);
      String listOfAgents = getAgentNames(bedrockAgent);
      return listOfAgents;
    });
  }

  private static String getAgentNames(BedrockAgentClient bedrockagent) {
    // Build a ListAgentsRequest instance without any filter criteria
    ListAgentsRequest listAgentsRequest = ListAgentsRequest.builder().build();

    // Call the listAgents method of the BedrockAgentClient instance
    ListAgentsResponse listAgentsResponse = bedrockagent.listAgents(listAgentsRequest);

    // Retrieve the list of agent summaries from the ListAgentsResponse instance
    List<AgentSummary> agentSummaries = listAgentsResponse.agentSummaries();

    // Extract the list of agent names from the list of agent summaries
    List<String> agentNames = agentSummaries.stream()
        .map(AgentSummary::agentName) // specify the type of the elements returned by the map() method
        .collect(Collectors.toList());

    // Create a JSONArray to store the agent names
    JSONArray jsonArray = new JSONArray(agentNames);

    // Create a JSONObject to store the JSONArray
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(AGENT_NAMES, jsonArray);

    // Convert the JSONObject to a JSON string
    String jsonString = jsonObject.toString();
    return jsonString;

  }

  private static Agent getAgentById(String agentId, BedrockAgentClient bedrockAgentClient) {
    // Build a GetAgentRequest instance with the agent ID
    GetAgentRequest getAgentRequest = GetAgentRequest.builder()
        .agentId(agentId)
        .build();

    // Call the getAgent method of the BedrockAgentClient instance
    GetAgentResponse getAgentResponse = bedrockAgentClient.getAgent(getAgentRequest);

    // Retrieve the agent from the GetAgentResponse instance
    Agent agent = getAgentResponse.agent();

    return agent;
  }

  public static String getAgentbyAgentId(String agentId, AwsbedrockConfiguration configuration,
                                         AwsbedrockAgentsParameters awsBedrockParameters) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameters);
      Agent agent = getAgentById(agentId, bedrockAgent);
      JSONObject jsonObject = new JSONObject();
      jsonObject.put(AGENT_ID, agent.agentId());
      jsonObject.put(AGENT_NAME, agent.agentName());
      jsonObject.put(AGENT_ARN, agent.agentArn());
      jsonObject.put(AGENT_STATUS, agent.agentStatusAsString());
      jsonObject.put(AGENT_RESOURCE_ROLE_ARN, agent.agentResourceRoleArn());
      jsonObject.put(CLIENT_TOKEN, agent.clientToken());
      jsonObject.put(CREATED_AT, agent.createdAt());
      jsonObject.put(DESCRIPTION, agent.description());
      jsonObject.put(FOUNDATION_MODEL, agent.foundationModel());
      jsonObject.put(IDLE_SESSION_TTL_IN_SECONDS, agent.idleSessionTTLInSeconds());
      jsonObject.put(INSTRUCTION, agent.instruction());
      jsonObject.put(PROMPT_OVERRIDE_CONFIGURATION, agent.promptOverrideConfiguration());
      jsonObject.put(UPDATED_AT, agent.updatedAt());

      return jsonObject.toString();
    });
  }

  private static Optional<Agent> getAgentByName(String agentName, BedrockAgentClient bedrockAgentClient) {
    // Build a ListAgentsRequest instance without any filter criteria
    ListAgentsRequest listAgentsRequest = ListAgentsRequest.builder()
        .build();

    // Call the listAgents method of the BedrockAgentClient instance
    ListAgentsResponse listAgentsResponse = bedrockAgentClient.listAgents(listAgentsRequest);

    // Retrieve the list of agent summaries from the ListAgentsResponse instance
    List<AgentSummary> agentSummaries = listAgentsResponse.agentSummaries();

    // Iterate through the list of agent summaries to find the one with the
    // specified name
    for (AgentSummary agentSummary : agentSummaries) {
      if (agentSummary.agentName().equals(agentName)) {
        // Retrieve the agent ID from the agent summary
        String agentId = agentSummary.agentId();

        // Build a GetAgentRequest instance with the agent ID
        GetAgentRequest getAgentRequest = GetAgentRequest.builder()
            .agentId(agentId)
            .build();

        // Call the getAgent method of the BedrockAgentClient instance
        GetAgentResponse getAgentResponse = bedrockAgentClient.getAgent(getAgentRequest);

        // Retrieve the agent from the GetAgentResponse instance
        Agent agent = getAgentResponse.agent();

        // Return the agent as an Optional object
        return Optional.of(agent);
      }
    }

    // No agent with the specified name was found
    return Optional.empty();
  }

  public static String getAgentbyAgentName(String agentName, AwsbedrockConfiguration configuration,
                                           AwsbedrockAgentsParameters awsBedrockParameters) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameters);
      Optional<Agent> optionalAgent = getAgentByName(agentName, bedrockAgent);
      if (optionalAgent.isPresent()) {
        Agent agent = optionalAgent.get();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(AGENT_ID, agent.agentId());
        jsonObject.put(AGENT_NAME, agent.agentName());
        jsonObject.put(AGENT_ARN, agent.agentArn());
        jsonObject.put(AGENT_STATUS, agent.agentStatusAsString());
        jsonObject.put(AGENT_RESOURCE_ROLE_ARN, agent.agentResourceRoleArn());
        jsonObject.put(CLIENT_TOKEN, agent.clientToken());
        jsonObject.put(CREATED_AT, agent.createdAt());
        jsonObject.put(DESCRIPTION, agent.description());
        jsonObject.put(FOUNDATION_MODEL, agent.foundationModel());
        jsonObject.put(IDLE_SESSION_TTL_IN_SECONDS, agent.idleSessionTTLInSeconds());
        jsonObject.put(INSTRUCTION, agent.instruction());
        jsonObject.put(PROMPT_OVERRIDE_CONFIGURATION, agent.promptOverrideConfiguration());
        jsonObject.put(UPDATED_AT, agent.updatedAt());
        return jsonObject.toString();
      } else {
        return NO_AGENT_FOUND;
      }
    });
  }

  public static String createAgentOperation(String name, String instruction, AwsbedrockConfiguration configuration,
                                            AwsbedrockAgentsParameters awsBedrockParameter) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameter);

      Role agentRole = createAgentRole(AGENT_POSTFIX, AGENT_ROLE_POLICY_NAME, configuration, awsBedrockParameter);

      Agent agent = createAgent(name, instruction, awsBedrockParameter.getModelName(), agentRole, bedrockAgent);

      PrepareAgentResponse agentDetails = prepareAgent(agent.agentId(), bedrockAgent);

      // AgentAlias AgentAlias = createAgentAlias(name, agent.agentId(),bedrockAgent);

      JSONObject jsonRequest = new JSONObject();
      jsonRequest.put(AGENT_ID, agentDetails.agentId());
      jsonRequest.put(AGENT_VERSION, agentDetails.agentVersion());
      jsonRequest.put(AGENT_STATUS, agentDetails.agentStatusAsString());
      jsonRequest.put(PREPARED_AT, agentDetails.preparedAt());
      jsonRequest.put(AGENT_ARN, agent.agentArn());
      jsonRequest.put(AGENT_NAME, agent.agentName());
      jsonRequest.put(AGENT_RESOURCE_ROLE_ARN, agent.agentResourceRoleArn());
      jsonRequest.put(CLIENT_TOKEN, agent.clientToken());
      jsonRequest.put(CREATED_AT, agent.createdAt());
      jsonRequest.put(DESCRIPTION, agent.description());
      jsonRequest.put(FOUNDATION_MODEL, agent.foundationModel());
      jsonRequest.put(IDLE_SESSION_TTL_IN_SECONDS, agent.idleSessionTTLInSeconds());
      jsonRequest.put(INSTRUCTION, agent.instruction());
      jsonRequest.put(PROMPT_OVERRIDE_CONFIGURATION, agent.promptOverrideConfiguration());
      jsonRequest.put(UPDATED_AT, agent.updatedAt());
      return jsonRequest.toString();
    });
  }

  public static String createAgentAlias(String name, String agentId, AwsbedrockConfiguration configuration,
                                        AwsbedrockAgentsParameters awsBedrockParameter) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameter);
      AgentAlias agentAlias = createAgentAlias(name, agentId, bedrockAgent);
      JSONObject jsonObject = new JSONObject();
      jsonObject.put(AGENT_ALIAS_ID, agentAlias.agentAliasId());
      jsonObject.put(AGENT_ALIAS_NAME, agentAlias.agentAliasName());
      jsonObject.put(AGENT_ALIAS_ARN, agentAlias.agentAliasArn());
      jsonObject.put(CLIENT_TOKEN, agentAlias.clientToken());
      jsonObject.put(CREATED_AT, agentAlias.createdAt());
      jsonObject.put(UPDATED_AT, agentAlias.updatedAt());

      return jsonObject.toString();
    });
  }

  private static Role createAgentRole(String postfix, String RolePolicyName, AwsbedrockConfiguration configuration,
                                      AwsbedrockAgentsParameters awsBedrockParameters) {
    String roleName = AGENT_EXECUTION_ROLE_PREFIX + postfix;
    String modelArn = "arn:aws:bedrock:" + awsBedrockParameters.getRegion() + "::foundation-model/"
        + awsBedrockParameters.getModelName() + "*";
    String ROLE_POLICY_NAME = RolePolicyName;

    logger.info("Creating an execution role for the agent...");

    // Create an IAM client
    IamClient iamClient = BedrockClients.getIamClient(configuration, awsBedrockParameters);
    // Check if the role exists
    Role agentRole = null;
    try {
      GetRoleResponse getRoleResponse = iamClient.getRole(GetRoleRequest.builder().roleName(roleName).build());
      agentRole = getRoleResponse.role();
      logger.info("Role already exists: {}", agentRole.arn());
    } catch (NoSuchEntityException e) {
      // Role does not exist, create it
      try {
        CreateRoleResponse createRoleResponse = iamClient.createRole(CreateRoleRequest.builder()
            .roleName(roleName)
            .assumeRolePolicyDocument(
                                      "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"Service\": \"bedrock.amazonaws.com\"},\"Action\": \"sts:AssumeRole\"}]}")
            .build());

        logger.info("Model ARN: {}", modelArn);
        String policyDocument = "{\n"
            + "    \"Version\": \"2012-10-17\",\n"
            + "    \"Statement\": [\n"
            + "        {\n"
            + "            \"Effect\": \"Allow\",\n"
            + "            \"Action\": [\n"
            + "                \"bedrock:ListFoundationModels\",\n"
            + "                \"bedrock:GetFoundationModel\",\n"
            + "                \"bedrock:TagResource\",\n"
            + "                \"bedrock:UntagResource\",\n"
            + "                \"bedrock:ListTagsForResource\",\n"
            + "                \"bedrock:CreateAgent\",\n"
            + "                \"bedrock:UpdateAgent\",\n"
            + "                \"bedrock:GetAgent\",\n"
            + "                \"bedrock:ListAgents\",\n"
            + "                \"bedrock:DeleteAgent\",\n"
            + "                \"bedrock:CreateAgentActionGroup\",\n"
            + "                \"bedrock:UpdateAgentActionGroup\",\n"
            + "                \"bedrock:GetAgentActionGroup\",\n"
            + "                \"bedrock:ListAgentActionGroups\",\n"
            + "                \"bedrock:DeleteAgentActionGroup\",\n"
            + "                \"bedrock:GetAgentVersion\",\n"
            + "                \"bedrock:ListAgentVersions\",\n"
            + "                \"bedrock:DeleteAgentVersion\",\n"
            + "                \"bedrock:CreateAgentAlias\",\n"
            + "                \"bedrock:UpdateAgentAlias\",\n"
            + "                \"bedrock:GetAgentAlias\",\n"
            + "                \"bedrock:ListAgentAliases\",\n"
            + "                \"bedrock:DeleteAgentAlias\",\n"
            + "                \"bedrock:AssociateAgentKnowledgeBase\",\n"
            + "                \"bedrock:DisassociateAgentKnowledgeBase\",\n"
            + "                \"bedrock:GetKnowledgeBase\",\n"
            + "                \"bedrock:ListKnowledgeBases\",\n"
            + "                \"bedrock:PrepareAgent\",\n"
            + "                \"bedrock:InvokeAgent\",\n"
            + "                \"bedrock:InvokeModel\"\n"
            + "            ],\n"
            + "            \"Resource\": \"*\"\n"
            + "        }\n"
            + "    ]\n"
            + "}";
        logger.info("Policy Document: {}", policyDocument);
        iamClient.putRolePolicy(PutRolePolicyRequest.builder()
            .roleName(roleName)
            .policyName(ROLE_POLICY_NAME)
            .policyDocument(policyDocument)
            .build());

        agentRole = Role.builder()
            .roleName(roleName)
            .arn(createRoleResponse.role().arn())
            .build();
      } catch (IamException ex) {
        logger.error("Couldn't create role {}. Here's why: {}", roleName, ex.getMessage(), ex);
        throw ex;
      }
    } catch (IamException ex) {
      logger.error("Couldn't get role {}. Here's why: {}", roleName, ex.getMessage(), ex);
      throw ex;
    }
    return agentRole;
  }

  private static Agent createAgent(String name, String instruction, String modelId, Role agentRole,
                                   BedrockAgentClient bedrockAgentClient) {
    logger.info("Creating the agent...");

    // String instruction = "You are a friendly chat bot. You have access to a
    // function called that returns information about the current date and time.
    // When responding with date or time, please make sure to add the timezone
    // UTC.";
    CreateAgentResponse createAgentResponse = bedrockAgentClient.createAgent(CreateAgentRequest.builder()
        .agentName(name)
        .foundationModel(modelId)
        .instruction(instruction)
        .agentResourceRoleArn(agentRole.arn())
        .build());

    waitForAgentStatus(createAgentResponse.agent().agentId(), AgentStatus.NOT_PREPARED.toString(),
                       bedrockAgentClient);

    return createAgentResponse.agent();
  }

  private static void waitForAgentStatus(String agentId, String status, BedrockAgentClient bedrockAgentClient) {
    while (true) {
      GetAgentResponse response = bedrockAgentClient.getAgent(GetAgentRequest.builder()
          .agentId(agentId)
          .build());

      if (response.agent().agentStatus().toString().equals(status)) {

        break;
      }

      try {
        logger.info("Waiting for agent get prepared...");
        Thread.sleep(AGENT_STATUS_CHECK_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static PrepareAgentResponse prepareAgent(String agentId, BedrockAgentClient bedrockAgentClient) {
    logger.info("Preparing the agent...");

    // String agentId = agent.agentId();
    PrepareAgentResponse preparedAgentDetails = bedrockAgentClient.prepareAgent(PrepareAgentRequest.builder()
        .agentId(agentId)
        .build());
    waitForAgentStatus(agentId, "PREPARED", bedrockAgentClient);

    return preparedAgentDetails;
  }

  private static AgentAlias createAgentAlias(String alias, String agentId, BedrockAgentClient bedrockAgentClient) {
    logger.info("Creating an agent alias for agentId: {}", agentId);
    CreateAgentAliasRequest request = CreateAgentAliasRequest.builder()
        .agentId(agentId)
        .agentAliasName(alias)
        .build();

    CreateAgentAliasResponse response = bedrockAgentClient.createAgentAlias(request);

    return response.agentAlias();
  }

  public static String listAllAgentAliases(String agentId, AwsbedrockConfiguration configuration,
                                           AwsbedrockAgentsParameters awsBedrockParameters) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameters);
      List<AgentAliasSummary> agentAliasSummaries = listAgentAliases(agentId, bedrockAgent);

      JSONArray jsonArray = new JSONArray();
      for (AgentAliasSummary agentAliasSummary : agentAliasSummaries) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(AGENT_ALIAS_ID, agentAliasSummary.agentAliasId());
        jsonObject.put(AGENT_ALIAS_NAME, agentAliasSummary.agentAliasName());
        jsonObject.put(CREATED_AT, agentAliasSummary.createdAt());
        jsonObject.put(UPDATED_AT, agentAliasSummary.updatedAt());
        jsonArray.put(jsonObject);
      }

      JSONObject jsonObject = new JSONObject();
      jsonObject.put(AGENT_ALIAS_SUMMARIES, jsonArray);

      return jsonObject.toString();
    });
  }

  private static List<AgentAliasSummary> listAgentAliases(String agentId, BedrockAgentClient bedrockAgentClient) {
    // Build a ListAgentAliasesRequest instance with the agent ID
    ListAgentAliasesRequest listAgentAliasesRequest = ListAgentAliasesRequest.builder()
        .agentId(agentId)
        .build();

    // Call the listAgentAliases method of the BedrockAgentClient instance
    ListAgentAliasesResponse listAgentAliasesResponse = bedrockAgentClient
        .listAgentAliases(listAgentAliasesRequest);

    // Retrieve the list of agent alias summaries from the ListAgentAliasesResponse
    // instance
    List<AgentAliasSummary> agentAliasSummaries = listAgentAliasesResponse.agentAliasSummaries();

    return agentAliasSummaries;
  }

  public static String deleteAgentAliasesByAgentId(String agentId, String agentAliasName,
                                                   AwsbedrockConfiguration configuration,
                                                   AwsbedrockAgentsParameters awsBedrockParameters) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameters);
      DeleteAgentAliasResponse response = deleteAgentAliasByName(agentId, agentAliasName, bedrockAgent);

      JSONObject jsonObject = new JSONObject();
      jsonObject.put(AGENT_ID, response.agentId());
      jsonObject.put(AGENT_ALIAS_ID, response.agentAliasId());
      jsonObject.put(AGENT_ALIAS_STATUS, response.agentAliasStatus());
      jsonObject.put(AGENT_STATUS, response.agentAliasStatusAsString());

      return jsonObject.toString();
    });
  }

  private static DeleteAgentAliasResponse deleteAgentAliasByName(String agentId, String agentAliasName,
                                                                 BedrockAgentClient bedrockAgentClient) {
    DeleteAgentAliasResponse response = null;

    // Build a ListAgentAliasesRequest instance with the agent ID
    ListAgentAliasesRequest listAgentAliasesRequest = ListAgentAliasesRequest.builder()
        .agentId(agentId)
        .build();

    // Call the listAgentAliases method of the BedrockAgentClient instance
    ListAgentAliasesResponse listAgentAliasesResponse = bedrockAgentClient
        .listAgentAliases(listAgentAliasesRequest);

    // Retrieve the list of agent alias summaries from the ListAgentAliasesResponse
    // instance
    List<AgentAliasSummary> agentAliasSummaries = listAgentAliasesResponse.agentAliasSummaries();

    // Iterate through the list of agent alias summaries to find the one with the
    // specified name
    Optional<AgentAliasSummary> agentAliasSummary = agentAliasSummaries.stream()
        .filter(alias -> alias.agentAliasName().equals(agentAliasName))
        .findFirst();

    // If the agent alias was found, delete it
    if (agentAliasSummary.isPresent()) {
      String agentAliasId = agentAliasSummary.get().agentAliasId();

      // Build a DeleteAgentAliasRequest instance with the agent ID and agent alias ID
      DeleteAgentAliasRequest deleteAgentAliasRequest = DeleteAgentAliasRequest.builder()
          .agentId(agentId)
          .agentAliasId(agentAliasId)
          .build();

      // Call the deleteAgentAlias method of the BedrockAgentClient instance
      DeleteAgentAliasResponse deleteAgentAliasResponse = bedrockAgentClient
          .deleteAgentAlias(deleteAgentAliasRequest);

      logger.info("Agent alias with name " + agentAliasName + " deleted successfully.");
      response = deleteAgentAliasResponse;
    } else {
      logger.info("No agent alias with name " + agentAliasName + " found.");
    }
    return response;
  }

  public static String deleteAgentByAgentId(String agentId, AwsbedrockConfiguration configuration,
                                            AwsbedrockAgentsParameters awsBedrockParameters) {
    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentClient bedrockAgent = BedrockClients.getAgentClient(configuration, awsBedrockParameters);
      DeleteAgentResponse response = deleteAgentById(agentId, bedrockAgent);

      JSONObject jsonObject = new JSONObject();
      jsonObject.put(AGENT_ID, response.agentId());
      jsonObject.put(AGENT_STATUS, response.agentStatusAsString());

      return jsonObject.toString();
    });
  }

  private static DeleteAgentResponse deleteAgentById(String agentId, BedrockAgentClient bedrockAgentClient) {
    // Build a DeleteAgentRequest instance with the agent ID
    DeleteAgentRequest deleteAgentRequest = DeleteAgentRequest.builder()
        .agentId(agentId)
        .build();

    // Call the deleteAgent method of the BedrockAgentClient instance
    DeleteAgentResponse deleteAgentResponse = bedrockAgentClient.deleteAgent(deleteAgentRequest);

    // Print a message indicating that the agent was deleted successfully
    return deleteAgentResponse;
  }

  public static String chatWithAgent(String agentAlias, String agentId, String prompt, Boolean enableTrace,
                                     Boolean latencyOptimized,
                                     AwsbedrockConfiguration configuration,
                                     AwsbedrockAgentsSessionParameters awsbedrockAgentsSessionParameters,
                                     AwsbedrockAgentsFilteringParameters awsBedrockAgentsFilteringParameters,
                                     AwsbedrockAgentsMultipleFilteringParameters awsBedrockAgentsMultipleFilteringParameters,
                                     AwsbedrockAgentsParameters awsbedrockAgentsParameters) {

    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentRuntimeAsyncClient bedrockAgentRuntimeAsyncClient = BedrockClients.getAgentRuntimeAsyncClient(configuration,
                                                                                                                awsbedrockAgentsParameters);

      String sessionId = awsbedrockAgentsSessionParameters.getSessionId();
      String effectiveSessionId = (sessionId != null && !sessionId.isEmpty()) ? sessionId
          : UUID.randomUUID().toString();
      logger.info("Using sessionId: {}", effectiveSessionId);

      return invokeAgent(agentAlias, agentId, prompt, enableTrace, latencyOptimized, effectiveSessionId,
                         awsbedrockAgentsSessionParameters.getExcludePreviousThinkingSteps(),
                         awsbedrockAgentsSessionParameters.getPreviousConversationTurnsToInclude(),
                         buildKnowledgeBaseConfigs(awsBedrockAgentsFilteringParameters,
                                                   awsBedrockAgentsMultipleFilteringParameters),
                         bedrockAgentRuntimeAsyncClient)
          .thenApply(response -> {
            logger.debug(response);
            return response;
          }).join();
    });
  }

  private static CompletableFuture<String> invokeAgent(String agentAlias, String agentId, String prompt,
                                                       Boolean enableTrace, Boolean latencyOptimized, String sessionId,
                                                       Boolean excludePreviousThinkingSteps,
                                                       Integer previousConversationTurnsToInclude,
                                                       java.util.List<org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> knowledgeBaseConfigs,
                                                       BedrockAgentRuntimeAsyncClient bedrockAgentRuntimeAsyncClient) {

    long startTime = System.currentTimeMillis();

    InvokeAgentRequest request = InvokeAgentRequest.builder()
        .agentId(agentId)
        .agentAliasId(agentAlias)
        .sessionId(sessionId)
        .inputText(prompt)
        .enableTrace(enableTrace)
        .sessionState(buildSessionState(knowledgeBaseConfigs))
        .bedrockModelConfigurations(buildModelConfigurations(latencyOptimized))
        .promptCreationConfigurations(buildPromptConfigurations(excludePreviousThinkingSteps, previousConversationTurnsToInclude))
        .build();

    CompletableFuture<String> completionFuture = new CompletableFuture<>();

    // Thread-safe collection to store different chunks
    List<JSONObject> chunks = Collections.synchronizedList(new ArrayList<>());

    InvokeAgentResponseHandler.Visitor visitor = InvokeAgentResponseHandler.Visitor.builder()
        .onChunk(chunk -> {
          JSONObject chunkData = new JSONObject();
          chunkData.put(TYPE, CHUNK);
          chunkData.put(TIMESTAMP, Instant.now().toString());

          if (chunk.bytes() != null) {
            String text = new String(chunk.bytes().asByteArray(), StandardCharsets.UTF_8);
            chunkData.put(TEXT, text);
          }

          // Add attribution/citations if present
          if (chunk.attribution() != null && chunk.attribution().citations() != null) {
            JSONArray citationsArray = new JSONArray();
            chunk.attribution().citations().forEach(citation -> {
              JSONObject citationData = new JSONObject();

              if (citation.generatedResponsePart() != null
                  && citation.generatedResponsePart().textResponsePart() != null) {
                citationData.put(GENERATED_RESPONSE_PART,
                                 citation.generatedResponsePart().textResponsePart().text());
              }

              if (citation.retrievedReferences() != null) {
                JSONArray referencesArray = new JSONArray();
                citation.retrievedReferences().forEach(ref -> {
                  JSONObject refData = new JSONObject();
                  if (ref.content() != null && ref.content().text() != null) {
                    refData.put(CONTENT, ref.content().text());
                  }
                  if (ref.location() != null) {
                    refData.put(LOCATION, ref.location().toString());
                  }
                  if (ref.metadata() != null) {
                    JSONObject metadataObject = new JSONObject(ref.metadata());
                    refData.put(METADATA, metadataObject);
                  }
                  referencesArray.put(refData);
                });
                citationData.put(RETRIEVED_REFERENCES, referencesArray);
              }
              citationsArray.put(citationData);
            });
            chunkData.put(CITATIONS, citationsArray);
          }

          chunks.add(chunkData);
        })
        .build();

    InvokeAgentResponseHandler handler = InvokeAgentResponseHandler.builder()
        .subscriber(visitor)
        .build();

    CompletableFuture<Void> invocationFuture = bedrockAgentRuntimeAsyncClient.invokeAgent(request, handler);

    invocationFuture.whenComplete((result, throwable) -> {
      if (throwable != null) {
        completionFuture.completeExceptionally(throwable);
      } else {
        JSONObject finalResult = new JSONObject();
        finalResult.put(SESSION_ID, sessionId);
        finalResult.put(AGENT_ID, agentId);
        finalResult.put(AGENT_ALIAS, agentAlias);
        finalResult.put(PROMPT, prompt);
        finalResult.put(PROCESSED_AT, Instant.now().toString());
        finalResult.put(CHUNKS, new JSONArray(chunks));

        // Add summary statistics
        JSONObject summary = new JSONObject();
        summary.put(TOTAL_CHUNKS, chunks.size());

        // Concatenate all chunk text for full response
        StringBuilder fullText = new StringBuilder();
        chunks.forEach(chunk -> {
          if (chunk.has(TEXT)) {
            fullText.append(chunk.getString(TEXT));
          }
        });
        summary.put(FULL_RESPONSE, fullText.toString());

        long endTime = System.currentTimeMillis();
        summary.put("total_duration_ms", endTime - startTime);

        finalResult.put(SUMMARY, summary);

        String finalJson = finalResult.toString();
        completionFuture.complete(finalJson);
      }
    });

    return completionFuture;
  }

  public static InputStream chatWithAgentSSEStream(String agentAlias, String agentId, String prompt,
                                                   Boolean enableTrace,
                                                   Boolean latencyOptimized,
                                                   AwsbedrockConfiguration configuration,
                                                   AwsbedrockAgentsSessionParameters awsBedrockSessionParameters,
                                                   AwsbedrockAgentsFilteringParameters awsBedrockAgentsFilteringParameters,
                                                   AwsbedrockAgentsMultipleFilteringParameters awsBedrockAgentsMultipleFilteringParameters,
                                                   AwsbedrockAgentsParameters awsBedrockParameters) {

    return BedrockClientInvoker.executeWithErrorHandling(() -> {
      BedrockAgentRuntimeAsyncClient bedrockAgentRuntimeAsyncClient = BedrockClients.getAgentRuntimeAsyncClient(configuration,
                                                                                                                awsBedrockParameters);

      String sessionId = awsBedrockSessionParameters.getSessionId();
      String effectiveSessionId = (sessionId != null && !sessionId.isEmpty()) ? sessionId
          : UUID.randomUUID().toString();
      logger.info("Using sessionId: {}", effectiveSessionId);

      return invokeAgentSSEStream(agentAlias, agentId, prompt, enableTrace, latencyOptimized, effectiveSessionId,
                                  awsBedrockSessionParameters.getExcludePreviousThinkingSteps(),
                                  awsBedrockSessionParameters.getPreviousConversationTurnsToInclude(),
                                  buildKnowledgeBaseConfigs(awsBedrockAgentsFilteringParameters,
                                                            awsBedrockAgentsMultipleFilteringParameters),
                                  bedrockAgentRuntimeAsyncClient);
    });
  }

  /**
   * Invokes Bedrock Agent and returns streaming SSE response as InputStream.
   *
   * This method is designed to work with Mule's binary streaming.
   **/
  public static InputStream invokeAgentSSEStream(String agentAlias, String agentId, String prompt,
                                                 Boolean enableTrace, Boolean latencyOptimized, String sessionId,
                                                 Boolean excludePreviousThinkingSteps, Integer previousConversationTurnsToInclude,
                                                 java.util.List<org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> knowledgeBaseConfigs,
                                                 BedrockAgentRuntimeAsyncClient bedrockAgentRuntimeAsyncClient) {
    try {
      // Create piped streams for real-time streaming with larger buffer
      // Default 1024 bytes is too small and can cause blocking
      PipedOutputStream outputStream = new PipedOutputStream();
      PipedInputStream inputStream = new PipedInputStream(outputStream, 65536); // 64KB buffer

      // Start the streaming process asynchronously
      CompletableFuture.runAsync(() -> {
        try {
          streamBedrockResponse(agentAlias, agentId, prompt, enableTrace, latencyOptimized, sessionId,
                                excludePreviousThinkingSteps, previousConversationTurnsToInclude,
                                knowledgeBaseConfigs, bedrockAgentRuntimeAsyncClient, outputStream);
        } catch (Exception e) {
          // CRITICAL: Log the primary error FIRST with full stack trace
          logger.error("PRIMARY STREAMING ERROR - Agent: {}, Session: {}, Error: {}",
                       agentId, sessionId, e.getMessage(), e);
          try {
            // Send error as SSE event
            String errorEvent = formatSSEEvent("error", createErrorJson(e).toString());
            outputStream.write(errorEvent.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
            outputStream.close();
            logger.error("Error event sent: {}", errorEvent);
          } catch (IOException ioException) {
            // Secondary error - pipe likely closed by consumer
            logger.error("Could not write error to stream (consumer disconnected): {}", ioException.getMessage());
          }
        }
      });

      return inputStream;

    } catch (IOException e) {
      // Return error as immediate SSE event
      String errorEvent = formatSSEEvent("error", createErrorJson(e).toString());
      logger.error("Failed to create piped streams for SSE streaming: {}", errorEvent);
      return new ByteArrayInputStream(errorEvent.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void streamBedrockResponse(String agentAlias, String agentId, String prompt, Boolean enableTrace,
                                            Boolean latencyOptimized, String sessionId,
                                            Boolean excludePreviousThinkingSteps, Integer previousConversationTurnsToInclude,
                                            java.util.List<org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> knowledgeBaseConfigs,
                                            BedrockAgentRuntimeAsyncClient client,
                                            PipedOutputStream outputStream)
      throws IOException {
    long startTime = System.currentTimeMillis();

    // Send initial event
    JSONObject startEvent = createSessionStartJson(agentAlias, agentId, prompt, sessionId, Instant.now().toString());
    String sseStart = formatSSEEvent("session-start", startEvent.toString());
    outputStream.write(sseStart.getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    logger.info(sseStart);

    InvokeAgentRequest request = InvokeAgentRequest.builder()
        .agentId(agentId)
        .agentAliasId(agentAlias)
        .sessionId(sessionId)
        .inputText(prompt)
        .streamingConfigurations(builder -> builder.streamFinalResponse(true))
        .enableTrace(enableTrace)
        .sessionState(buildSessionState(knowledgeBaseConfigs))
        .bedrockModelConfigurations(buildModelConfigurations(latencyOptimized))
        .promptCreationConfigurations(buildPromptConfigurations(excludePreviousThinkingSteps, previousConversationTurnsToInclude))
        .build();

    InvokeAgentResponseHandler.Visitor visitor = InvokeAgentResponseHandler.Visitor.builder()
        .onChunk(chunk -> {
          try {
            JSONObject chunkData = createChunkJson(chunk);
            String sseEvent = formatSSEEvent("chunk", chunkData.toString());
            outputStream.write(sseEvent.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
            logger.debug(sseEvent);
          } catch (IOException e) {
            // Log chunk write error with details
            logger.error("Error writing chunk to stream: {}", e.getMessage(), e);
            try {
              String errorEvent = formatSSEEvent("chunk-error", createErrorJson(e).toString());
              outputStream.write(errorEvent.getBytes(StandardCharsets.UTF_8));
              outputStream.flush();
              logger.error("Chunk error event sent: {}", errorEvent);
            } catch (IOException ioException) {
              // Can't write error, stream is likely closed by consumer
              logger.error("Could not write chunk error to stream (consumer disconnected): {}", ioException.getMessage());
            }
          }
        }).build();

    InvokeAgentResponseHandler handler = InvokeAgentResponseHandler.builder()
        .subscriber(visitor)
        .onError(throwable -> {
          // CRITICAL: Log the streaming error FIRST with full details
          logger.error("STREAMING ERROR from Bedrock Agent - Agent: {}, Session: {}, Error: {}",
                       agentId, sessionId, throwable.getMessage(), throwable);
          try {
            // Send error event if the async streaming operation fails
            String errorEvent = formatSSEEvent("streaming-error", createErrorJson(throwable).toString());
            outputStream.write(errorEvent.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
            logger.error("Streaming error event sent: {}", errorEvent);
          } catch (IOException ioException) {
            // Can't write error, stream is likely closed by consumer
            logger.error("Could not write streaming error to stream (consumer disconnected): {}", ioException.getMessage());
          } finally {
            try {
              outputStream.close();
            } catch (IOException ioException) {
              logger.error("Could not close output stream (already closed): {}", ioException.getMessage());
            }
          }
        })
        .onComplete(() -> {
          try {
            // Send completion event
            long endTime = System.currentTimeMillis();
            JSONObject completionData = createCompletionJson(sessionId, agentId, agentAlias, endTime - startTime);
            String completionEvent = formatSSEEvent("session-complete", completionData.toString());
            outputStream.write(completionEvent.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
            logger.info(completionEvent);
          } catch (IOException e) {
            // Log completion write error with details
            logger.error("Error writing completion event to stream: {}", e.getMessage(), e);
            try {
              String errorEvent = formatSSEEvent("completion-error", createErrorJson(e).toString());
              outputStream.write(errorEvent.getBytes(StandardCharsets.UTF_8));
              outputStream.flush();
              logger.error("Completion error event sent: {}", errorEvent);
            } catch (IOException ioException) {
              // Can't write error, stream is likely closed by consumer
              logger.error("Could not write completion error to stream (consumer disconnected): {}", ioException.getMessage());
            }
          } finally {
            try {
              outputStream.close();
            } catch (IOException ioException) {
              // Log error but can't do much more
              logger.error("Could not close output stream (already closed): {}", ioException.getMessage());
            }
          }
        })
        .build();

    // Start async streaming - chunks will be written as they arrive via the onChunk callback
    // Do NOT call .get() here as it would block and defeat the purpose of real-time streaming
    // Errors are handled by the onError callback in the handler
    client.invokeAgent(request, handler);
  }

  private static JSONObject createChunkJson(PayloadPart chunk) {
    JSONObject chunkData = new JSONObject();
    chunkData.put(TYPE, CHUNK);
    chunkData.put(TIMESTAMP, Instant.now().toString());

    try {
      if (chunk.bytes() != null) {
        String text = new String(chunk.bytes().asByteArray(), StandardCharsets.UTF_8);
        chunkData.put(TEXT, text);
      }

    } catch (Exception e) {
      chunkData.put("error", "Error processing chunk: " + e.getMessage());
    }

    return chunkData;
  }

  private static JSONObject createSessionStartJson(String agentAlias, String agentId, String prompt,
                                                   String sessionId, String timestamp) {
    JSONObject startData = new JSONObject();
    startData.put(SESSION_ID, sessionId);
    startData.put(AGENT_ID, agentId);
    startData.put(AGENT_ALIAS, agentAlias);
    startData.put(PROMPT, prompt);
    startData.put(PROCESSED_AT, timestamp);
    startData.put("status", "started");
    return startData;
  }

  private static JSONObject createCompletionJson(String sessionId, String agentId, String agentAlias, long duration) {
    JSONObject completionData = new JSONObject();
    completionData.put(SESSION_ID, sessionId);
    completionData.put(AGENT_ID, agentId);
    completionData.put(AGENT_ALIAS, agentAlias);
    completionData.put("status", "completed");
    completionData.put("total_duration_ms", duration);
    completionData.put(TIMESTAMP, Instant.now().toString());
    return completionData;
  }

  private static String formatSSEEvent(String eventType, String data) {
    int eventId = eventCounter.incrementAndGet();
    return String.format("id: %d%nevent: %s%ndata: %s%n%n", eventId, eventType, data);
  }

  private static JSONObject createErrorJson(Throwable error) {
    JSONObject errorData = new JSONObject();
    errorData.put("error", error.getMessage());
    errorData.put("type", error.getClass().getSimpleName());
    errorData.put(TIMESTAMP, Instant.now().toString());
    return errorData;
  }

  private static java.util.function.Consumer<SessionState.Builder> buildSessionState(
                                                                                     java.util.List<org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> kbConfigs) {
    return sessionStateBuilder -> {
      if (kbConfigs == null || kbConfigs.isEmpty()) {
        return;
      }

      List<KnowledgeBaseConfiguration> sdkKbConfigs = kbConfigs.stream().map(kb -> {
        KnowledgeBaseVectorSearchConfiguration vectorCfg = buildVectorSearchConfiguration(kb.getNumberOfResults(),
                                                                                          kb.getOverrideSearchType(),
                                                                                          kb.getRetrievalMetadataFilterType(),
                                                                                          kb.getMetadataFilters());
        KnowledgeBaseRetrievalConfiguration retrievalCfg = KnowledgeBaseRetrievalConfiguration.builder()
            .vectorSearchConfiguration(vectorCfg)
            .build();
        return KnowledgeBaseConfiguration.builder().knowledgeBaseId(kb.getKnowledgeBaseId())
            .retrievalConfiguration(retrievalCfg)
            .build();
      }).collect(Collectors.toList());

      if (!sdkKbConfigs.isEmpty()) {
        sessionStateBuilder.knowledgeBaseConfigurations(sdkKbConfigs);
      }
    };
  }

  private static KnowledgeBaseVectorSearchConfiguration buildVectorSearchConfiguration(Integer numberOfResults,
                                                                                       AwsbedrockAgentsFilteringParameters.SearchType overrideSearchType,
                                                                                       AwsbedrockAgentsFilteringParameters.RetrievalMetadataFilterType filterType,
                                                                                       Map<String, String> metadataFilters) {

    if (metadataFilters == null || metadataFilters.isEmpty()) {
      return null;
    }

    // Filter out null and empty values
    Map<String, String> nonEmptyFilters = metadataFilters.entrySet().stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (nonEmptyFilters.isEmpty()) {
      return null;
    }

    // apply numberOfResults only if not null and greater than 0
    Consumer<KnowledgeBaseVectorSearchConfiguration.Builder> applyOptionalNumberOfResults =
        b -> {
          if (numberOfResults != null && numberOfResults.intValue() > 0)
            b.numberOfResults(numberOfResults);
        };

    // apply overrideSearchType only if not null
    Consumer<KnowledgeBaseVectorSearchConfiguration.Builder> applyOptionalOverrideSearchType =
        b -> {
          if (overrideSearchType != null)
            // Use the conversion function to pass the SDK's SearchType
            b.overrideSearchType(convertToSdkSearchType(overrideSearchType));
        };

    if (nonEmptyFilters.size() > 1) {
      List<RetrievalFilter> retrievalFilters = nonEmptyFilters.entrySet().stream()
          .map(entry -> RetrievalFilter.builder()
              .equalsValue(FilterAttribute.builder()
                  .key(entry.getKey())
                  .value(Document.fromString(entry.getValue()))
                  .build())
              .build())
          .collect(Collectors.toList());

      RetrievalFilter compositeFilter = RetrievalFilter.builder()
          .applyMutation(builder -> {
            if (filterType == AwsbedrockAgentsFilteringParameters.RetrievalMetadataFilterType.AND_ALL) {
              builder.andAll(retrievalFilters);
            } else if (filterType == AwsbedrockAgentsFilteringParameters.RetrievalMetadataFilterType.OR_ALL) {
              builder.orAll(retrievalFilters);
            }
          })
          .build();

      return KnowledgeBaseVectorSearchConfiguration.builder()
          .filter(compositeFilter)
          .applyMutation(applyOptionalNumberOfResults)
          .applyMutation(applyOptionalOverrideSearchType)
          .build();
    } else {
      String key = nonEmptyFilters.entrySet().iterator().next().getKey();
      return KnowledgeBaseVectorSearchConfiguration.builder()
          .filter(retrievalFilter -> retrievalFilter.equalsValue(FilterAttribute.builder()
              .key(key)
              .value(Document.fromString(nonEmptyFilters.get(key)))
              .build()).build())
          .applyMutation(applyOptionalNumberOfResults)
          .applyMutation(applyOptionalOverrideSearchType)
          .build();
    }

  }

  private static java.util.List<org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig> buildKnowledgeBaseConfigs(
                                                                                                                                                                org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters legacyParams,
                                                                                                                                                                org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsMultipleFilteringParameters multipleParams) {
    if (multipleParams != null && multipleParams.getKnowledgeBases() != null && !multipleParams.getKnowledgeBases().isEmpty()) {
      // If legacy single-KB fields are also provided, warn that they will be ignored
      if (legacyParams != null && (legacyParams.getKnowledgeBaseId() != null
          || legacyParams.getNumberOfResults() != null
          || legacyParams.getOverrideSearchType() != null
          || legacyParams.getRetrievalMetadataFilterType() != null
          || (legacyParams.getMetadataFilters() != null && !legacyParams.getMetadataFilters().isEmpty()))) {
        logger
            .warn("Multiple knowledge bases provided; legacy single-KB fields will be ignored.");
      }
      return multipleParams.getKnowledgeBases();
    }

    if (legacyParams == null) {
      return null;
    }

    // Fallback: if legacy single KB id is provided, map it to a per-KB config
    String id = legacyParams.getKnowledgeBaseId();
    if (id == null || id.isEmpty()) {
      return null;
    }

    return Collections
        .singletonList(new org.mule.extension.mulechain.internal.agents.AwsbedrockAgentsFilteringParameters.KnowledgeBaseConfig(
                                                                                                                                id,
                                                                                                                                legacyParams
                                                                                                                                    .getNumberOfResults(),
                                                                                                                                legacyParams
                                                                                                                                    .getOverrideSearchType(),
                                                                                                                                legacyParams
                                                                                                                                    .getRetrievalMetadataFilterType(),
                                                                                                                                legacyParams
                                                                                                                                    .getMetadataFilters()));


  }

  private static software.amazon.awssdk.services.bedrockagentruntime.model.SearchType convertToSdkSearchType(AwsbedrockAgentsFilteringParameters.SearchType connectorSearchType) {
    if (connectorSearchType == null) {
      return null;
    }
    switch (connectorSearchType) {
      case HYBRID:
        return software.amazon.awssdk.services.bedrockagentruntime.model.SearchType.HYBRID;
      case SEMANTIC:
        return software.amazon.awssdk.services.bedrockagentruntime.model.SearchType.SEMANTIC;
      default:
        // Fail fast: return the error back by throwing an exception so callers can handle it
        throw new IllegalArgumentException("Unsupported SearchType: " + connectorSearchType);
    }
  }

  private static Consumer<BedrockModelConfigurations.Builder> buildModelConfigurations(
                                                                                       Boolean isLatencyOptimized) {
    return builder -> builder.performanceConfig(
                                                performanceConfig -> performanceConfig.latency(
                                                                                               isLatencyOptimized ? "optimized"
                                                                                                   : "standard"));
  }

  private static Consumer<PromptCreationConfigurations.Builder> buildPromptConfigurations(
                                                                                          Boolean excludePreviousThinkingSteps,
                                                                                          Integer previousConversationTurnsToInclude) {
    return builder -> builder
        .excludePreviousThinkingSteps(excludePreviousThinkingSteps)
        .previousConversationTurnsToInclude(previousConversationTurnsToInclude);
  }

}
