package com.lab;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.CosmosDBOutput;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusQueueTrigger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Azure Functions with ServiceBusQueue.
 */
public class ServiceBusToCosmosFunction {

  @FunctionName("ServiceBusToCosmosFunction")
  public void run(
      @ServiceBusQueueTrigger(
          name = "message",
          queueName = "QueueDemo01",
          connection = "ServiceBusConnectionString"
      )
      String message,
      @CosmosDBOutput(
          name = "outputItem",
          databaseName = "AccountManagementDB",
          collectionName = "AccountsContainer",
          connectionStringSetting = "CosmosDBConnectionString"
      )
      OutputBinding<Account> outputItem,
      final ExecutionContext context
  ) {
    context.getLogger().info("Java Service Bus trigger procesó mensaje: " + message);

    String[] extractedData = extractAccountAndStatus(message);
    if (extractedData != null) {
      String accountId = extractedData[0]+"0000";
      String status = extractedData[1];
      Account account = new Account(accountId, status);
      outputItem.setValue(account);

      context.getLogger().info("Objeto enviado a Cosmos DB con ID: " + accountId);
    }
  }

  private String[] extractAccountAndStatus(String message) {
    Pattern pattern = Pattern.compile("(\\d+):([A-Z]+)");
    Matcher matcher = pattern.matcher(message);

    if (matcher.matches()) {
      String accountId = matcher.group(1);
      String status = matcher.group(2);
      return new String[]{accountId, status};
    }
    return null;
  }

}
