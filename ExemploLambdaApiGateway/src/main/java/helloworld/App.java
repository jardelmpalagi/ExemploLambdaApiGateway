package helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.GetItemExpressionSpec;
import com.amazonaws.services.dynamodbv2.xspec.ScanExpressionSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.simpleemailv2.model.DataFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    Logger LOG = LoggerFactory.getLogger(App.class);



    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("PrimeiraTabela");

        try {

            String output = "{}";

            if ("GET".equals(input.getHttpMethod())) {
                System.out.println(input.getPath());

                if (input.getPath().contains("categoria")) {
                    return fluxoGetByCategoriaWithScan(input.getPathParameters(), table);
                }
                System.out.println("PRINT GET");
                LOG.info("LOG GET");
                return fluxoGet(input.getPathParameters(), table);
            } else if ("POST".equals(input.getHttpMethod())) {
                System.out.println("PRINT POST");
                LOG.info("LOG POST");

                return fluxoPost(table, input.getBody());
            } else if ("DELETE".equals(input.getHttpMethod())) {
                System.out.println("PRINT DELETE");
                LOG.info("LOG DELETE");

                return delete(input.getPathParameters(), table);
            } else if ("PUT".equals(input.getHttpMethod())) {
                System.out.println("PRINT PUT");
                LOG.info("LOG PUT");

                return fluxoPut(input.getPathParameters(), table, input.getBody());
            }

            return buildResponse(500, "{}");

        } catch (Exception e) {
            e.printStackTrace();
            return buildResponse(500, "{}");
        }
    }

    private APIGatewayProxyResponseEvent buildResponse(Integer code, String body) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");

        return new APIGatewayProxyResponseEvent().withHeaders(headers).withStatusCode(code).withBody(body);
    }

    private String getPageContents(String address) throws IOException{
        URL url = new URL(address);
        try(BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    private APIGatewayProxyResponseEvent fluxoGetByCategoriaWithScan(Map<String, String> pathParams, Table table) {

        try {
            ScanSpec scanSpec = new ScanSpec().withFilterExpression("categoria=:categoria and contains(nome, :nome)")
                    .withValueMap(new ValueMap().withNumber(":categoria", 1).with(":nome", "Ja"));

            List<Map> items = new ArrayList<>();
            table.scan(scanSpec).forEach(i -> items.add(i.asMap()));

            ObjectMapper mapper = new ObjectMapper();

            return buildResponse(200, mapper.writeValueAsString(items));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private APIGatewayProxyResponseEvent fluxoGetByCategoria(Map<String, String> pathParams, Table table) {

        try {
            Index index = table.getIndex("categoria-index");

            QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("categoria=:categoria")
                    .withFilterExpression("nome=:nome")
                    .withValueMap(new ValueMap().withNumber(":categoria", 1).withString(":nome", "Jota"))
                    .withProjectionExpression("nome");

            List<Map> items = new ArrayList<>();
            index.query(querySpec).forEach(i -> items.add(i.asMap()));

            ObjectMapper mapper = new ObjectMapper();

            return buildResponse(200, mapper.writeValueAsString(items));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private APIGatewayProxyResponseEvent fluxoGet(Map<String, String> pathParams, Table table) {
        String id = pathParams.get("id");

        KeyAttribute key = new KeyAttribute("id", id);
        Item item = table.getItem(key);

        if (item == null) {
            return buildResponse(404, "{}");
        }

        return buildResponse(200, item.toJSON());
    }

    private APIGatewayProxyResponseEvent fluxoPost(Table table, String body) {

        Item item = Item.fromJSON(body).with("id", UUID.randomUUID().toString());

        PutItemOutcome outcome = table.putItem(item);

        System.out.println(outcome.getPutItemResult());

        return buildResponse(201, item.toJSON());
    }

    private APIGatewayProxyResponseEvent fluxoPut(Map<String, String> pathParams, Table table, String body) {
        String id = pathParams.get("id");

        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(body, LinkedHashMap.class);
            map.put("id", id);
            Item item = Item.fromMap(map);

            table.putItem(item);
            return buildResponse(200, item.toJSON());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return buildResponse(500, "{}");
    }

    private APIGatewayProxyResponseEvent fluxoPut1(Map<String, String> pathParams, Table table, String body) {
        String id = pathParams.get("id");

        PrimaryKey key = new PrimaryKey("id", id);

        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(body, LinkedHashMap.class);
            Item item = Item.fromMap(map);

            AttributeUpdate[] attributes = map.entrySet().stream()
                    .map(e -> new AttributeUpdate(e.getKey()).put(e.getValue())).toArray(AttributeUpdate[]::new);

            table.updateItem(key, attributes);
            return buildResponse(200, item.toJSON());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return buildResponse(500, "{}");
    }

    private APIGatewayProxyResponseEvent delete(Map<String, String> pathParams, Table table) {
        String id = pathParams.get("id");

        KeyAttribute key = new KeyAttribute("id", id);

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(key);

        Item item = table.deleteItem(deleteItemSpec).getItem();

        return buildResponse(200, "{}");
    }
}