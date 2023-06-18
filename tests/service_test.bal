import ballerina/test;
import ballerina/http;
import ballerinax/java.jdbc;
import ballerina/websocket;
import ballerina/sql;

@test:BeforeSuite
function beforeSuiteRest() {
    coreClient = test:mock(http:Client);
    jdbcClient = test:mock(jdbc:Client);
}

http:Client testClient = check new ("http://localhost:8100");

@test:Config {}
function testGetCustomerRest() returns error?{
    CustomerDto customer = {
        "customerId": "bunlo9vk5f",
        "firstname": "Ado",
        "lastname": "Kinnett",
        "birthday": "1975-06-13T23:00:00.000+00:00",
        "streetAddress": "2 Autumn Leaf Lane",
        "postalCode": "6500",
        "city": "Bellinzona",
        "email": "akinnetta@example.com",
        "phoneNumber": "055 222 4111",
        "moveHistory": []
    };

    CustomersDto mockDto = {customers: [customer]};
    
    json expected = customer.toJson();
    
    test:prepare(coreClient).when("get").withArguments("/customers/bunlo9vk5f").thenReturn(mockDto);
    http:Response result = check testClient->get("/customers/bunlo9vk5f");
    test:assertEquals(result.statusCode,http:STATUS_OK);
    test:assertEquals(result.getTextPayload(),expected.toJsonString());
}

@test:Config
function testGetInteractionLogRest() returns error?{
    stream<InteractionEntityRecord, sql:Error?> mockEntities = new();

    InteractionLogAggregateRootRecord mockLog = {
        "customer_Id": "bunlo9vk5f",
        "username": "test",
        "last_acknowledged_interaction_id": "",
        "interactions": []
    };

    json expected = mockLog.toJson();

    test:prepare(jdbcClient).when("queryRow").thenReturn(mockLog);
    test:prepare(jdbcClient).when("query").thenReturn(mockEntities);
    http:Response result = check testClient->get("/interaction-logs/bunlo9vk5f");
    test:assertEquals(result.statusCode,http:STATUS_OK);
    test:assertEquals(result.getTextPayload(),expected.toJsonString());
}

@test:Config
public function testWebsocket() returns error? {
    websocket:Client wsTestClient = check new ("ws://localhost:8100/ws");
    var resp = wsTestClient.getHttpResponse();
    if resp is http:Response {
        test:assertEquals(resp.statusCode,http:STATUS_SWITCHING_PROTOCOLS);
    }
}