import ballerina/test;
import ballerina/http;
import ballerinax/java.jdbc;
import ballerina/sql;

@test:BeforeSuite
function beforeSuite() {
    coreClient = test:mock(http:Client);
    jdbcClient = test:mock(jdbc:Client);
}

@test:Config
function testGetNumberOfUnacknowledgedInteractions(){
    InteractionEntity mockEntity = test:mock(InteractionEntity);
    test:prepare(mockEntity).when("isSentByOperator").thenReturn(false);
    InteractionLogAggregateRoot log = new InteractionLogAggregateRoot("bunlo9vk5f","test","",[mockEntity]);
    test:assertEquals(log.getNumberOfUnacknowledgedInteractions(),1);
}


@test:Config {}
function testGetCustomer() {
    CustomerId id = new CustomerId("bunlo9vk5f");

    CustomerDto expected = {
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
    CustomersDto mockDto = {customers: [expected]};
    
    test:prepare(coreClient).when("get").withArguments("/customers/bunlo9vk5f").thenReturn(mockDto);
    test:assertEquals(getCustomer(id),expected);
}

@test:Config
function testGetInteractionLog(){
    InteractionLogAggregateRoot expected = new InteractionLogAggregateRoot("bunlo9vk5f","test","",[]);

    stream<InteractionEntityRecord, sql:Error?> mockEntities = new();

    InteractionLogAggregateRootRecord mockLog = {
        "customer_Id": "bunlo9vk5f",
        "username": "test",
        "last_acknowledged_interaction_id": "",
        "interactions": []
    };

    test:prepare(jdbcClient).when("queryRow").thenReturn(mockLog);
    test:prepare(jdbcClient).when("query").thenReturn(mockEntities);
    InteractionLogAggregateRoot|error result= getInteractionLog("bunlo9vk5f");
    if(result is InteractionLogAggregateRoot){
        test:assertExactEquals(result.getCustomerId(),expected.getCustomerId());
        test:assertExactEquals(result.getUsername(),expected.getUsername());
        test:assertExactEquals(result.getLastAcknowledgedInteractionId(),expected.getLastAcknowledgedInteractionId());
        test:assertEquals(result.getInteractions().toString(),expected.getInteractions().toString());
    }
    
}