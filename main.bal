import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/websocket;
import ballerina/sql;
import ballerina/uuid;
import ballerinax/java.jdbc;

public class CustomerId {
    private string? id;

    public isolated function init(string? id){
        self.id = id;
    }

    public isolated function getId() returns string? {
        return self.id;
    }

    public isolated function setId(string? id) {
        self.id = id;
    }
};

public class InteractionEntity {
    private string? id;
    private string? date;
    private string? content;
    private boolean? sentByOperator;

    public isolated function init(string? id, string? date, string? content, boolean? sentByOperator){
        self.id = id;
        self.date = date;
        self.content = content;
        self.sentByOperator = sentByOperator;
    }

    public isolated function getId() returns string? {
        return self.id;
    }

    public isolated function setId(string? id) {
        self.id = id;
    }

    public isolated function getDate() returns string? {
        return self.date;
    }

    public isolated function setDate(string? date) {
        self.date = date;
    }

    public isolated function getContent() returns string? {
        return self.content;
    }

    public isolated function setContent(string? content) {
        self.content = content;
    }

    public isolated function isSentByOperator() returns boolean? {
        return self.sentByOperator;
    }

    public isolated function setSentByOperator(boolean? sentByOperator) {
        self.sentByOperator = sentByOperator;
    }

    public isolated function toJson() returns json {
        return {"id": self.id,"date": self.date.toJson(), "content": self.content,"sentByOperator": self.sentByOperator};
    }
};

public type InteractionEntityRecord record {|
    string id?;
    string date?;
    string content?;
    boolean sent_by_operator?;
|};

public class InteractionLogAggregateRoot {
    private string? customerId;
	private string? username;
	private string? lastAcknowledgedInteractionId;
    private InteractionEntity[]? interactions;

    public isolated function init (string? customerId,string? username, string? lastAcknowledgedInteractionId, InteractionEntity[]? interactions){
        self.customerId = customerId;
        self.username = username;
        self.lastAcknowledgedInteractionId = lastAcknowledgedInteractionId;
        self.interactions = interactions;
    }

    public isolated function getCustomerId() returns string? {
        return self.customerId;
    }

    public isolated function getUsername() returns string? {
        return self.username;
    }

    public isolated function getLastAcknowledgedInteractionId() returns string? {
        return self.lastAcknowledgedInteractionId;
    }

    public isolated function setLastAcknowledgedInteractionId(string? lastAcknowledgedInteractionId){
        self.lastAcknowledgedInteractionId = lastAcknowledgedInteractionId;
    }

    public isolated function getInteractions() returns InteractionEntity[]? {
        return self.interactions;
    }

    public isolated function toJson() returns json{
        json[] interactions = [];
        any tmp = self.getInteractions();
        if(tmp is InteractionEntity[]){
            foreach InteractionEntity item in tmp {
                interactions.push(item.toJson());
            }
        }
        return {"customerId":self.customerId,"username":self.username,"lastAcknowledgedInteractionId":self.lastAcknowledgedInteractionId,"interactions":interactions};
    }

    public isolated function getNumberOfUnacknowledgedInteractions() returns int{
        any tmp = self.getInteractions();
        if(tmp is InteractionEntity[]){
            InteractionEntity[] interactions = tmp.filter(i => i.isSentByOperator() == false);
            if(self.lastAcknowledgedInteractionId == ""){
                return interactions.length();
            }else{
                int count = 0;
                InteractionEntity[] interactionsTmp = interactions.reverse();
                foreach InteractionEntity item in interactionsTmp {
                    if(self.lastAcknowledgedInteractionId == item.getId()){
                        break;
                    }else{
                        count +=1;
                    }
                }
                return count;
            }
        }
        return -1;
    }
};

public type InteractionLogAggregateRootRecord record {|
    string customer_Id;
    string username;
    string? last_acknowledged_interaction_id;
    InteractionEntityRecord[] interactions;
|};

public class Notification {
    private final string? customerId;
    private final string? username;
    private final int? count;

    public isolated function init(string? customerId, string? username, int? count){
        self.customerId = customerId;
        self.username = username;
        self.count = count;
    }

    public isolated function getCustomerId() returns string? {
        return self.customerId;
    }

    public isolated function getUsername() returns string? {
        return self.username;
    }

    public isolated function getCount() returns int? {
        return self.count;
    }

    public function toJson() returns json {
        return {"customerId": self.customerId,"username": self.username, "count": self.count};
    }
}

public type AddressDto record {|
    string streetAddress;
    string postalCode;
    string city;
|};

public type CustomerDto record {
    string customerId;
    string firstname;
    string lastname;
    string birthday;
    string streetAddress;
	string postalCode;
	string city;
    string email;
    string phoneNumber;
    AddressDto[] moveHistory;
};

public type CustomerProfileDto record {|
    string firstname;
    string lastname;
    string birthday;
    string streetAddress;
	string postalCode;
	string city;
    string email;
    string phoneNumber;
    AddressDto[] moveHistory;
|};

public type CustomersDto record {
    CustomerDto[] customers;
};

public type InteractionAcknowledgementDto record {|
    string lastAcknowledgedInteractionId;
|};

public type MessageDto record {|
    string id?;
    string date?;
    string customerId;
    string username;
    string content;
    boolean sentByOperator;
|};

public type NotificationDto record {|
    string customerId?;
    string username?;
    int count?;
|};

public type PaginatedCustomerResponseDto record {
    string filter;
    int 'limit;
    int offset;
    int size;
    CustomerDto[] customers;
};

public type CustomerCoreNotAvailableException record {|
    *http:BadGateway;
|};

public type CustomerNotFoundException record {|
    *http:NotFound;
|};

public type InteractionLogNotFoundException record {|
    *http:NotFound;
|};

configurable string apiKeyValue = ?;
configurable string baseUrl = ?;
configurable string datasource = ?;
configurable string username = ?;
configurable string password = ?;
configurable string ddl_auto = ?;

http:Client coreClient = check new ("http://"+baseUrl,
    auth = {
        token: apiKeyValue
    }
);

final string errorMessage = "Failed to connect to Customer Core.";

jdbc:Client jdbcClient = check new (
    url =  datasource, 
    user = username, password = password,
    options = {
        properties: {"connectionTimeout": "300000"}
    },
    connectionPool = {
        maxOpenConnections: 10000
    });

public function getCustomer(CustomerId customerId) returns (()|CustomerDto|error<CustomerCoreNotAvailableException>) {
    string? customerIdString = customerId.getId();
    if(customerIdString is string){
        CustomersDto|error response = coreClient->get("/customers/" + customerIdString);
        if(response is CustomersDto){
            if(response.customers.length() == 0){
                return ();
            }else{
                return response.customers[0];
            }
        }else{
            log:printInfo(errorMessage,response);
            error<CustomerCoreNotAvailableException> err = error(errorMessage);
            return err;
        }
    }else{
        log:printInfo(errorMessage,customerIdString);
        error<CustomerCoreNotAvailableException> err = error(errorMessage);
        return err;
    }
}

public function getCustomers(string filter = "", int 'limit = 10, int offset = 0) returns (PaginatedCustomerResponseDto|error<CustomerCoreNotAvailableException>) {
    PaginatedCustomerResponseDto|error response = coreClient->get("/customers?filter=" + filter + "&limit=" + 'limit.toString() + "&offset=" + offset.toString());

    if(response is PaginatedCustomerResponseDto){
        return response;
    }else{
        log:printInfo(errorMessage,response);
        error<CustomerCoreNotAvailableException> err = error(errorMessage);
        return err;
    }
}

public function updateCustomer(CustomerId customerId, CustomerProfileDto customerProfile) returns (CustomerDto|error<CustomerCoreNotAvailableException> ){
    string? customerIdString = customerId.getId();
    if(customerIdString is string){
        http:Request request = new;
        request.setJsonPayload(customerProfile.toJson());

        CustomerDto|error response = coreClient->put("/customers/" + customerIdString,request);

        if(response is CustomerDto){
            return response;
        }else{
            log:printInfo(errorMessage,response);
            error<CustomerCoreNotAvailableException> err = error(errorMessage);
            return err;
        }
    }else{
        log:printInfo(errorMessage,customerIdString);
        error<CustomerCoreNotAvailableException> err = error(errorMessage);
        return err;
    }
}

public function getInteractionLogs() returns InteractionLogAggregateRoot[]|error{
    stream<InteractionLogAggregateRootRecord, error?> entries = jdbcClient->query(`SELECT CUSTOMER_ID, USERNAME,LAST_ACKNOWLEDGED_INTERACTION_ID FROM INTERACTIONLOGS`);
    InteractionLogAggregateRoot[] logs = [];
    check from InteractionLogAggregateRootRecord item in entries
        do {
            stream<InteractionEntityRecord, error?> interactionsEntries = jdbcClient->query(`SELECT ID,DATE,CONTENT,SENT_BY_OPERATOR FROM INTERACTIONS WHERE ID IN (SELECT INTERACTIONS_ID FROM INTERACTIONLOGS_INTERACTIONS WHERE INTERACTION_LOG_AGGREGATE_ROOT_CUSTOMER_ID = ${item.customer_Id})`);
            InteractionEntity[] interactions = [];
            InteractionEntityRecord[] recs = check from InteractionEntityRecord item2 in interactionsEntries select item2;
            foreach InteractionEntityRecord rec in recs {
                InteractionEntity e = new(rec.id,rec.date,rec.content,rec.sent_by_operator);
                interactions.push(e);
            }
            check interactionsEntries.close();
            logs.push(new(item.customer_Id,item.username,item.last_acknowledged_interaction_id, interactions));
        };
    check entries.close();
    return logs;
}

public function getInteractionLog(string customerId) returns InteractionLogAggregateRoot|error {
    InteractionLogAggregateRootRecord rec = check jdbcClient->queryRow(`SELECT CUSTOMER_ID, USERNAME,LAST_ACKNOWLEDGED_INTERACTION_ID FROM INTERACTIONLOGS WHERE CUSTOMER_ID = ${customerId}`);
    stream<InteractionEntityRecord, error?> interactionsEntries = jdbcClient->query(`SELECT ID,DATE,CONTENT,SENT_BY_OPERATOR FROM INTERACTIONS WHERE ID IN (SELECT INTERACTIONS_ID FROM INTERACTIONLOGS_INTERACTIONS WHERE INTERACTION_LOG_AGGREGATE_ROOT_CUSTOMER_ID = ${rec.customer_Id})`);
    InteractionEntity[] interactions = [];
    check from InteractionEntityRecord item2 in interactionsEntries 
        do {
            interactions.push(new(item2.id,item2.date,item2.content,item2.sent_by_operator));
        };
    check interactionsEntries.close();
    return new(rec.customer_Id,rec.username,rec.last_acknowledged_interaction_id,interactions);
}

public function addInteractionLog(InteractionLogAggregateRoot log) returns string?|error {
    sql:ExecutionResult result = check jdbcClient->execute(`INSERT INTO INTERACTIONLOGS VALUES (${log.getCustomerId()}, ${log.getUsername()},${log.getLastAcknowledgedInteractionId()})`);
    int|string? affectedRowCount = result.affectedRowCount;
    if(affectedRowCount is int && affectedRowCount != 0){
        InteractionEntity[]? tmp = log.getInteractions();
        if(tmp is InteractionEntity[]){ 
            if(tmp.length()>0){
                sql:ParameterizedQuery[] batch = from InteractionEntity item in tmp select `INSERT INTO INTERACTIONS VALUES (${item.getId()}, ${item.getDate()},${item.getContent()},${item.isSentByOperator()})`;
                _ = check jdbcClient->batchExecute(batch); 
                sql:ParameterizedQuery[] batch2 = from InteractionEntity item in tmp select `INSERT INTO INTERACTIONLOGS_INTERACTIONS VALUES (${log.getCustomerId()},${item.getId()})`;
                _ = check jdbcClient->batchExecute(batch2); 
            }                                           
            return log.getCustomerId();
        }
    }
    return error("Failed to insert record");
}

public function updateInteractionLog(InteractionLogAggregateRoot log) returns string?|error {
    sql:ExecutionResult result = check jdbcClient->execute(`UPDATE INTERACTIONLOGS SET USERNAME = ${log.getUsername()},LAST_ACKNOWLEDGED_INTERACTION_ID = ${log.getLastAcknowledgedInteractionId()} WHERE CUSTOMER_ID = ${log.getCustomerId()}`);
    int|string? affectedRowCount = result.affectedRowCount;
    if(affectedRowCount is int && affectedRowCount != 0){
        InteractionEntity[]? tmp = log.getInteractions();
        if(tmp is InteractionEntity[]){
            if(tmp.length() > 0){
                sql:ParameterizedQuery[] batch = from InteractionEntity item in tmp select `MERGE INTO INTERACTIONS KEY (ID) VALUES (${item.getId()},${item.getDate()},${item.getContent()},${item.isSentByOperator()})`;
                _ = check jdbcClient->batchExecute(batch);  
                sql:ParameterizedQuery[] batch2 = from InteractionEntity item in tmp select `MERGE INTO INTERACTIONLOGS_INTERACTIONS KEY (INTERACTIONS_ID) VALUES (${log.getCustomerId()},${item.getId()})`;
                _ = check jdbcClient->batchExecute(batch2);   
            }                                             
            return log.getCustomerId();
        }else{
            return error("Record is broken");
        }
    }
    return error("Failed to update record");
}

public function deleteInteractionLog(string customerId) returns int|error {
    sql:ExecutionResult result = check jdbcClient->execute(`DELETE FROM INTERACTIONLOGS_INTERACTIONS WHERE INTERACTION_LOG_AGGREGATE_ROOT_CUSTOMER_ID = ${customerId}`);
    int|string? affectedRowCount = result.affectedRowCount;
    if(affectedRowCount is int && affectedRowCount != 0){
        _ = check jdbcClient->execute(`DELETE FROM INTERACTIONS WHERE ID NOT IN (SELECT INTERACTIONS_ID FROM INTERACTIONLOGS_INTERACTIONS)`);
    }
    sql:ExecutionResult result3 = check jdbcClient->execute(`DELETE FROM INTERACTIONLOGS WHERE CUSTOMER_ID = ${customerId}`);
    int|string? affectedRowCount3 = result3.affectedRowCount;
    return affectedRowCount3 is int ? affectedRowCount3 : error("Unable to obtain the affected row count");
    
}

public function getNotifications() returns Notification[]|error {
    Notification[] notifications = [];
    InteractionLogAggregateRoot[] interactionlogs = check getInteractionLogs();
    foreach InteractionLogAggregateRoot item in interactionlogs {
        int count = item.getNumberOfUnacknowledgedInteractions();
        if(count > 0){
            Notification notification = new(item.getCustomerId(),item.getUsername(),count);
            notifications.push(notification);
        }
    }
    return notifications;
}

service class WsService {
    *websocket:Service;

    remote function onMessage(websocket:Caller caller,MessageDto message) returns websocket:Error?|error?{
        log:printInfo("Processing message from " + message.username);
        string clientUrl = "ws://localhost:"+port.toString()+"/ws/topic/messages";
        final string? customerId = message.customerId;
        final string id = uuid:createType1AsString();
        string date = time:utcToString(time:utcNow());
        final InteractionEntity interaction = new(id,date,message.content,message.sentByOperator);

        InteractionLogAggregateRoot|error optInteractionLog = getInteractionLog(message.customerId);
        InteractionLogAggregateRoot interactionLog;
        if(optInteractionLog is InteractionLogAggregateRoot){
            InteractionEntity[]? interactions = optInteractionLog.getInteractions();
            if(interactions is InteractionEntity[]){
                interactions.push(interaction);
                interactionLog = new (optInteractionLog.getCustomerId(),optInteractionLog.getUsername(),optInteractionLog.getLastAcknowledgedInteractionId(),interactions);
                _ = check updateInteractionLog(interactionLog);
            }
        }else{
            InteractionEntity[] interactions = [];
            interactions.push(interaction);
            interactionLog = new (customerId,message.username,(),interactions);
            _ = check addInteractionLog(interactionLog);
        }
        _ = check broadcastNotifications();
        websocket:Client wsClient = check new(clientUrl);
        MessageDto dto = {id:id, date:date, customerId:message.customerId, username:message.username, content:message.content, sentByOperator:message.sentByOperator};
        check wsClient->writeMessage(dto);
    }
}

function broadcastNotifications() returns error?{
    log:printInfo("Broadcasting updated notifications");
    Notification[]|error tmp = getNotifications();
    if(tmp is Notification[]){
        NotificationDto[] notifications = [];
        foreach Notification item in tmp {
            notifications.push({customerId: item.getCustomerId(),username:item.getUsername(),count: item.getCount()});
        }
        string clientUrl = "ws://localhost:"+port.toString()+"/ws/topic/notifications";
        websocket:Client wsClient = check new(clientUrl);
        check wsClient->writeMessage(notifications);
    }else{
        log:printInfo(tmp.message(),tmp);
    }
}

public function main() returns error?{
    log:printInfo("--- Customer Management backend started ---");
    log:printInfo("Start of main function.. ");

    if(ddl_auto == "drop_and_create"){
        _ = check jdbcClient->execute(`DROP TABLE IF EXISTS INTERACTIONLOGS_INTERACTIONS`);
        _ = check jdbcClient->execute(`DROP TABLE IF EXISTS INTERACTIONLOGS`);
        _ = check jdbcClient->execute(`DROP TABLE IF EXISTS INTERACTIONS`);
    }

    _ = check jdbcClient->execute(`CREATE TABLE IF NOT EXISTS INTERACTIONS(ID VARCHAR(255) NOT NULL,DATE TIMESTAMP,CONTENT VARCHAR(255),SENT_BY_OPERATOR BOOLEAN NOT NULL,PRIMARY KEY (ID))`);

    _ = check jdbcClient->execute(`CREATE TABLE IF NOT EXISTS INTERACTIONLOGS(CUSTOMER_ID VARCHAR(255) NOT NULL,USERNAME VARCHAR(255),LAST_ACKNOWLEDGED_INTERACTION_ID  VARCHAR(255),PRIMARY KEY (CUSTOMER_ID))`);

    _ = check jdbcClient->execute(`CREATE TABLE IF NOT EXISTS INTERACTIONLOGS_INTERACTIONS(INTERACTION_LOG_AGGREGATE_ROOT_CUSTOMER_ID VARCHAR(255) NOT NULL,INTERACTIONS_ID VARCHAR(255) NOT NULL,CONSTRAINT FKNRLR4POAGW2DTE8QMGEWNL9EU_INDEX_B FOREIGN KEY (INTERACTION_LOG_AGGREGATE_ROOT_CUSTOMER_ID) REFERENCES INTERACTIONLOGS(CUSTOMER_ID),CONSTRAINT UK_F9MORY4MPI8W7CI4IBSS33M11_INDEX_B UNIQUE (INTERACTIONS_ID))`);
    
    log:printInfo("End of main function.. ");      
}