import ballerina/log;
import ballerina/http;
import ballerina/websocket;

configurable int port = ?;

listener http:Listener httpListener = check new(port);

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"],
        allowMethods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allowHeaders: ["authorization", "content-type", "x-auth-token"],
        exposeHeaders: ["x-auth-token"]
    }
}
service / on httpListener{

    isolated resource function get customers(http:Caller caller, http:Request request, string filter = "", int 'limit = 10, int offset = 0) returns error? {
        http:Response response = new;
        PaginatedCustomerResponseDto|error<CustomerCoreNotAvailableException> result = getCustomers(filter,'limit,offset);
        if(result is PaginatedCustomerResponseDto){
            response.statusCode = 200;
            response.setJsonPayload(result.toJson());
        }else{
            return result;
        }
        check caller->respond(response);
        return;
    }

    isolated resource function get customers/[string customerId](http:Caller caller, http:Request request) returns error? {
        http:Response response = new;
        CustomerId id = new(customerId);
        ()|CustomerDto|error result = getCustomer(id);
        if(result is CustomerDto){
            response.statusCode = 200;
            response.setJsonPayload(result.toJson());
        }else if(result is ()){
            string errorMessage = "Failed to find a customer with id '" + customerId + "'.";
            log:printInfo(errorMessage);
            error<CustomerNotFoundException> err = error(errorMessage);
            return err;
        }else{
            return result;
        }
        check caller->respond(response);
        return;
    }

    isolated resource function put customers/[string customerId](http:Caller caller, http:Request request) returns error? { 
        http:Response response = new;
        CustomerId id = new(customerId);
        json payload = check request.getJsonPayload();
        CustomerProfileDto customerProfile = check payload.cloneWithType(CustomerProfileDto);
        CustomerDto|error result = updateCustomer(id,customerProfile);
        if(result is CustomerDto){
            response.statusCode = 200;
            response.setJsonPayload(result.toJson());
        }else{
            return result;
        }
        check caller->respond(response);
        return;
    }

    isolated resource function get interaction\-logs/[string customerId](http:Caller caller, http:Request request) returns error? {
        http:Response response = new;
        InteractionLogAggregateRoot|error optInteractionLog = getInteractionLog(customerId);
        if(optInteractionLog is error){
            log:printInfo("Failed to find an interaction log for the customer with id '" + customerId + "'. Returning an empty interaction log instead.");
            InteractionLogAggregateRoot emptyInteractionLog = new(customerId,"",(),[]);
            response.statusCode = 200;
            response.setJsonPayload(emptyInteractionLog.toJson());
        }else{
            response.statusCode = 200;
            response.setJsonPayload(optInteractionLog.toJson());
        }
        check caller->respond(response);
        return;
    }

    isolated resource function patch interaction\-logs/[string customerId](http:Caller caller, http:Request request) returns error? {
        http:Response response = new;
        json payload = check request.getJsonPayload();

        InteractionAcknowledgementDto interactionAcknowledgementDto = check payload.cloneWithType(InteractionAcknowledgementDto );
        InteractionLogAggregateRoot|error optInteractionLog = getInteractionLog(customerId);
        if(optInteractionLog is error){
            string errorMessage = "Failed to acknowledge interactions, because there is no interaction log for customer with id '" + customerId + "'.";
            log:printInfo(errorMessage);
            error<InteractionLogNotFoundException> err = error(errorMessage);
            return err;
        }else{
            optInteractionLog.setLastAcknowledgedInteractionId(interactionAcknowledgementDto.lastAcknowledgedInteractionId);
            _ = check updateInteractionLog(optInteractionLog);
            //_ = check broadcastNotifications();
            response.statusCode = 200;
            response.setJsonPayload(optInteractionLog.toJson());
        }

        check caller->respond(response);
        return;
    }
    
    isolated resource function get notifications(http:Caller caller, http:Request request) returns error?{
        http:Response response = new;
        NotificationDto[] notifications = [];
        foreach Notification item in check getNotifications() {
            notifications.push({customerId: item.getCustomerId(),username:item.getUsername(),count: item.getCount()});
        }
        response.statusCode = 200;
        response.setJsonPayload(notifications.toJson());
        check caller->respond(response);
        return;
    }
}

listener websocket:Listener wsListener = new websocket:Listener(httpListener);

service /ws/chat/messages on wsListener{
    resource function get .(http:Request req) returns websocket:Service|websocket:UpgradeError{
        return new WsService();
    }
}

service /ws/topic/messages on wsListener{
    resource function get .(http:Request req) returns websocket:Service|websocket:UpgradeError{
        return service object websocket:Service {
            remote function onMessage(websocket:Caller caller,json message) returns websocket:Error?|error?{
                log:printInfo(message.toJsonString());
            }
        };
    }
}

service /ws/topic/notifications on wsListener{
    resource function get .(http:Request req) returns websocket:Service|websocket:UpgradeError{
        return service object websocket:Service {
            remote function onMessage(websocket:Caller caller,json message) returns websocket:Error?|error?{
                log:printInfo(message.toJsonString());
            }
        };
    }
}

