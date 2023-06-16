import ballerina/log;
import ballerina/http;
import ballerina/websocket;

configurable int port = ?;

listener http:Listener httpListener = check new(port, {
    httpVersion: http:HTTP_1_1
});

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"],
        allowCredentials: true,
        allowMethods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allowHeaders: ["authorization", "content-type", "x-auth-token"],
        exposeHeaders: ["x-auth-token"]
    }
}
service / on httpListener{

    resource function get customers(http:Caller caller, http:Request request, string filter = "", int 'limit = 10, int offset = 0) returns error? {
        http:Response response = new;
        lock {
            PaginatedCustomerResponseDto|error<CustomerCoreNotAvailableException> result = getCustomers(filter,'limit,offset);
            if(result is PaginatedCustomerResponseDto){
                response.statusCode = 200;
                response.setJsonPayload(result.toJson());
            }else{
                return result;
            }
        }
        check caller->respond(response);
        return;
    }

    resource function get customers/[string customerId](http:Caller caller, http:Request request) returns error? {
        http:Response response = new;
        CustomerId id = new(customerId);
        lock {
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
        }
        check caller->respond(response);
        return;
    }

    resource function put customers/[string customerId](http:Caller caller, http:Request request) returns error? { 
        http:Response response = new;
        CustomerId id = new(customerId);
        json payload = check request.getJsonPayload();
        CustomerProfileDto customerProfile = check payload.cloneWithType(CustomerProfileDto);
        lock {
            CustomerDto|error result = updateCustomer(id,customerProfile);
            if(result is CustomerDto){
                response.statusCode = 200;
                response.setJsonPayload(result.toJson());
            }else{
                return result;
            }
        }
        check caller->respond(response);
        return;
    }

    resource function get interaction\-logs/[string customerId](http:Caller caller, http:Request request) returns error? {
        http:Response response = new;
        lock {
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
        }
        check caller->respond(response);
        return;
    }

    resource function patch interaction\-logs/[string customerId](http:Caller caller, http:Request request) returns error? {
        http:Response response = new;
        json payload = check request.getJsonPayload();

        InteractionAcknowledgementDto interactionAcknowledgementDto = check payload.cloneWithType(InteractionAcknowledgementDto );
        lock {
            InteractionLogAggregateRoot|error optInteractionLog = getInteractionLog(customerId);
            if(optInteractionLog is error){
                string errorMessage = "Failed to acknowledge interactions, because there is no interaction log for customer with id '" + customerId + "'.";
                log:printInfo(errorMessage);
                error<InteractionLogNotFoundException> err = error(errorMessage);
                return err;
            }else{
                optInteractionLog.setLastAcknowledgedInteractionId(interactionAcknowledgementDto.lastAcknowledgedInteractionId);
                _ = check updateInteractionLog(optInteractionLog);
                _ = check broadcastNotifications();
                response.statusCode = 200;
                response.setJsonPayload(optInteractionLog.toJson());
            }
        }
        check caller->respond(response);
        return;
    }
    
    resource function get notifications(http:Caller caller, http:Request request) returns error?{
        http:Response response = new;
        NotificationDto[] notifications = [];
        lock {
            foreach Notification item in check getNotifications() {
                notifications.push({customerId: item.getCustomerId(),username:item.getUsername(),count: item.getCount()});
            }
            response.statusCode = 200;
            response.setJsonPayload(notifications.toJson());
        }
        check caller->respond(response);
        return;
    }
}

listener websocket:Listener wsListener = new websocket:Listener(httpListener);

service /ws on wsListener{
    resource function get .(http:Request req) returns websocket:Service|websocket:UpgradeError{
        if(req.rawPath.includes("/chat/messages")){
            return new WsService();
        }else{
            return service object websocket:Service {
                remote function onMessage(websocket:Caller caller,json message) returns websocket:Error?|error?{
                    log:printInfo(message.toJsonString());
                }
            };
        }
    }
}

