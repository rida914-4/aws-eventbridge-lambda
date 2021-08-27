
# EventBridge-Lambda-Stack

A simple AWS services stack which processes events using


    - EventBridge
    - Fargate
    - ECS
    - Lambda
    - DynamoDB
    - AWS CDK


### Solution 1
Fargate and ECS is used to process events, lambda to load the results

    Push notification         Eventbridge             Fargate                        Lambda to load into dynamodb
    ------------       -------------------         ------------------                  ------------------
        SNS  ----->          Event Bus  ------->      Process events   --------->               Lambda          ----> dynamoDB
    ____________        ________________        __________________                      -----------------


### Solution 2
Fargate is used to populate EventBridge event bus. These events are injected into long running Lambda functions which process
and then load the results into dynamodb

    Data Source         Queue               Lambda to trigger Fargate    Fargate            Eventbridge     Lambda to proces     lambda to load into dynamodb
    ------------       -----------          ------------------          ----------          -------------      ---------------   ------------------
        S3          --->   SQS  ----->           Lambda     ------->  populate event bus  ---->  event bus ---->  process events  ---> store result
    ____________        ___________         ____________            __________________      ________________    ______________      _____________________
    Long Term Data storage


