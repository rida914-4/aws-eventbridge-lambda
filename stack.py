"""
A simple AWS services stack which processes events using
    - EventBridge
    - Fargate
    - ECS
    - Lambda
    - DynamoDB
    - AWS CDK

"""
__author__ = "Ridah Naseem"
__credits__ = [""]
__version__ = "1.0.1"
__maintainer__ = "Ridah Naseem"
__email__ = "rida914@gmail.com"
__status__ = "Development"


from aws_cdk import (
    aws_lambda as _lambda,
    aws_lambda_event_sources as _event,
    aws_dynamodb as dynamo_db,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as targets,
    core
)
import json


class EventBridgeLambda(core.Stack):
    """"""

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        """

        :param scope:
        :param id:
        :param kwargs:
        """

        # Do not override the base class contructor, we want it's properties as well
        super().__init__(scope, id, **kwargs)

        # todo: Keep this for now, observe the level of concurrency lambda achieves if we
        # scale it incrementally
        self.lambda_throttle_size = 2

        # permissions
        self.eventbridge_write_policy = self.allow_eventbridge_writeeventbus()

        # Setup AWS resources
        self.table = self.generate_dynamodb_table()
        self.bucket = self.generate_s3_bucket()
        self.queue = self.generate_sqs_queue()
        self.generate_s3sqs_trigger()
        self.vpc = self.generate_vpc()
        self.cluster = self.generate_ecs_cluster()
        self.task = self.generate_fargate_task()
        self.container = self.generate_ecs_container()
        self.subnet_id = self.generate_subnet_ids()

    def run(self):
        """
            Generate the stack, create lambdas and give appropriate permissions
        :return:
        """

        # Generate lambdas
        run_fargate_lambda = self.generate_lambda(lambda_name="RunFargatePopulateEventBridgeTask",
                                                       handler="s3SqsEventConsumer.handler",
                                                       code_name="run_fargate_task_lambda")
        load_dynamodb_lambda = self.generate_lambda(lambda_name="DataIntoDynamoDB", handler="load.handler",
                                                         code_name="load_data_to_dynamodb_lambda")

        self.run_fargate_task(run_fargate_lambda)
        self.load_data_to_dynamodb(load_dynamodb_lambda)

    def generate_dynamodb_table(self):
        """

        :return: table
        """
        # DynamoDB table
        return dynamo_db.Table(self, "Result",
                               partition_key=dynamo_db.Attribute(name="id", type=dynamo_db.AttributeType.STRING)
                               )

    def generate_s3_bucket(self):
        """
            S3 bucket, source of all the incoming data
        :return:
        """
        return s3.Bucket(self, "SourceBucketDevelopment")

    def generate_sqs_queue(self):
        """
        # SQS : this will act a trigger for the lambda. we can pull events from this queue
        # note: do not use SNS here. if SNS is needed, do use SQS with it. if lambda fails you WILL lose
        # your event. SQS would help here as it is an Atleast-once-delivery system
        # note: visibility_timeout 30 is enough for now. if the event is not processed it will become visible
        # in the queue again. create a heartbeat if you are not sure how long the event processing time
        :return: queue
        """
        return sqs.Queue(self, 'TriggerLambdaQueue', visibility_timeout=core.Duration.seconds(30))

    def generate_vpc(self):
        """

        :return:
        """
        return ec2.Vpc(self, "DevVPC", max_azs=2)

    def generate_ecs_cluster(self):
        """

        :return:
        """
        return ecs.Cluster(self, 'DEVCluster', vpc=self.vpc)

    def trigger_s3_queue(self):
        """
            Trigger SQS event drop when an object is created in S3
        :return:
        """
        return self.bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(self.queue))

    def generate_fargate_task(self):
        """
            Task to load events into the fargate
        :return:
        """

        return ecs.TaskDefinition(self, 'DEVFargateTask',
                                             memory_mib="512",
                                             cpu="256",
                                             compatibility=ecs.Compatibility.FARGATE)

    def allow_eventbridge_writeeventbus(self):
        """
            allow anyone to put events in the event bus
        :return:
        """
        return iam.PolicyStatement(effect=iam.Effect.ALLOW, resources=['*'], actions=['events:PutEvents'])


    def allow_fargate_readaccess(self):
        """
            Fargate puts events on the eventbridge; give it write permission
            Fargate needs to access S3 object: give it read permission
        :return:
        """
        self.task.add_to_task_role_policy(self.eventbridge_write_policy)
        self.bucket.grant_read(self.task_definition.task_role)

    def generate_ecs_container(self):
        """

        :return: ecs container
        """
        return self.task_definition.add_container('DevContainer',
                                                  image=ecs.ContainerImage.from_asset('container'),
                                                  environment={
                                                      'S3_BUCKET_NAME': self.bucket.bucket_name,
                                                      'S3_OBJECT_KEY': ''
                                                  })

    def generate_subnet_ids(self):
        """
            Generate subnet ID
        :return:
        """
        subnet_ids = []
        for subnet in self.vpc.private_subnets:
            subnet_ids.append(subnet.subnet_id)
        return subnet_ids

    def generate_lambda(self, handler, code_name, lambda_name):
        """
            AWS Lambda resource to trigger various AWS services
        :return:
        """
        return _lambda.Function(self, lambda_name,
                                          runtime=_lambda.Runtime.PYTHON_3_7,
                                          handler=handler,
                                          code=_lambda.Code.from_asset(code_name),
                                          reserved_concurrent_executions=self.lambda_throttle_size,
                                          environment={
                                              "CLUSTER_NAME": self.cluster.cluster_name,
                                              "TASK_DEFINITION": self.task_definition.task_definition_arn,
                                              "SUBNETS": json.dumps(self.subnet_id),
                                              "CONTAINER_NAME": self.container.container_name
                                          }
                                          )

    def load_data_to_dynamodb(self, load_dynamodb_lambda):
        """
            load the results at the end of the parsing
        :return:
        """

        load_dynamodb_lambda.add_to_role_policy(self.eventbridge_write_policy)
        self.table.grant_read_write_data(load_dynamodb_lambda)

        load_rule = events.Rule(self, 'DevLoadDataDynamoDB',
                                description='load results into dynamodb',
                                event_pattern=events.EventPattern(source=['.'],
                                                                  detail_type=[''],
                                                                  detail={
                                                                      "status": ["finalized"]
                                                                  }))
        load_rule.add_target(targets.LambdaFunction(handler=load_dynamodb_lambda))

    def run_fargate_task(self, run_fargate_lambda):
        """

        :return:
        """
        # Grant permissions to consume messages from a queue
        self.queue.grant_consume_messages(self.run_fargate_lambda)

        # add SQS queue event source and inject the queue
        run_fargate_lambda.add_event_source(_event.SqsEventSource(queue=self.queue))

        self.allow_permissions_to_lambda()

    def allow_permissions_to_lambda(self, run_fargate_lambda):

        # allow the lambda function to write to event bus
        run_fargate_lambda.add_to_role_policy(self.eventbridge_write_policy)

        # allow lambda to run the ECS container
        run_task = iam.PolicyStatement(
            effect=iam.Effect.ALLOW, resources=[self.task_definition.task_definition_arn], actions=['ecs:RunTask'])
        run_fargate_lambda.add_to_role_policy(run_task)

        # assign a role
        task_execution_role = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            resources=[self.task_definition.obtain_execution_role().role_arn,
                       self.task_definition.task_role.role_arn],
            actions=['iam:PassRole'])
        run_fargate_lambda.add_to_role_policy(task_execution_role)
