import json
import uuid
import boto3
import random
import datetime
from typing import List
from dataclasses import dataclass, asdict
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools.utilities.batch import sqs_batch_processor
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.data_classes import SQSEvent

logger = Logger()
tracer = Tracer()
metrics = Metrics()


@dataclass
class OrderItem:
    productId: int
    amount: int

@tracer.capture_method
def record_handler(record):
    record_body = json.loads(record["body"])
    logger.info(record_body)
    order_items = [OrderItem(productId=item["id"], amount=item["amount"]) for item in record_body["orderItems"]]
    create_order(order_items)


@metrics.log_metrics
@logger.inject_lambda_context
@tracer.capture_lambda_handler
@sqs_batch_processor(record_handler=record_handler)
def lambda_handler(event: SQSEvent, context):
    return {'message': 'Orders batch has been processed successfully'}
    

@tracer.capture_method
def create_order(order_items: List[OrderItem]):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Orders')
    response = table.put_item(
       Item={
            'OrderId': str(uuid.uuid4()),
            'OrderItems': [asdict(item) for item in order_items],
            'TotalPrice': random.randint(1, 1000),
            'Currency': 'PLN',
            "CreatedAt": datetime.datetime.utcnow().strftime('%Y%m%dT%H:%M:%S')
        }
    )
    return response
