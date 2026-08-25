import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Students')

def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    http_method = event['httpMethod']
    path = event['path']
    
    # Route the request
    if http_method == 'GET' and path == '/students':
        return get_all_students()
    
    elif http_method == 'GET' and '/students/' in path:
        student_id = event['pathParameters']['id']
        return get_student(student_id)
    
    elif http_method == 'POST' and path == '/students':
        body = json.loads(event['body'])
        return create_student(body)
    
    elif http_method == 'PUT' and '/students/' in path:
        student_id = event['pathParameters']['id']
        body = json.loads(event['body'])
        return update_student(student_id, body)
    
    elif http_method == 'DELETE' and '/students/' in path:
        student_id = event['pathParameters']['id']
        return delete_student(student_id)
    
    return response(400, {'error': 'Invalid request'})


def get_all_students():
    result = table.scan()
    return response(200, result['Items'])


def get_student(student_id):
    result = table.get_item(Key={'student_id': student_id})
    if 'Item' not in result:
        return response(404, {'error': 'Student not found'})
    return response(200, result['Item'])


def create_student(body):
    table.put_item(Item=body)
    return response(201, {'message': 'Student created', 'student': body})


def update_student(student_id, body):
    table.update_item(
        Key={'student_id': student_id},
        UpdateExpression='SET #n = :name, mark = :mark, attendance = :att, risk_score = :risk',
        ExpressionAttributeNames={'#n': 'name'},
        ExpressionAttributeValues={
            ':name': body.get('name', ''),
            ':mark': body.get('mark', 0),
            ':att': body.get('attendance', 0),
            ':risk': body.get('risk_score', 'LOW')
        }
    )
    return response(200, {'message': 'Student updated'})


def delete_student(student_id):
    table.delete_item(Key={'student_id': student_id})
    return response(200, {'message': 'Student deleted'})


def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body, default=str)
    }