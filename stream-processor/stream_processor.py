import json

def lambda_handler(event, context):
    print(f"Stream event received: {json.dumps(event)}")
    
    for record in event['Records']:
        event_name = record['eventName']  # INSERT, MODIFY, REMOVE
        
        if event_name == 'MODIFY':
            new_image = record['dynamodb'].get('NewImage', {})
            old_image = record['dynamodb'].get('OldImage', {})
            
            student_id = new_image.get('student_id', {}).get('S', '')
            new_risk = new_image.get('risk_score', {}).get('S', '')
            old_risk = old_image.get('risk_score', {}).get('S', '')
            
            if new_risk != old_risk:
                print(f"RISK CHANGE DETECTED!")
                print(f"Student {student_id}: {old_risk} -> {new_risk}")
                
                if new_risk == 'HIGH':
                    print(f"ALERT: Student {student_id} is now HIGH risk!")
        
        elif event_name == 'INSERT':
            new_image = record['dynamodb'].get('NewImage', {})
            student_id = new_image.get('student_id', {}).get('S', '')
            print(f"New student added: {student_id}")
        
        elif event_name == 'REMOVE':
            old_image = record['dynamodb'].get('OldImage', {})
            student_id = old_image.get('student_id', {}).get('S', '')
            print(f"Student removed: {student_id}")
    
    return {'statusCode': 200}