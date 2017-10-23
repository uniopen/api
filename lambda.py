# -*- coding: utf-8 -*-
import boto3
import json
from botocore.exceptions import ClientError


client = boto3.client('s3',
                      region_name='eu-west-1')


def respond(data, code=200):
    return {"body": json.dumps(data), "headers": {"Content-Type": "application/json"}, "statusCode": code}


def s3call(prefix, delimiter):
    s3result = client.list_objects_v2(Bucket="uniopen-data", Prefix=prefix, Delimiter=delimiter)
    prefixes = s3result.get("CommonPrefixes")
    return prefixes


def get_universities():
    unis = s3call("", "/")
    if unis is not None:
        keys = [u.get("Prefix")[:-1] for u in unis]
        return keys, 200
    else:
        return {"error": "not found"}, 500


def get_structures(uni_id):
    pref = uni_id + "/"
    unis = s3call(pref, ".json")
    if unis is not None:
        keys = [u.get("Prefix").replace(pref, "")[:-5] for u in unis]
        return keys, 200
    else:
        return {"error": "not found"}, 500


def get_details(uni_id, structure_id):
    key = "%s/%s.json" % (uni_id, structure_id)
    try:
        response = client.get_object(Bucket="uniopen-data", Key=key)
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return {"error": "not found"}, 500
    details = json.loads(response['Body'].read())
    return details, 200


def get_single_details(uni_id, structure_id, item_id):
    key = "%s/%s.json" % (uni_id, structure_id)
    try:
        response = client.get_object(Bucket="uniopen-data", Key=key)
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return {"error": "not found"}, 500
    details = json.loads(response['Body'].read()).get(item_id)
    if details is None:
        return {"error": "not found"}, 500
    return details, 200


def main(event, context):
    print(event)
    req = event.get("path").replace("/v1/", "/").replace("/v1", "/")
    print("Requested path: %s" % req)
    if req == "/":
        data, code = get_universities()
        return respond(data, code)

    req = req[1:].strip("/").split("/")
    if len(req) == 1:
        data, code = get_structures(req[0])
        return respond(data, code)
    elif len(req) == 2:
        data, code = get_details(req[0], req[1])
        return respond(data, code)
    elif len(req) == 3:
        data, code = get_single_details(req[0], req[1], req[2])
        return respond(data, code)
    else:
        return respond({"error": "check request path"}, 400)




# event = {'resource': '/{proxy+}', 'path': '/unipd/mensa/acli', 'httpMethod': 'GET', 'headers': {'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'en-GB,en;q=0.8,en-US;q=0.6,da;q=0.4,it;q=0.2', 'cache-control': 'no-cache', 'CloudFront-Forwarded-Proto': 'https', 'CloudFront-Is-Desktop-Viewer': 'true', 'CloudFront-Is-Mobile-Viewer': 'false', 'CloudFront-Is-SmartTV-Viewer': 'false', 'CloudFront-Is-Tablet-Viewer': 'false', 'CloudFront-Viewer-Country': 'DK', 'dnt': '1', 'Host': '7yvrlfhe86.execute-api.eu-west-1.amazonaws.com', 'postman-token': 'ce306e1a-8ffb-8b6f-5f77-3ef6933b0c7b', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36', 'Via': '2.0 c21dd0d2f06b14a25afdabda3a7f96a6.cloudfront.net (CloudFront)', 'X-Amz-Cf-Id': 'h60u3QkUCiAtfOATSxdUroHhu7Jvd_7AtePTT_XdsTx8FGhIpcX1ZQ==', 'X-Amzn-Trace-Id': 'Root=1-59ecaad2-2c9d4b7948143675441e596e', 'X-Forwarded-For': '130.226.41.9, 52.46.52.16', 'X-Forwarded-Port': '443', 'X-Forwarded-Proto': 'https'}, 'queryStringParameters': None, 'pathParameters': {'proxy': 'mensa'}, 'stageVariables': None, 'requestContext': {'path': '/v1/mensa/', 'accountId': '921570358849', 'resourceId': '286e1x', 'stage': 'v1', 'requestId': '22a04d12-b735-11e7-a260-e18ce7e2d4ee', 'identity': {'cognitoIdentityPoolId': None, 'accountId': None, 'cognitoIdentityId': None, 'caller': None, 'apiKey': '', 'sourceIp': '130.226.41.9', 'accessKey': None, 'cognitoAuthenticationType': None, 'cognitoAuthenticationProvider': None, 'userArn': None, 'userAgent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36', 'user': None}, 'resourcePath': '/{proxy+}', 'httpMethod': 'GET', 'apiId': '7yvrlfhe86'}, 'body': None, 'isBase64Encoded': False}

# print(main(event, 1))