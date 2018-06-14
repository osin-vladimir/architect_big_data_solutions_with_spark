import requests, json, datetime, boto3, time

currencies = ['USD','GBP','EUR']
rates = {}

session = boto3.Session(profile_name='default')
kinesis_client = session.client('kinesis')

while True:
  for currency in currencies:
    req = requests.get('https://api.coindesk.com/v1/bpi/currentprice/{}.json'.format(currency))
    req_json = json.loads(req.content)
    rate = float(req_json['bpi'][currency]['rate_float'])
    timestamp = datetime.datetime.now()
    rates['timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    rates[currency] = rate
  print (json.dumps(rates))
  kinesis_client.put_record(StreamName='bitcoin-exchange-rate',Data=json.dumps(rates),PartitionKey=rates['timestamp'])
  time.sleep(60)

  
