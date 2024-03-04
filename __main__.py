import json
import boto3
import requests
from argparse import ArgumentParser, ArgumentError

def get_json_data(json_data):
  """
  Reads JSON data from a file or URL.

  Args:
      json_data: The path to a file or URL containing JSON data.

  Returns:
      A list of dictionaries representing the JSON data.
  """
  if json_data.startswith("http"):
    # Handle URL: Get request and read content
    response = requests.get(json_data)
    response.raise_for_status()  # Raise error for non-2xx status codes
    return json.loads(response.content)
  else:
    # Handle file path: Read file contents
    with open(json_data, 'r') as f:
      return json.load(f)


def import_data(profile_name, table_name, json_data):
  """
  Imports a JSON array into a DynamoDB table using batch insertion.

  Args:
      profile_name: The name of the AWS profile to use (defaults to 'default').
      table_name: The name of the DynamoDB table to import data into.
      json_data: The path to a file or URL containing JSON data.
  """

  try:
    # Set up boto3 session with profile
    session = boto3.Session(profile_name=profile_name)
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Batch size for DynamoDB writes
    batch_size = 25

    # Split data into batches
    batches = [json_data[i:i+batch_size] for i in range(0, len(json_data), batch_size)]

    for batch in batches:
      # Convert each item to the format expected by DynamoDB
      items = [{k: v for k, v in item.items()} for item in batch]
      with table.batch_writer() as batch_writer:
        for item in items:
          batch_writer.put_item(Item=item)

    print(f"Successfully imported {len(json_data)} items into table {table_name}")
  except (boto3.exceptions. botocore.exceptions.ClientError, requests.exceptions.RequestException) as e:
    print(f"Error: {e}")


if __name__ == "__main__":
  parser = ArgumentParser(description="Import JSON data to DynamoDB")
  parser.add_argument("-p", "--profile", dest="profile_name", default="default",
                      help="The name of the AWS profile to use (defaults to 'default')")
  parser.add_argument("table_name", help="The name of the DynamoDB table")
  parser.add_argument("json_data", help="The path to a file or URL containing JSON data")
  args = parser.parse_args()

  # Validate JSON data format
  if not (args.json_data.startswith("http") or args.json_data.endswith(".json")):
    raise ArgumentError("json_data", "Invalid JSON data format. Must be a URL or file ending in '.json'")

  # Read JSON data based on type (file or URL)
  json_data = get_json_data(args.json_data)

  import_data(args.profile_name, args.table_name, json_data)
