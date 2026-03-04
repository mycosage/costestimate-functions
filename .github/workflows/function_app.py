import azure.functions as func
import json
import logging
import os
import string
import random
from datetime import datetime, timezone
from azure.data.tables import TableClient

app = func.FunctionApp()

TABLE_NAME = "urlshortener"


def get_table_client():
    conn_str = os.environ["TABLE_STORAGE_CONNECTION"]
    return TableClient.from_connection_string(conn_str, TABLE_NAME)


def generate_short_id(length=8):
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


@app.route(route="save", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def save(req: func.HttpRequest) -> func.HttpResponse:
    """Save a JSON payload and return a short ID."""
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    short_id = generate_short_id()
    table_client = get_table_client()

    entity = {
        "PartitionKey": "estimates",
        "RowKey": short_id,
        "payload": json.dumps(payload),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        table_client.create_entity(entity)
    except Exception as e:
        logging.error(f"Table storage error: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Failed to save"}),
            status_code=500,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps({"id": short_id}),
        status_code=201,
        mimetype="application/json",
    )


@app.route(route="load", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def load(req: func.HttpRequest) -> func.HttpResponse:
    """Load a saved payload by short ID."""
    short_id = req.params.get("id")

    if not short_id:
        return func.HttpResponse(
            json.dumps({"error": "Missing 'id' parameter"}),
            status_code=400,
            mimetype="application/json",
        )

    table_client = get_table_client()

    try:
        entity = table_client.get_entity(partition_key="estimates", row_key=short_id)
        payload = json.loads(entity["payload"])
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "Not found"}),
            status_code=404,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        mimetype="application/json",
    )
