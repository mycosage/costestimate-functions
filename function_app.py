import azure.functions as func
import json
import logging
import os
import string
import random
import urllib.request
import urllib.parse
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


# ---------------------------------------------------------------------------
# Address Autocomplete — proxies Azure Maps Geocode Autocomplete API
# Keeps subscription key server-side. Restricted to US (OR/WA bias).
# ---------------------------------------------------------------------------

AZURE_MAPS_BASE = "https://atlas.microsoft.com/geocode:autocomplete"
AZURE_MAPS_API_VERSION = "2025-06-01-preview"

# Center of Oregon/Washington region for geographic bias
# (roughly Salem, OR — biases results toward PNW without hard-excluding others)
BIAS_LAT = 44.9429
BIAS_LON = -123.0351


@app.route(route="address-autocomplete", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def address_autocomplete(req: func.HttpRequest) -> func.HttpResponse:
    """Proxy Azure Maps autocomplete. Query param: q (partial address string)."""
    query = req.params.get("q", "").strip()

    if not query or len(query) < 3:
        return func.HttpResponse(
            json.dumps({"results": []}),
            status_code=200,
            mimetype="application/json",
        )

    maps_key = os.environ.get("AZURE_MAPS_KEY", "")
    if not maps_key:
        logging.error("AZURE_MAPS_KEY not configured")
        return func.HttpResponse(
            json.dumps({"error": "Maps not configured"}),
            status_code=500,
            mimetype="application/json",
        )

    # Build Azure Maps request
    params = urllib.parse.urlencode({
        "api-version": AZURE_MAPS_API_VERSION,
        "subscription-key": maps_key,
        "query": query,
        "coordinates": f"{BIAS_LON},{BIAS_LAT}",
        "countryRegion": "US",
        "top": "5",
        "resultTypeGroups": "Address",
    })

    url = f"{AZURE_MAPS_BASE}?{params}"

    try:
        req_maps = urllib.request.Request(url, method="GET")
        req_maps.add_header("Accept", "application/json")

        with urllib.request.urlopen(req_maps, timeout=5) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, str):
                data = json.loads(data)

    except Exception as e:
        logging.error(f"Azure Maps error: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Address lookup failed"}),
            status_code=502,
            mimetype="application/json",
        )

    # Parse the GeoJSON response into a clean list for the frontend
    results = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        addr = props.get("address", {})

        # Extract state from adminDistricts array
        admin = addr.get("adminDistricts", [])
        state = admin[0].get("shortName", "") if len(admin) > 0 else ""
        county = admin[1].get("name", "") if len(admin) > 1 else ""

        # Only return OR and WA results
        if state not in ("OR", "WA", "Ore.", "Wash.", "Oregon", "Washington"):
            continue

        # Normalize state abbreviation
        state_abbr = state
        if state in ("Oregon", "Ore."):
            state_abbr = "OR"
        elif state in ("Washington", "Wash."):
            state_abbr = "WA"

        results.append({
            "address": addr.get("addressLine", ""),
            "city": addr.get("locality", ""),
            "state": state_abbr,
            "zip": addr.get("postalCode", ""),
            "county": county.replace(" County", ""),
            "formatted": addr.get("formattedAddress", ""),
        })

    return func.HttpResponse(
        json.dumps({"results": results}),
        status_code=200,
        mimetype="application/json",
    )


# ---------------------------------------------------------------------------
# Property Lookup — stub for ATTOM integration (requires API key)
# ---------------------------------------------------------------------------

@app.route(route="property-lookup", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def property_lookup(req: func.HttpRequest) -> func.HttpResponse:
    """Look up property data by address. Requires ATTOM API key."""
    address1 = req.params.get("address1", "").strip()
    address2 = req.params.get("address2", "").strip()

    if not address1 or not address2:
        return func.HttpResponse(
            json.dumps({"error": "Missing address1 and/or address2 parameters"}),
            status_code=400,
            mimetype="application/json",
        )

    attom_key = os.environ.get("ATTOM_API_KEY", "")
    if not attom_key:
        # Return stub response until ATTOM is configured
        return func.HttpResponse(
            json.dumps({
                "stub": True,
                "message": "ATTOM not configured — returning placeholder",
                "tax": {"taxamt": None, "taxyear": None},
                "property": {"sqft": None, "yearbuilt": None, "bedrooms": None, "bathrooms": None},
                "assessed": {"total": None, "land": None, "improvements": None},
            }),
            status_code=200,
            mimetype="application/json",
        )

    # ATTOM assessment/detail endpoint
    attom_url = "https://api.gateway.attomdata.com/propertyapi/v1.0.0/assessment/detail"
    params = urllib.parse.urlencode({
        "address1": address1,
        "address2": address2,
    })

    try:
        req_attom = urllib.request.Request(
            f"{attom_url}?{params}", method="GET"
        )
        req_attom.add_header("Accept", "application/json")
        req_attom.add_header("apikey", attom_key)

        with urllib.request.urlopen(req_attom, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

    except Exception as e:
        logging.error(f"ATTOM API error: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Property lookup failed"}),
            status_code=502,
            mimetype="application/json",
        )

    # Extract relevant fields from ATTOM response
    prop = data.get("property", [{}])[0] if data.get("property") else {}
    assessment = prop.get("assessment", {})
    tax = assessment.get("tax", {})
    assessed = assessment.get("assessed", {})
    building = prop.get("building", {})
    summary = building.get("summary", {}) if building else {}
    rooms = building.get("rooms", {}) if building else {}
    lot = prop.get("lot", {})

    result = {
        "stub": False,
        "tax": {
            "taxamt": tax.get("taxamt"),
            "taxyear": tax.get("taxyear"),
        },
        "property": {
            "sqft": summary.get("sizeInd"),
            "yearbuilt": summary.get("yearbuilt"),
            "bedrooms": rooms.get("beds"),
            "bathrooms": rooms.get("bathsfull"),
            "lotsize": lot.get("lotsize2"),
            "stories": summary.get("stories"),
        },
        "assessed": {
            "total": assessed.get("assdttlvalue"),
            "land": assessed.get("assdlandvalue"),
            "improvements": assessed.get("assdimprvalue"),
        },
    }

    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype="application/json",
    )
