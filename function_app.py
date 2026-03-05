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
# Property Lookup — calls ATTOM assessment + property detail endpoints
# ---------------------------------------------------------------------------

def _attom_get(attom_key, resource, address1, address2):
    """Helper: call an ATTOM endpoint, return parsed dict or None."""
    base = f"https://api.gateway.attomdata.com/propertyapi/v1.0.0/{resource}"
    params = urllib.parse.urlencode({"address1": address1, "address2": address2})
    req_a = urllib.request.Request(f"{base}?{params}", method="GET")
    req_a.add_header("Accept", "application/json")
    req_a.add_header("apikey", attom_key)
    try:
        with urllib.request.urlopen(req_a, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            if isinstance(data, str):
                data = json.loads(data)
            return data
    except Exception as e:
        logging.warning(f"ATTOM {resource} error: {e}")
        return None


@app.route(route="property-lookup", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def property_lookup(req: func.HttpRequest) -> func.HttpResponse:
    """Look up property data by address. Calls ATTOM assessment + property detail."""
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
        return func.HttpResponse(
            json.dumps({
                "stub": True,
                "message": "ATTOM not configured — returning placeholder",
                "tax": {"taxamt": None, "taxyear": None},
                "property": {},
                "assessed": {},
                "sale": {},
                "owner": {},
                "location": {},
            }),
            status_code=200,
            mimetype="application/json",
        )

    # Call both endpoints in sequence
    assess_data = _attom_get(attom_key, "assessment/detail", address1, address2)
    prop_data = _attom_get(attom_key, "property/detail", address1, address2)

    # Parse assessment response
    a_prop = {}
    if assess_data and assess_data.get("property"):
        a_prop = assess_data["property"][0] if isinstance(assess_data["property"], list) else assess_data["property"]

    assessment = a_prop.get("assessment", {})
    tax = assessment.get("tax", {})
    assessed = assessment.get("assessed", {})
    market = assessment.get("market", {})

    # Parse property detail response
    p_prop = {}
    if prop_data and prop_data.get("property"):
        p_prop = prop_data["property"][0] if isinstance(prop_data["property"], list) else prop_data["property"]

    building = p_prop.get("building", {})
    b_summary = building.get("summary", {}) if isinstance(building, dict) else {}
    b_size = building.get("size", {}) if isinstance(building, dict) else {}
    b_rooms = building.get("rooms", {}) if isinstance(building, dict) else {}
    b_interior = building.get("interior", {}) if isinstance(building, dict) else {}
    b_construction = building.get("construction", {}) if isinstance(building, dict) else {}
    b_parking = building.get("parking", {}) if isinstance(building, dict) else {}
    b_heating = building.get("heating", {}) if isinstance(building, dict) else {}

    lot = p_prop.get("lot", {})
    location = p_prop.get("area", {})
    address = p_prop.get("address", {})
    vintage = p_prop.get("vintage", {})
    summary_p = p_prop.get("summary", {})

    # Sale history (may be in property detail)
    sale = p_prop.get("sale", {})
    sale_history = p_prop.get("salehistory", [])

    # Owner info
    owner = p_prop.get("owner", {})

    # Build comprehensive result
    result = {
        "stub": False,
        "tax": {
            "taxamt": tax.get("taxamt"),
            "taxyear": tax.get("taxyear"),
            "taxpersizeunit": tax.get("taxpersizeunit"),
        },
        "assessed": {
            "total": assessed.get("assdttlvalue"),
            "land": assessed.get("assdlandvalue"),
            "improvements": assessed.get("assdimprvalue"),
        },
        "market": {
            "total": market.get("mktttlvalue"),
            "land": market.get("mktlandvalue"),
            "improvements": market.get("mktimprvalue"),
        },
        "property": {
            "yearbuilt": b_summary.get("yearbuilt") or vintage.get("lastModified"),
            "sqft": b_size.get("livingsize") or b_size.get("universalsize") or b_summary.get("sizeInd"),
            "bedrooms": b_rooms.get("beds") or b_rooms.get("bathstotal"),
            "bathrooms": b_rooms.get("bathsfull"),
            "bathshalf": b_rooms.get("bathshalf"),
            "stories": b_summary.get("stories"),
            "units": b_summary.get("unitsCount"),
            "condition": b_construction.get("condition"),
            "rooftype": b_construction.get("roofcover"),
            "roofmaterial": b_construction.get("roofShape"),
            "walltype": b_construction.get("wallType"),
            "foundation": b_construction.get("foundationType"),
            "heating": b_heating.get("heattype"),
            "cooling": b_heating.get("actype"),
            "fireplace": b_interior.get("fplccount"),
            "pool": lot.get("pooltype"),
            "parking": b_parking.get("prkgType"),
            "garagesize": b_parking.get("prkgSize"),
            "propertytype": summary_p.get("propclass") or summary_p.get("proptype"),
            "propsubtype": summary_p.get("propsubtype"),
        },
        "lot": {
            "size_sqft": lot.get("lotsize2"),
            "size_acres": lot.get("lotsize1"),
            "depth": lot.get("depth"),
            "frontage": lot.get("frontage"),
            "lotnum": lot.get("lotnum"),
            "zoning": lot.get("zoningType"),
        },
        "location": {
            "county": location.get("countrysecsubd"),
            "subdivision": location.get("subdname"),
            "taxcodearea": location.get("taxcodearea"),
            "census_tract": location.get("censustractident"),
            "legal": p_prop.get("legal", {}),
        },
        "owner": {
            "name": f"{owner.get('owner1', {}).get('firstnameandmi', '')} {owner.get('owner1', {}).get('lastnameorsinglename', '')}".strip() if isinstance(owner.get("owner1"), dict) else None,
            "mailingaddr": owner.get("mailingaddressoneline"),
            "ownertype": owner.get("corporateindicator"),
        },
        "sale": {
            "last_sale_date": sale.get("saleTransDate") or sale.get("salesearchdate"),
            "last_sale_price": sale.get("saleamt") or sale.get("saledisclosuretype"),
        },
        "sale_history": [
            {
                "date": sh.get("saleTransDate") or sh.get("salesearchdate"),
                "price": sh.get("saleamt"),
                "type": sh.get("saledisclosuretype"),
            }
            for sh in (sale_history if isinstance(sale_history, list) else [])
        ][:5],  # last 5 sales
        "address": {
            "oneline": address.get("oneLine"),
            "line1": address.get("line1"),
            "line2": address.get("line2"),
            "locality": address.get("locality"),
            "countrySubd": address.get("countrySubd"),
            "postal1": address.get("postal1"),
        },
        "_debug_summary": b_summary,
        "_debug_prop_summary": summary_p,
        "_debug_building_size": b_size,
        "_debug_vintage": vintage,
        "_raw_keys": {
            "assessment": list(assessment.keys()) if assessment else [],
            "building": list(building.keys()) if isinstance(building, dict) else [],
            "prop_root": list(p_prop.keys()) if p_prop else [],
        },
    }

    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype="application/json",
    )
