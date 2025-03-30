import os
import json
from email.mime import image
from io import BytesIO

import azure.functions as func
import requests
from PIL import Image
from azure.storage.blob import BlobServiceClient
from iptcinfo3 import IPTCInfo

import tempfile

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def get_file(container, path):
    """Fetch image from Azure Blob Storage and retain IPTC metadata"""
    # Get connection string from environment variable
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set")

    # Create a BlobServiceClient
    service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = service_client.get_container_client(container)
    blob_client = container_client.get_blob_client(path)

    # Download blob data as bytes
    blob_data = blob_client.download_blob()
    blob_bytes = blob_data.readall()

    # Write blob bytes to a temporary file to preserve metadata
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
        temp_file.write(blob_bytes)
        temp_file_path = temp_file.name
        print(f"Saved temporary file to {temp_file_path}")

    # Load IPTC metadata from the temporary file
    try:
        print("Loading IPTC Info")
        info = IPTCInfo(temp_file_path, force=True)
        print("Loaded IPTC Info:", info)

    except Exception as e:
        print(f"Error loading IPTC info: {e}")
        info = None

    # Load the image into PIL after extracting metadata
    image = Image.open(BytesIO(blob_bytes))
    print(f"Image size: {image.size}")

    # Clean up the temporary file after reading metadata
    os.remove(temp_file_path)

    return image, info


def detect(_image: Image):
    endpoint = os.getenv("DETECT_ENDPOINT")
    if not endpoint:
        raise ValueError("DETECT_ENDPOINT is not set")

    img_byte_arr = BytesIO()
    _image.save(img_byte_arr, format=_image.format)  # Preserve original format (e.g., JPEG/PNG)
    img_bytes = img_byte_arr.getvalue()
    response = requests.post(endpoint, files={'file': img_bytes})
    if response.status_code == 200:
        content = json.loads(response.text)
        detections = content["response"]["extractedImages"]
        return detections
    return []


def get_iptc_field(_image: Image, tag: int):
    print(_image)
    print(f"Looking for IPTC data for tag {tag}")
    if "iptc" in _image.info:
        print("IPTC Data found")
        iptc_data = _image.info["iptc"]
        if tag in iptc_data:
            caption = iptc_data[tag].decode("utf-8", errors="ignore")
            return caption
    return None


def get_tag_for_field(field: str) -> int:
    print("Looking for tag corresponding to {}".format(field))
    for k, v in IPTC_TAGS.items():
        if field.lower() in v.lower():
            print(f"Found {field} in {v}. Tag: {k}")
            return k
    return -1


def get_id(_image: Image, path: str, info: IPTCInfo, id_field=None, folder_id_idx=None):
    print(f"Getting ID")
    if id_field == "folder":
        print(f"Using Folder ID structure")
        if folder_id_idx is None:
            return None
        return path.split("/")[int(folder_id_idx)]

    print(f"Trying to get ID from IPTC data based on {id_field}")
    tag = get_tag_for_field(id_field)
    if tag == -1:
        return None
    print(f"Using Tag {tag}")
    try:
        identifier = info[tag].decode("utf-8")
    except Exception as e:
        print(f"Error getting ID from IPTC data: {e}")
        identifier = None
    return identifier


@app.route(route="process_file", methods=["GET"])
def process_file_function(req: func.HttpRequest) -> func.HttpResponse:
    try:
        print("Params:", req.params)
        # Get query parameters
        container = req.params.get("container")
        path = req.params.get("path")
        id_field = req.params.get("id_field")
        folder_id_idx = req.params.get("folder_id_idx")
        # Validate query parameters
        if not container or not path:
            return func.HttpResponse(
                json.dumps({"error": "Missing required query parameters. Expected: container, path."}),
                status_code=400,
                mimetype="application/json"
            )

        # Fetch file paths
        _image, _iptc = get_file(container, path)
        detections = detect(_image)
        identifier = get_id(_image, path, _iptc, id_field, folder_id_idx)

        return func.HttpResponse(
            json.dumps({"container": container, "path": path, "detections": detections, "identifier": identifier}),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


IPTC_TAGS = {
    5: "Object Name",                  # Title or Name of the object
    7: "Edit Status",                  # Indicates editing information
    8: "Editorial Update",             # Date and time of the last edit
    10: "Urgency",                     # Urgency of the content (0–9 scale)
    12: "Subject Reference",           # Subject or category reference
    15: "Category",                    # Category of the content
    20: "Supplemental Category",       # Additional category information
    22: "Fixture Identifier",          # Identifier for fixture
    25: "Keywords",                    # Keywords describing the content
    30: "Release Date",                # Date of the media release
    35: "Release Time",                # Time of the media release
    40: "Special Instructions",        # Special usage or handling instructions
    45: "Reference Service",           # Service reference
    47: "Reference Date",              # Date reference
    50: "Reference Number",            # Reference number
    55: "Created Date",                # Creation date of the content
    60: "Created Time",                # Creation time of the content
    65: "Originating Program",         # Program used to originate the object
    70: "Program Version",             # Version of the originating program
    75: "Object Cycle",                # Cycle of the object
    80: "Byline",                      # Name of the author/photographer
    85: "Byline Title",                # Title of the author/photographer
    90: "City",                        # City where the content was created
    92: "Sublocation",                 # More specific location within the city
    95: "State/Province",              # State or province of the location
    100: "Country Code",               # ISO country code
    101: "Country Name",               # Full name of the country
    103: "Original Transmission Reference",  # Reference for transmission
    105: "Headline",                   # Short headline for the object
    110: "Credit",                     # Provider of the content
    115: "Source",                     # Original source of the content
    116: "Copyright Notice",           # Copyright information
    118: "Contact",                    # Contact details
    120: "caption",           # Description or caption
    121: "Local Caption",              # Localized caption
    122: "Writer/Editor",              # Name of the writer/editor
    130: "Image Type",                 # Type of image
    131: "Image Orientation",          # Orientation of the image
    135: "Language Identifier",        # Language of the content
    150: "Audio Type",                 # Type of audio
    151: "Audio Sampling Rate",        # Sampling rate of audio
    152: "Audio Sampling Resolution",  # Resolution of audio
    153: "Audio Duration",             # Duration of audio
    154: "Audio Outcue",               # Audio outcue
    184: "Job Identifier",             # Job or project identifier
    187: "Master Document Identifier", # Master document identifier
    188: "Short Document Identifier",  # Short document identifier
    189: "Unique Document Identifier", # Unique document identifier
    190: "Owner ID",                   # Owner identifier
    221: "Object Preview Data",        # Preview data for the object
    225: "Classified Indicator",       # Classification indicator
    230: "Person Shown",               # Name(s) of persons shown
    231: "Location Shown",             # Name(s) of locations shown
    232: "Organization Shown",         # Name(s) of organizations shown
    240: "Content Description",        # Description of the content
    242: "Data Source",                # Source of data
    255: "Rasterized Caption"          # Caption in raster format
}