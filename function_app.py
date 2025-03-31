import base64
import io
import os
import json
from email.mime import image
from io import BytesIO
from typing import List

import azure.functions as func
import requests
from PIL import Image
from azure.storage.blob import BlobServiceClient
from iptcinfo3 import IPTCInfo

import tempfile

# Create a Function App with HTTP trigger and function-level authorization
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def get_file(container, path, connection_string_env_var: str):
    """
    Fetch an image from Azure Blob Storage and retain IPTC metadata.

    Args:
        container (str): Name of the Azure Blob Storage container.
        path (str): Path to the blob in the container.
        connection_string_env_var (str): Name of the environment variable holding the connection string.

    Returns:
        tuple: A tuple containing:
            - PIL.Image: The image loaded using PIL.
            - IPTCInfo: The IPTC metadata, if available.
    """
    # Get connection string from environment variable
    connection_string = os.getenv(connection_string_env_var)
    if not connection_string:
        raise ValueError(f"{connection_string_env_var} is not set")

    # Create a BlobServiceClient to connect to the blob storage account
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
        info = IPTCInfo(temp_file_path, force=True)  # Force reading even if incomplete
        print("Loaded IPTC Info:", info)
    except Exception as e:
        print(f"Error loading IPTC info: {e}")
        info = None

    # Load the image into PIL after extracting metadata
    img = Image.open(BytesIO(blob_bytes))
    print(f"Image size: {img.size}")

    # Clean up the temporary file after reading metadata
    os.remove(temp_file_path)

    return img, info


def detect(_image: Image):
    """
    Send an image to a detection endpoint to extract bounding boxes.

    Args:
        _image (PIL.Image): The image to be sent to the detection API.

    Returns:
        list: List of extracted image detections if available, otherwise an empty list.
    """
    endpoint = os.getenv("DETECT_ENDPOINT")
    if not endpoint:
        raise ValueError("DETECT_ENDPOINT is not set")

    # Convert PIL image to bytes and preserve the original format
    img_byte_arr = BytesIO()
    _image.save(img_byte_arr, format=_image.format)
    img_bytes = img_byte_arr.getvalue()

    # Send the image to the detection API endpoint
    response = requests.post(endpoint, files={'file': img_bytes})

    # If the request succeeds, parse the response
    if response.status_code == 200:
        content = json.loads(response.text)
        detections = content["response"]["extractedImages"]
        return detections

    # Return an empty list if detection failed
    return []


def get_iptc_field(_image: Image, tag: int):
    """
    Extract an IPTC metadata field from a PIL image.

    Args:
        _image (PIL.Image): The image to extract metadata from.
        tag (int): IPTC tag number.

    Returns:
        str | None: The metadata value if found, otherwise None.
    """
    print(f"Looking for IPTC data for tag {tag}")
    if "iptc" in _image.info:
        iptc_data = _image.info["iptc"]
        if tag in iptc_data:
            # Decode and return IPTC data if found
            caption = iptc_data[tag].decode("utf-8", errors="ignore")
            return caption
    return None


def get_tag_for_field(field: str) -> int:
    """
    Get the IPTC tag corresponding to a given field name.

    Args:
        field (str): The name of the IPTC field.

    Returns:
        int: Corresponding tag number, or -1 if the field is not found.
    """
    print(f"Looking for tag corresponding to {field}")
    for k, v in IPTC_TAGS.items():
        if field.lower() in v.lower():
            print(f"Found {field} in {v}. Tag: {k}")
            return k
    return -1


def get_id(_image: Image, path: str, info: IPTCInfo, id_field=None, folder_id_idx=None):
    """
    Retrieve the unique identifier for the image based on IPTC metadata or folder structure.

    Args:
        _image (PIL.Image): The image to extract ID from.
        path (str): The path to the image in the blob storage.
        info (IPTCInfo): The IPTC metadata extracted from the image.
        id_field (str, optional): The field name to use for retrieving the ID.
        folder_id_idx (int, optional): Index of folder name to use as ID.

    Returns:
        str | None: The identifier, if available.
    """
    if id_field == "folder":
        if folder_id_idx is not None:
            return path.split("/")[int(folder_id_idx)]
        return None

    # Get the tag for the specified field
    tag = get_tag_for_field(id_field)
    if tag == -1:
        return None

    # Try to retrieve the ID from IPTC data
    try:
        identifier = info[tag].decode("utf-8")
    except Exception as e:
        print(f"Error getting ID from IPTC data: {e}")
        identifier = None

    return identifier


def load_image_from_base64(base64_string):
    """
    Decode a base64 string and return a PIL image.

    Args:
        base64_string (str): The base64-encoded string of the image.

    Returns:
        PIL.Image: The decoded image.
    """
    image_data = base64.b64decode(base64_string)
    _image = Image.open(BytesIO(image_data))
    return _image


def write_output(
        source: str,
        connection_string_env_variable: str,
        container: str,
        folder: str,
        detections: List[str],
        identifier: str | None
):
    """
    Write extracted image detections to Azure Blob Storage.

    Args:
        source (str): The original image path.
        connection_string_env_variable (str): Environment variable containing the connection string.
        container (str): Name of the target Azure Blob Storage container.
        folder (str): Target folder to store cropped detections.
        detections (List[str]): List of extracted detections.
        identifier (str | None): The identifier for the image.

    Returns:
        list: Paths where cropped images were stored.
    """
    if identifier is None:
        return

    # Get connection string from environment variable
    connection_string = os.getenv(connection_string_env_variable)
    if not connection_string:
        raise ValueError(f"{connection_string_env_variable} is not set")

    # Create a BlobServiceClient and get container client
    service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = service_client.get_container_client(container)

    paths = []
    for idx, detection in enumerate(detections):
        # Generate unique file path for cropped image
        path_basename = os.path.basename(source).split(".")[0] + f"_cropped_{idx}.JPG"
        path = os.path.join(folder, identifier, path_basename)
        blob_client = container_client.get_blob_client(path)

        # Load cropped image from base64 string
        img = load_image_from_base64(detection)
        img_data = get_image_data(img)

        # Upload image to Azure Blob Storage
        blob_client.upload_blob(img_data, overwrite=True)
        print(f"Image persisted at {path}")
        paths.append(path)

    return paths


def get_image_data(img: Image):
    """
    Convert a PIL image to binary data.

    Args:
        img (PIL.Image): The image to convert.

    Returns:
        BytesIO: Binary data of the image.
    """
    fp = io.BytesIO()
    img.save(fp, format="JPEG")
    fp.seek(0)
    return fp


@app.route(route="process_file", methods=["GET"])
def process_file_function(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main HTTP-triggered function to process an image and return detections.

    Query Parameters:
        - container: Source container
        - path: Path to the image in the container
        - id_field: Field to extract the identifier
        - folder_id_idx: Index of folder name to use as identifier
        - con_env_in: Environment variable with input connection string
        - con_env_out: Environment variable with output connection string
        - container_out: Target container for detections
        - folder_out: Target folder for detections

    Returns:
        HttpResponse: JSON response with results.
    """
    try:
        print("Params:", req.params)

        # Extract query parameters
        container = req.params.get("container")
        path = req.params.get("path")
        id_field = req.params.get("id_field")
        folder_id_idx = req.params.get("folder_id_idx")
        connection_string_input_env_var = req.params.get("con_env_in")
        connection_string_output_env_var = req.params.get("con_env_out")
        container_out = req.params.get("container_out")
        folder_out = req.params.get("folder_out")

        # Validate required query parameters
        if not container or not path:
            return func.HttpResponse(
                json.dumps({"error": "Missing required query parameters. Expected: container, path."}),
                status_code=400,
                mimetype="application/json"
            )

        # Fetch image and IPTC metadata from Azure Blob Storage
        _image, _iptc = get_file(container, path, connection_string_input_env_var)

        # Detect objects in the image
        detections = detect(_image)

        # Get unique identifier based on folder or metadata
        identifier = get_id(_image, path, _iptc, id_field, folder_id_idx)

        # Write cropped detections to output storage
        output_paths = write_output(
            detections=detections,
            container=container_out,
            identifier=identifier,
            folder=folder_out,
            connection_string_env_variable=connection_string_output_env_var,
            source=path
        )

        # Return successful response
        return func.HttpResponse(
            json.dumps({
                "container": container,
                "path": path,
                "detections": detections,
                "identifier": identifier,
                "output_paths": output_paths
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        # Return error response
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