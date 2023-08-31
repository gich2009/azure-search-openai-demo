import io
import logging
import mimetypes
import os
import time

import aiohttp
import openai
from azure.identity.aio import DefaultAzureCredential
from azure.monitor.opentelemetry import configure_azure_monitor
from azure.search.documents.aio import SearchClient
from azure.storage.blob.aio import BlobServiceClient
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from quart import (
    Blueprint,
    Quart,
    abort,
    current_app,
    jsonify,
    request,
    send_file,
    send_from_directory,
)

from indexer import add_file

from approaches.chatreadretrieveread import ChatReadRetrieveReadApproach
from approaches.readdecomposeask import ReadDecomposeAsk
from approaches.readretrieveread import ReadRetrieveReadApproach
from approaches.retrievethenread import RetrieveThenReadApproach

CONFIG_OPENAI_TOKEN = "openai_token"
CONFIG_CREDENTIAL = "azure_credential"
CONFIG_ASK_APPROACHES = "ask_approaches"
CONFIG_CHAT_APPROACHES = "chat_approaches"
CONFIG_BLOB_CONTAINER_CLIENT = "blob_container_client"
glob_blob_container_clients: dict = {}
glob_search_clients: dict = {}

bp = Blueprint("routes", __name__, static_folder='static')

@bp.route("/")
async def index():
    return await bp.send_static_file("index.html")

@bp.route("/favicon.ico")
async def favicon():
    return await bp.send_static_file("favicon.ico")

@bp.route("/assets/<path:path>")
async def assets(path):
    return await send_from_directory("static/assets", path)

# Serve content files from blob storage from within the app to keep the example self-contained.
# *** NOTE *** this assumes that the content files are public, or at least that all users of the app
# can access all the files. This is also slow and memory hungry.
@bp.route("/content/<index_name>/<path>")
async def content_file(index_name, path):
    print("Index name", index_name)
    print("Path", path)
    blob_container_clients = current_app.config[CONFIG_BLOB_CONTAINER_CLIENT]
    blob_container_client = blob_container_clients.get(index_name)
    
    if not blob_container_client:
        return jsonify({"error": "unknown index_name for blob container"}), 400

    blob = await blob_container_client.get_blob_client(path).download_blob()
    
    if not blob.properties or not hasattr(blob.properties, "content_settings"):
        abort(404)
    
    mime_type = blob.properties["content_settings"]["content_type"]
    if mime_type == "application/octet-stream":
        mime_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
    
    blob_file = io.BytesIO()
    await blob.readinto(blob_file)
    blob_file.seek(0)
    return await send_file(blob_file, mimetype=mime_type, as_attachment=False, attachment_filename=path)



@bp.route("/upload", methods=["POST"])
async def upload_file():
    # Check if a file part is present in the request
    try:
        uploaded_file = request.files.get("file")
    except Exception as error:
        print(error)
        return jsonify({"error": error}), 500
    
    if not uploaded_file:
        return jsonify({"error": "No file provided."}), 400

    # Check if the file has a name
    if uploaded_file.filename == "":
        return jsonify({"error": "No selected file."}), 400

    index_name: str = request.args.get('index_name', 'natural-capital')
    
    upload_successful: bool = await add_file(uploaded_file=uploaded_file, index=index_name)
   
    if upload_successful:
        return jsonify({"success": "File uploaded successfully!"}), 200
    else:
        return jsonify({"error": "Failed to upload file"}), 501

    # Save the file to Azure Blob Storage
    # blob_client = blob_container_client.get_blob_client(uploaded_file.filename)
    # await blob_client.upload_blob(uploaded_file.read(), overwrite=True)

    # Provide the desired blob container (assuming natural-capital as default)
    # index_name: str = request.args.get('index_name', 'natural-capital')
    # blob_container_clients = current_app.config[CONFIG_BLOB_CONTAINER_CLIENT]
    # blob_container_client = blob_container_clients.get(index_name)
    # if not blob_container_client:
    #     return jsonify({"error": "unknown index_name for blob container"}), 400


@bp.route("/ask", methods=["POST"])
async def ask():
    if not request.is_json:
        return jsonify({"error": "request must be json"}), 415
    request_json = await request.get_json()
    approach = request_json["approach"]
    
    # Obtain the overridden index_name, if provided.
    overrides = request_json.get("overrides") or {}
    index_name = overrides.get("index_name") or 'natural-capital'

    try:
        impl = None
        # Check if an index_name override is provided and get the specific implementation for that.
        print("This is the index name: ", index_name)
        if index_name:
            impl = current_app.config[CONFIG_ASK_APPROACHES].get(index_name, {}).get(approach)
        else:
            for index_impls in current_app.config[CONFIG_ASK_APPROACHES].values():
                if approach in index_impls:
                    impl = index_impls[approach]
                    break

        print("This is the implementation: ", impl)
        if not impl:
            return jsonify({"error": "unknown approach or index_name"}), 400
        
        # Workaround for: https://github.com/openai/openai-python/issues/371
        async with aiohttp.ClientSession() as s:
            openai.aiosession.set(s)
            r = await impl.run(request_json["question"], overrides)
        return jsonify(r)
    except Exception as e:
        logging.exception("Exception in /ask")
        return jsonify({"error": str(e)}), 500


@bp.route("/chat", methods=["POST"])
async def chat():
    if not request.is_json:
        return jsonify({"error": "request must be json"}), 415
    request_json = await request.get_json()
    approach = request_json["approach"]

    # Obtain the overridden index_name, if provided.
    overrides = request_json.get("overrides") or {}
    index_name = overrides.get("index_name") or 'natural-capital'

    try:
        impl = None
        # For the chat approach, only one primary index has been setup. We can expand this in a similar manner as the ask route if needed.
        if index_name:
            impl = current_app.config[CONFIG_CHAT_APPROACHES].get(index_name, {}).get(approach)
        else:
            impl = current_app.config[CONFIG_CHAT_APPROACHES].get(approach)

        if not impl:
            return jsonify({"error": "unknown approach or index_name"}), 400
        
        # Workaround for: https://github.com/openai/openai-python/issues/371
        async with aiohttp.ClientSession() as s:
            openai.aiosession.set(s)
            r = await impl.run(request_json["history"], overrides)
        return jsonify(r)
    except Exception as e:
        logging.exception("Exception in /chat")
        return jsonify({"error": str(e)}), 500


@bp.before_request
async def ensure_openai_token():
    openai_token = current_app.config[CONFIG_OPENAI_TOKEN]
    if openai_token.expires_on < time.time() + 60:
        openai_token = await current_app.config[CONFIG_CREDENTIAL].get_token("https://cognitiveservices.azure.com/.default")
        current_app.config[CONFIG_OPENAI_TOKEN] = openai_token
        openai.api_key = openai_token.token

@bp.before_app_serving
async def setup_clients():
    # Fetch environment variables or use default values.
    AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
    AZURE_STORAGE_CONTAINER = ["natural-capital", "energy", "climate-financing", "green-minerals", "sust-agric", "adaptation", "infrastructure"]
    AZURE_SEARCH_SERVICE = os.getenv("AZURE_SEARCH_SERVICE")
    AZURE_SEARCH_INDICES = ["natural-capital", "energy", "climate-financing", "green-minerals", "sust-agric", "adaptation", "infrastructure"]
    AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")
    AZURE_OPENAI_CHATGPT_DEPLOYMENT = os.getenv("AZURE_OPENAI_CHATGPT_DEPLOYMENT")
    AZURE_OPENAI_CHATGPT_MODEL = os.getenv("AZURE_OPENAI_CHATGPT_MODEL")
    AZURE_OPENAI_EMB_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMB_DEPLOYMENT")

    KB_FIELDS_CONTENT = os.getenv("KB_FIELDS_CONTENT", "content")
    KB_FIELDS_SOURCEPAGE = os.getenv("KB_FIELDS_SOURCEPAGE", "sourcepage")

    # Set up Azure authentication.
    azure_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)

    # Set up Blob Storage clients.
    blob_client = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=azure_credential
    )

    blob_container_clients = {}
    for container in AZURE_STORAGE_CONTAINER:
        blob_container_clients[container] = blob_client.get_container_client(container)

    # Set up Search clients for multiple indices.
    search_clients = {}
    for index_name in AZURE_SEARCH_INDICES:
        search_clients[index_name] = SearchClient(
            endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net",
            index_name=index_name,
            credential=azure_credential
        )

    # Setup OpenAI
    openai.api_base = f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com"
    openai.api_version = "2023-05-15"
    openai.api_type = "azure_ad"
    openai_token = await azure_credential.get_token(
        "https://cognitiveservices.azure.com/.default"
    )
    openai.api_key = openai_token.token

    # Store some configuration data for use in later requests.
    current_app.config[CONFIG_OPENAI_TOKEN] = openai_token
    current_app.config[CONFIG_CREDENTIAL] = azure_credential
    current_app.config[CONFIG_BLOB_CONTAINER_CLIENT] = blob_container_clients

    # Update the approaches to integrate GPT with external knowledge.
    current_app.config[CONFIG_ASK_APPROACHES] = {
        index_name: {
            "rtr": RetrieveThenReadApproach(
                search_clients[index_name],
                AZURE_OPENAI_CHATGPT_DEPLOYMENT,
                AZURE_OPENAI_CHATGPT_MODEL,
                AZURE_OPENAI_EMB_DEPLOYMENT,
                KB_FIELDS_SOURCEPAGE,
                KB_FIELDS_CONTENT
            ),
            "rrr": ReadRetrieveReadApproach(
                search_clients[index_name],
                AZURE_OPENAI_CHATGPT_DEPLOYMENT,
                AZURE_OPENAI_EMB_DEPLOYMENT,
                KB_FIELDS_SOURCEPAGE,
                KB_FIELDS_CONTENT
            ),
            "rda": ReadDecomposeAsk(
                search_clients[index_name],
                AZURE_OPENAI_CHATGPT_DEPLOYMENT,
                AZURE_OPENAI_EMB_DEPLOYMENT,
                KB_FIELDS_SOURCEPAGE,
                KB_FIELDS_CONTENT
            )
        }
        for index_name in AZURE_SEARCH_INDICES
    }

    current_app.config[CONFIG_CHAT_APPROACHES] = {
        index_name: {
            "rrr": ChatReadRetrieveReadApproach(
                search_clients[index_name],
                AZURE_OPENAI_CHATGPT_DEPLOYMENT,
                AZURE_OPENAI_CHATGPT_MODEL,
                AZURE_OPENAI_EMB_DEPLOYMENT,
                KB_FIELDS_SOURCEPAGE,
                KB_FIELDS_CONTENT
            )
        }
        for index_name in AZURE_SEARCH_INDICES
    }

def create_app():
    if os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"):
        configure_azure_monitor()
        AioHttpClientInstrumentor().instrument()
    app = Quart(__name__)
    app.register_blueprint(bp)
    app.asgi_app = OpenTelemetryMiddleware(app.asgi_app)

    return app


# @bp.route("/chat", methods=["POST"])
# async def chat():
#     if not request.is_json:
#         return jsonify({"error": "request must be json"}), 415
#     request_json = await request.get_json()
#     approach = request_json["approach"]
#     try:
#         impl = current_app.config[CONFIG_CHAT_APPROACHES].get(approach)
#         if not impl:
#             return jsonify({"error": "unknown approach"}), 400
#         # Workaround for: https://github.com/openai/openai-python/issues/371
#         async with aiohttp.ClientSession() as s:
#             openai.aiosession.set(s)
#             r = await impl.run(request_json["history"], request_json.get("overrides") or {})
#         return jsonify(r)
#     except Exception as e:
#         logging.exception("Exception in /chat")
#         return jsonify({"error": str(e)}), 500






    # # Set up Blob Storage clients.
    # blob_client = BlobServiceClient(
    #     account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
    #     credential=azure_credential
    # )
    # blob_container_client = blob_client.get_container_client(AZURE_STORAGE_CONTAINER)
