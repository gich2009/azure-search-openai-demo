import argparse
import base64
import glob
import html
import io
import os
import re
import time

import openai
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.identity import AzureDeveloperCliCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswParameters,
    PrioritizedFields,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SemanticConfiguration,
    SemanticField,
    SemanticSettings,
    SimpleField,
    VectorSearch,
    VectorSearchAlgorithmConfiguration,
)
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pypdf import PdfReader, PdfWriter
from tenacity import retry, stop_after_attempt, wait_random_exponential

MAX_SECTION_LENGTH = 1000
SENTENCE_SEARCH_LIMIT = 100
SECTION_OVERLAP = 100

open_ai_token_cache = {}
CACHE_KEY_TOKEN_CRED = 'openai_token_cred'
CACHE_KEY_CREATED_TIME = 'created_time'
CACHE_KEY_TOKEN_TYPE = 'token_type'

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_CONTAINER = ["natural-capital", "energy", "climate-financing", "green-minerals", "sust-agric", "adaptation", "infrastructure"]
AZURE_SEARCH_SERVICE = os.getenv("AZURE_SEARCH_SERVICE")
AZURE_SEARCH_INDICES = ["natural-capital", "energy", "climate-financing", "green-minerals", "sust-agric", "adaptation", "infrastructure"]
AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")
AZURE_OPENAI_CHATGPT_DEPLOYMENT = os.getenv("AZURE_OPENAI_CHATGPT_DEPLOYMENT")
AZURE_OPENAI_CHATGPT_MODEL = os.getenv("AZURE_OPENAI_CHATGPT_MODEL")
AZURE_OPENAI_EMB_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMB_DEPLOYMENT")
AZURE_TENANT_ID=os.getenv("AZURE_TENANT_ID")



def blob_name_from_file_page(filename, page = 0):
    if os.path.splitext(filename)[1].lower() == ".pdf":
        return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"
    else:
        return os.path.basename(filename)


def upload_blobs(uploaded_file, container_name, storage_creds, verbose=True):
    blob_service = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(container_name)
    
    # Check if the container exists, if not, create it
    if not blob_container.exists():
        blob_container.create_container()

    file_content = uploaded_file.read()
    file_name = uploaded_file.filename
    file_extension = os.path.splitext(file_name)[1].lower()

    if file_extension == ".pdf":
        # Convert BytesIO stream to a PdfReader object
        reader = PdfReader(io.BytesIO(file_content))
        pages = reader.pages
        for i in range(len(pages)):
            blob_name = blob_name_from_file_page(file_name, i)
            if verbose: 
                print(f"\tUploading blob for page {i} -> {blob_name}")
            f = io.BytesIO()
            writer = PdfWriter()
            writer.add_page(pages[i])
            writer.write(f)
            f.seek(0)
            blob_container.upload_blob(blob_name, f, overwrite=True)
    else:
        blob_name = blob_name_from_file_page(file_name)
        blob_container.upload_blob(blob_name, io.BytesIO(file_content), overwrite=True)




def remove_blobs(filename, storage_creds, container, container_name=None, verbose=True):
    if verbose: 
        print(f"Removing blobs for '{filename or '<all>'}'")
    
    blob_service = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=storage_creds)
    
    if container_name is None:
        container_name = container

    blob_container = blob_service.get_container_client(container_name)
    if blob_container.exists():
        if filename is None:
            blobs = blob_container.list_blob_names()
        else:
            prefix = os.path.splitext(os.path.basename(filename))[0]
            blobs = filter(lambda b: re.match(f"{prefix}-\d+\.pdf", b), blob_container.list_blob_names(name_starts_with=os.path.splitext(os.path.basename(prefix))[0]))
        
        for b in blobs:
            if verbose: 
                print(f"\tRemoving blob {b}")
            blob_container.delete_blob(b)



def table_to_html(table):
    table_html = "<table>"
    rows = [sorted([cell for cell in table.cells if cell.row_index == i], key=lambda cell: cell.column_index) for i in range(table.row_count)]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag = "th" if (cell.kind == "columnHeader" or cell.kind == "rowHeader") else "td"
            cell_spans = ""
            if cell.column_span > 1: cell_spans += f" colSpan={cell.column_span}"
            if cell.row_span > 1: cell_spans += f" rowSpan={cell.row_span}"
            table_html += f"<{tag}{cell_spans}>{html.escape(cell.content)}</{tag}>"
        table_html +="</tr>"
    table_html += "</table>"
    return table_html


def get_document_text(uploaded_file):
    offset = 0
    page_map = []
    file_content = uploaded_file.read()  # read the content once
    file_name = uploaded_file.filename
    
    #check if localpdfparser
    reader = PdfReader(io.BytesIO(file_content))
    pages = reader.pages
    for page_num, p in enumerate(pages):
        page_text = p.extract_text()
        page_map.append((page_num, offset, page_text))
        offset += len(page_text)
    # else:
    #     if args.verbose: 
    #         print(f"Extracting text from '{file_name}' using Azure Form Recognizer")
        
    #     form_recognizer_client = DocumentAnalysisClient(endpoint=f"https://{args.formrecognizerservice}.cognitiveservices.azure.com/", 
    #                                                     credential=formrecognizer_creds, 
    #                                                     headers={"x-ms-useragent": "azure-search-chat-demo/1.0.0"})
    #     poller = form_recognizer_client.begin_analyze_document("prebuilt-layout", document=io.BytesIO(file_content))
    #     form_recognizer_results = poller.result()

    #     for page_num, page in enumerate(form_recognizer_results.pages):
    #         tables_on_page = [table for table in form_recognizer_results.tables if table.bounding_regions[0].page_number == page_num + 1]

    #         # mark all positions of the table spans in the page
    #         page_offset = page.spans[0].offset
    #         page_length = page.spans[0].length
    #         table_chars = [-1]*page_length
    #         for table_id, table in enumerate(tables_on_page):
    #             for span in table.spans:
    #                 # replace all table spans with "table_id" in table_chars array
    #                 for i in range(span.length):
    #                     idx = span.offset - page_offset + i
    #                     if idx >=0 and idx < page_length:
    #                         table_chars[idx] = table_id

    #         # build page text by replacing characters in table spans with table html
    #         page_text = ""
    #         added_tables = set()
    #         for idx, table_id in enumerate(table_chars):
    #             if table_id == -1:
    #                 page_text += form_recognizer_results.content[page_offset + idx]
    #             elif table_id not in added_tables:
    #                 page_text += table_to_html(tables_on_page[table_id])
    #                 added_tables.add(table_id)

    #         page_text += " "
    #         page_map.append((page_num, offset, page_text))
    #         offset += len(page_text)

    return page_map


def split_text(page_map, uploaded_file, verbose=True):
    filename=uploaded_file.filename
    SENTENCE_ENDINGS = [".", "!", "?"]
    WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]
    if verbose: print(f"Splitting '{filename}' into sections")

    def find_page(offset):
        num_pages = len(page_map)
        for i in range(num_pages - 1):
            if offset >= page_map[i][1] and offset < page_map[i + 1][1]:
                return i
        return num_pages - 1

    all_text = "".join(p[2] for p in page_map)
    length = len(all_text)
    start = 0
    end = length
    while start + SECTION_OVERLAP < length:
        last_word = -1
        end = start + MAX_SECTION_LENGTH

        if end > length:
            end = length
        else:
            # Try to find the end of the sentence
            while end < length and (end - start - MAX_SECTION_LENGTH) < SENTENCE_SEARCH_LIMIT and all_text[end] not in SENTENCE_ENDINGS:
                if all_text[end] in WORDS_BREAKS:
                    last_word = end
                end += 1
            if end < length and all_text[end] not in SENTENCE_ENDINGS and last_word > 0:
                end = last_word # Fall back to at least keeping a whole word
        if end < length:
            end += 1

        # Try to find the start of the sentence or at least a whole word boundary
        last_word = -1
        while start > 0 and start > end - MAX_SECTION_LENGTH - 2 * SENTENCE_SEARCH_LIMIT and all_text[start] not in SENTENCE_ENDINGS:
            if all_text[start] in WORDS_BREAKS:
                last_word = start
            start -= 1
        if all_text[start] not in SENTENCE_ENDINGS and last_word > 0:
            start = last_word
        if start > 0:
            start += 1

        section_text = all_text[start:end]
        yield (section_text, find_page(start))

        last_table_start = section_text.rfind("<table")
        if (last_table_start > 2 * SENTENCE_SEARCH_LIMIT and last_table_start > section_text.rfind("</table")):
            # If the section ends with an unclosed table, we need to start the next section with the table.
            # If table starts inside SENTENCE_SEARCH_LIMIT, we ignore it, as that will cause an infinite loop for tables longer than MAX_SECTION_LENGTH
            # If last table starts inside SECTION_OVERLAP, keep overlapping
            if verbose: print(f"Section ends with unclosed table, starting next section with the table at page {find_page(start)} offset {start} table start {last_table_start}")
            start = min(end - SECTION_OVERLAP, start + last_table_start)
        else:
            start = end - SECTION_OVERLAP

    if start + SECTION_OVERLAP < end:
        yield (all_text[start:end], find_page(start))

def filename_to_id(filename):
    filename_ascii = re.sub("[^0-9a-zA-Z_-]", "_", filename)
    filename_hash = base64.b16encode(filename.encode('utf-8')).decode('ascii')
    return f"file-{filename_ascii}-{filename_hash}"


def create_sections(uploaded_file, page_map, use_vectors):
    file_name = uploaded_file.filename
    file_id = filename_to_id(file_name)
    for i, (content, pagenum) in enumerate(split_text(page_map)):
        section = {
            "id": f"{file_id}-page-{i}",
            "content": content,
            "category": None,
            "sourcepage": blob_name_from_file_page(file_name, pagenum),
            "sourcefile": file_name
        }
        if use_vectors:
            section["embedding"] = compute_embedding(content)
        yield section


def before_retry_sleep(retry_state, verbose=True):
    if verbose: print("Rate limited on the OpenAI embeddings API, sleeping before retrying...")

@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(15), before_sleep=before_retry_sleep)
def compute_embedding(text):
    refresh_openai_token()
    return openai.Embedding.create(engine=AZURE_OPENAI_EMB_DEPLOYMENT, input=text)["data"][0]["embedding"]

# def create_search_index(files, verbose=False):
#     index_client = SearchIndexClient(endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net/",
#                                      credential=azure_credential)
    
#     for file_arg in files:
#         _, index_name, _ = file_arg.split(':')  # Split the file argument to extract the index name

#         if verbose:
#             print(f"Ensuring search index {index_name} exists")
        
#         if index_name not in index_client.list_index_names():
#             index = SearchIndex(
#                 name=index_name,
#                 fields=[
#                     SimpleField(name="id", type="Edm.String", key=True),
#                     SearchableField(name="content", type="Edm.String", analyzer_name="en.microsoft"),
#                     SearchField(name="embedding", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
#                                 hidden=False, searchable=True, filterable=False, sortable=False, facetable=False,
#                                 vector_search_dimensions=1536, vector_search_configuration="default"),
#                     SimpleField(name="category", type="Edm.String", filterable=True, facetable=True),
#                     SimpleField(name="sourcepage", type="Edm.String", filterable=True, facetable=True),
#                     SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True)
#                 ],
#                 semantic_settings=SemanticSettings(
#                     configurations=[SemanticConfiguration(
#                         name='default',
#                         prioritized_fields=PrioritizedFields(
#                             title_field=None, prioritized_content_fields=[SemanticField(field_name='content')]))]),
#                 vector_search=VectorSearch(
#                     algorithm_configurations=[
#                         VectorSearchAlgorithmConfiguration(
#                             name="default",
#                             kind="hnsw",
#                             hnsw_parameters=HnswParameters(metric="cosine")
#                         )
#                     ]
#                 )
#             )

#             if verbose:
#                 print(f"Creating {index_name} search index")

#             index_client.create_index(index)
#         else:
#             if verbose:
#                 print(f"Search index {index_name} already exists")


async def index_sections(uploaded_file, sections, search_creds, searchService, index_name, verbose=True):
    filename = uploaded_file.filename
    if verbose: print(f"Indexing sections from '{filename}' into search index '{index_name}'")
    search_client = SearchClient(endpoint=f"https://{searchService}.search.windows.net/",
                                    index_name=index_name,
                                    credential=search_creds)
    i = 0
    batch = []
    for s in sections:
        batch.append(s)
        i += 1
        if i % 1000 == 0:
            results = search_client.upload_documents(documents=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if verbose: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")
            batch = []

    if len(batch) > 0:
        results = search_client.upload_documents(documents=batch)
        succeeded = sum([1 for r in results if r.succeeded])
        if verbose: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")


def remove_from_index(filename, searchservice, search_creds, index_name=None, verbose=True):

    if verbose: 
        print(f"Removing sections from '{filename or '<all>'}' from search index '{index_name}'")
    
    search_client = SearchClient(endpoint=f"https://{searchservice}.search.windows.net/",
                                    index_name=index_name,
                                    credential=search_creds)
    
    while True:
        filter = None if filename is None else f"sourcefile eq '{os.path.basename(filename)}'"
        r = search_client.search("", filter=filter, top=1000, include_total_count=True)
        if r.get_count() == 0:
            break
        r = search_client.delete_documents(documents=[{ "id": d["id"] } for d in r])
        if verbose: 
            print(f"\tRemoved {len(r)} sections from index")
        
        # It can take a few seconds for search results to reflect changes, so wait a bit
        time.sleep(2)


# refresh open ai token every 5 minutes
def refresh_openai_token():
    if open_ai_token_cache[CACHE_KEY_TOKEN_TYPE] == 'azure_ad' and open_ai_token_cache[CACHE_KEY_CREATED_TIME] + 300 < time.time():
        token_cred = open_ai_token_cache[CACHE_KEY_TOKEN_CRED]
        openai.api_key = token_cred.get_token("https://cognitiveservices.azure.com/.default").token
        open_ai_token_cache[CACHE_KEY_CREATED_TIME] = time.time()


async def add_file(uploaded_file: any, index: str) -> bool:
    AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
    AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")

    # KB_FIELDS_CONTENT = os.getenv("KB_FIELDS_CONTENT", "content")
    # KB_FIELDS_SOURCEPAGE = os.getenv("KB_FIELDS_SOURCEPAGE", "sourcepage")

    # Set up Azure authentication.
    azure_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)

    openai.api_key = azure_credential.get_token("https://cognitiveservices.azure.com/.default").token
    openai.api_type = "azure_ad"
    open_ai_token_cache[CACHE_KEY_CREATED_TIME] = time.time()
    open_ai_token_cache[CACHE_KEY_TOKEN_CRED] = azure_credential
    open_ai_token_cache[CACHE_KEY_TOKEN_TYPE] = "azure_ad"

    openai.api_base = f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com"
    openai.api_version = "2022-12-01"
    
    await upload_blobs(uploaded_file=uploaded_file, container_name=index, storage_creds=azure_credential, storageaccount=AZURE_STORAGE_ACCOUNT, verbose=True)

    page_map = get_document_text(uploaded_file=uploaded_file)
    sections = create_sections(uploaded_file=uploaded_file, page_map=page_map, use_vectors=True)
    await index_sections(uploaded_file=uploaded_file, search_creds=azure_credential, sections=sections, index_name=index, verbose=True)
    return True




# Set up Blob Storage clients.
    # blob_client = BlobServiceClient(
    #     account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
    #     credential=azure_credential
    # )

    # blob_container_clients = {}
    # for container in AZURE_STORAGE_CONTAINER:
    #     blob_container_clients[container] = blob_client.get_container_client(container)

    # # Set up Search clients for multiple indices.
    # search_clients = {}
    # for index_name in AZURE_SEARCH_INDICES:
    #     search_clients[index_name] = SearchClient(
    #         endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net",
    #         index_name=index_name,
    #         credential=azure_credential
    #     )
    # AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
    # AZURE_STORAGE_CONTAINER = ["natural-capital", "energy", "climate-financing", "green-minerals", "sust-agric", "adaptation", "infrastructure"]
    # AZURE_SEARCH_SERVICE = os.getenv("AZURE_SEARCH_SERVICE")
    # AZURE_SEARCH_INDICES = ["natural-capital", "energy", "climate-financing", "green-minerals", "sust-agric", "adaptation", "infrastructure"]
    # AZURE_TENANT_ID=os.getenv("AZURE_TENANT_ID")
    # AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")
    # AZURE_OPENAI_CHATGPT_DEPLOYMENT = os.getenv("AZURE_OPENAI_CHATGPT_DEPLOYMENT")
    # AZURE_OPENAI_CHATGPT_MODEL = os.getenv("AZURE_OPENAI_CHATGPT_MODEL")
    # AZURE_OPENAI_EMB_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMB_DEPLOYMENT")
    

    # azd_credential = AzureDeveloperCliCredential(tenant_id=AZURE_TENANT_ID, process_timeout=60)
    # default_creds = azd_credential
    # search_creds = AzureKeyCredential(default_creds)
    # storage_creds = default_creds