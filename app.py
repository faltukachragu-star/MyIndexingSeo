import json
import xml.etree.ElementTree as ET
import asyncio
import aiohttp
import os
from datetime import datetime, date, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import Optional

# --- Configuration from Environment Variables ---
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
SECRET_KEY = os.environ.get("SECRET_KEY")

if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SECRET_KEY]):
    raise Exception("Missing required environment variables: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SECRET_KEY")

# --- Application Setup ---
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# --- Constants ---
CONCURRENT_REQUEST_LIMIT = 25
REQUEST_TIMEOUT_SECONDS = 30
API_ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/indexing",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/webmasters.readonly"
]

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# --- Google API Service Helper ---
def get_gsc_service(session: dict):
    """Builds the GSC service object from session credentials."""
    creds_data = session['credentials']
    credentials = Credentials(
        token=creds_data['token'],
        refresh_token=creds_data['refresh_token'],
        token_uri=creds_data['token_uri'],
        client_id=creds_data['client_id'],
        client_secret=creds_data['client_secret'],
        scopes=creds_data['scopes']
    )
    return build('webmasters', 'v3', credentials=credentials)

# --- OAuth 2.0 Flow Functions ---
def get_oauth_flow(request: Request):
    return Flow.from_client_config(
        client_config={
            "web": {
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [str(request.url_for('auth_callback'))],
            }
        },
        scopes=SCOPES,
        redirect_uri=str(request.url_for('auth_callback'))
    )

@app.get('/login')
async def login(request: Request, flow: Flow = Depends(get_oauth_flow)):
    authorization_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true', prompt='consent')
    request.session['state'] = state
    return RedirectResponse(authorization_url)

@app.get('/auth/callback')
async def auth_callback(request: Request, flow: Flow = Depends(get_oauth_flow)):
    flow.fetch_token(authorization_response=str(request.url))
    credentials = flow.credentials
    request.session['credentials'] = {
        'token': credentials.token, 'refresh_token': credentials.refresh_token, 'token_uri': credentials.token_uri,
        'client_id': credentials.client_id, 'client_secret': credentials.client_secret, 'scopes': credentials.scopes
    }
    async with aiohttp.ClientSession() as session:
        async with session.get('https://www.googleapis.com/oauth2/v1/userinfo', headers={'Authorization': f'Bearer {credentials.token}'}) as resp:
            user_info = await resp.json()
            request.session['user'] = {'email': user_info.get('email')}
    return RedirectResponse(url=app.url_path_for('read_root'))

@app.get('/logout')
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url=app.url_path_for('read_root'))

# --- Page Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open('index.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())
@app.get("/privacy", response_class=HTMLResponse)
async def read_privacy():
    with open('privacy.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())
@app.get("/terms", response_class=HTMLResponse)
async def read_terms():
    with open('terms.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())

# --- API Endpoints ---
@app.get("/status")
async def status(request: Request):
    return {"logged_in": 'user' in request.session, "user": request.session.get('user')}

def handle_http_error(e: HttpError):
    """Parses a Google API HttpError for a user-friendly message."""
    try:
        error_content = json.loads(e.content)
        message = error_content.get("error", {}).get("message", "An unknown API error occurred.")
        if e.resp.status == 403:
            return f"Permission Denied. Check your GSC permissions. Details: {message}"
        return f"GSC API Error (Code {e.resp.status}): {message}"
    except (json.JSONDecodeError, AttributeError):
        return f"An unexpected API error occurred (Status: {e.resp.status})."

# --- NEW GSC ENDPOINT: List sites user has access to ---
@app.get("/gsc-sites")
async def get_gsc_sites(request: Request):
    if 'credentials' not in request.session:
        raise HTTPException(status_code=401, detail="Authentication required.")
    try:
        def fetch_sites():
            service = get_gsc_service(request.session)
            return service.sites().list().execute()
        
        site_list = await asyncio.to_thread(fetch_sites)
        return {"sites": site_list.get('siteEntry', [])}
    except HttpError as e:
        log(f"GSC Sites API Error: {e}")
        raise HTTPException(status_code=e.resp.status, detail=handle_http_error(e))
    except Exception as e:
        log(f"An unexpected error occurred in get_gsc_sites: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

# --- UPDATED GSC ENDPOINT: Get performance data ---
@app.get("/gsc-data")
async def get_gsc_data(request: Request, site_url: str):
    if 'credentials' not in request.session:
        raise HTTPException(status_code=401, detail="Authentication required.")
    try:
        def fetch_data():
            service = get_gsc_service(request.session)
            end_date = date.today().strftime('%Y-%m-%d')
            start_date = (date.today() - timedelta(days=7)).strftime('%Y-%m-%d')
            api_request = {'startDate': start_date, 'endDate': end_date, 'dimensions': ['page'], 'rowLimit': 25}
            return service.searchanalytics().query(siteUrl=site_url, body=api_request).execute()

        gsc_data_raw = await asyncio.to_thread(fetch_data)
        rows = gsc_data_raw.get('rows', [])
        formatted_data = [{
            "page": row['keys'][0], "clicks": row['clicks'], "impressions": row['impressions'],
            "ctr": f"{row['ctr'] * 100:.2f}%", "position": f"{row['position']:.2f}"
        } for row in rows]
        return {"data": formatted_data}
    except HttpError as e:
        log(f"GSC Data API Error: {e}")
        raise HTTPException(status_code=e.resp.status, detail=handle_http_error(e))
    except Exception as e:
        log(f"An unexpected error occurred in get_gsc_data: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

# --- NEW GSC ENDPOINT: Inspect a URL ---
@app.get("/inspect-url")
async def inspect_url(request: Request, site_url: str, inspection_url: str):
    if 'credentials' not in request.session:
        raise HTTPException(status_code=401, detail="Authentication required.")
    try:
        def do_inspection():
            service = get_gsc_service(request.session)
            body = {'inspectionUrl': inspection_url, 'siteUrl': site_url}
            return service.urlInspection().index().inspect(body=body).execute()

        inspection_result = await asyncio.to_thread(do_inspection)
        status = inspection_result.get('inspectionResult', {}).get('indexStatusResult', {})
        return {"status": status}
    except HttpError as e:
        log(f"URL Inspection API Error: {e}")
        raise HTTPException(status_code=e.resp.status, detail=handle_http_error(e))
    except Exception as e:
        log(f"An unexpected error occurred in inspect_url: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")


# --- Indexing API WebSocket Logic (Mostly Unchanged) ---
async def process_url_task(session, access_token, url, semaphore):
    async with semaphore:
        log(f"  > Submitting URL: {url}")
        headers = {"Authorization": f"Bearer {access_token}"}
        payload = {'url': url, 'type': 'URL_UPDATED'}
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        try:
            async with session.post(API_ENDPOINT, json=payload, headers=headers, timeout=timeout) as response:
                response_json = await response.json()
                if response.status >= 400:
                    error_message = response_json.get("error", {}).get("message", "Unknown API error")
                    return {"type": "result", "status": "error", "url": url, "message": f"API Error ({response.status}): {error_message}"}
                return {"type": "result", "status": "success", "url": url, "message": "Notification received."}
        except asyncio.TimeoutError:
            return {"type": "result", "status": "error", "url": url, "message": f"Request Timed Out ({REQUEST_TIMEOUT_SECONDS}s)"}
        except aiohttp.ClientError as e:
            return {"type": "result", "status": "error", "url": url, "message": f"Connection Error: {e}"}

async def parse_urls_from_payload(file_payload):
    content = file_payload['content']
    filename = file_payload['filename']
    urls = []
    try:
        if filename.endswith('.txt'): urls = content.strip().splitlines()
        elif filename.endswith('.xml'):
            root = ET.fromstring(content)
            loc_tags = root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
            urls = [tag.text.strip() for tag in loc_tags]
    except Exception as e:
        log(f"  ! Error parsing file content: {e}")
        return []
    return [url.strip() for url in urls if url.strip().startswith('http')]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    if 'credentials' not in websocket.session:
        await websocket.send_json({"type": "critical_error", "message": "Authentication failed. Please log in again."})
        await websocket.close(code=1008); return
    try:
        initial_data = json.loads(await websocket.receive_text())
        creds_data = websocket.session['credentials']
        credentials = Credentials(**creds_data)
        if credentials.expired and credentials.refresh_token:
            from google.auth.transport.requests import Request as GRequest
            credentials.refresh(GRequest())
            websocket.session['credentials']['token'] = credentials.token
        
        urls_to_process = []
        if initial_data.get('urls_file'):
            urls_to_process = await parse_urls_from_payload(initial_data['urls_file'])
        elif initial_data.get('urls_text'):
            urls_to_process = [u.strip() for u in initial_data['urls_text'].split('\n') if u.strip().startswith('http')]
        
        if not urls_to_process: raise ValueError("No valid URLs were found for indexing.")

        semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
        async with aiohttp.ClientSession() as session:
            tasks = [process_url_task(session, credentials.token, url, semaphore) for url in urls_to_process]
            for future in asyncio.as_completed(tasks):
                await websocket.send_json(await future)
        await websocket.send_json({"type": "done", "message": "All URLs processed."})
    except WebSocketDisconnect:
        log("WebSocket connection closed by client.")
    except Exception as e:
        log(f"An error occurred in WebSocket: {e}")
        await websocket.send_json({"type": "critical_error", "message": str(e)})
    finally:
        if not websocket.client_state == 'DISCONNECTED': await websocket.close()
