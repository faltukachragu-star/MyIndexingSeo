import json
import xml.etree.ElementTree as ET
import asyncio
import aiohttp
import os
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from typing import Optional

# --- NEW: Configuration from Environment Variables ---
# You must set these in your terminal before running the app.
# See the updated tutorial.html for instructions.
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
# This is for signing the session cookie.
SECRET_KEY = os.environ.get("SECRET_KEY")

if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SECRET_KEY]):
    raise Exception("Missing required environment variables: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SECRET_KEY")

# --- Application Setup ---
app = FastAPI()
# Add session middleware to handle user login sessions
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# --- Constants ---
CONCURRENT_REQUEST_LIMIT = 25
REQUEST_TIMEOUT_SECONDS = 30
API_ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
SCOPES = [
    "https://www.googleapis.com/auth/indexing",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email"
]

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# --- NEW: OAuth 2.0 Flow Functions ---
def get_oauth_flow(request: Request):
    """Creates an OAuth 2.0 Flow instance."""
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
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent' # Use 'consent' to always get a refresh token
    )
    request.session['state'] = state
    return RedirectResponse(authorization_url)

@app.get('/auth/callback')
async def auth_callback(request: Request, flow: Flow = Depends(get_oauth_flow)):
    flow.fetch_token(authorization_response=str(request.url))
    credentials = flow.credentials
    
    # Store user credentials in the session
    request.session['credentials'] = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
    
    # Get user info to display
    async with aiohttp.ClientSession() as session:
        async with session.get('https://www.googleapis.com/oauth2/v1/userinfo',
                               headers={'Authorization': f'Bearer {credentials.token}'}) as resp:
            user_info = await resp.json()
            request.session['user'] = {'email': user_info.get('email')}

    return RedirectResponse(url=app.url_path_for('read_root'))

@app.get('/logout')
async def logout(request: Request):
    request.session.pop('credentials', None)
    request.session.pop('user', None)
    return RedirectResponse(url=app.url_path_for('read_root'))

# --- API & Page Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open('index.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())

@app.get("/tutorial", response_class=HTMLResponse)
async def read_tutorial():
    with open('tutorial.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())

@app.get("/status")
async def status(request: Request):
    """Endpoint for the frontend to check if a user is logged in."""
    if 'user' in request.session:
        return {"logged_in": True, "user": request.session['user']}
    return {"logged_in": False}


# --- URL Processing Logic (Mostly Unchanged) ---
async def process_url_task(session, access_token, url, semaphore):
    # This function is the same as before
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
                    return {"type": "result", "status": "error", "url": url, "message": f"API Error (Code {response.status}): {error_message}"}
                else:
                    return {"type": "result", "status": "success", "url": url, "message": "Notification received."}
        except asyncio.TimeoutError:
            log(f"  ! FAILED URL (Timeout): {url}")
            return {"type": "result", "status": "error", "url": url, "message": f"Request Timed Out ({REQUEST_TIMEOUT_SECONDS}s)"}
        except aiohttp.ClientError as e:
            log(f"  ! FAILED URL (ClientError): {url}, Reason: {e}")
            return {"type": "result", "status": "error", "url": url, "message": f"Connection Error: {e}"}

async def parse_urls_from_payload(file_payload):
    # This function is the same as before
    content = file_payload['content']
    filename = file_payload['filename']
    urls = []
    log(f"\n--- Parsing received file content from: {filename} ---")
    try:
        if filename.endswith('.txt'): urls = content.strip().splitlines()
        elif filename.endswith('.xml'):
            root = ET.fromstring(content)
            loc_tags = root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
            urls = [tag.text.strip() for tag in loc_tags]
    except Exception as e:
        log(f"  ! Error parsing file content: {e}")
        return []
    valid_urls = [url.strip() for url in urls if url.strip().startswith('http')]
    log(f"--- Found {len(valid_urls)} valid URLs in file content. ---")
    return valid_urls

# --- WebSocket Endpoint (REFACTORED) ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    log("WebSocket connection accepted.")
    
    # --- CHANGED: Authentication via Session Cookie ---
    if 'credentials' not in websocket.session:
        log("WebSocket connection rejected: No user credentials in session.")
        await websocket.send_json({"type": "critical_error", "message": "Authentication failed. Please log in again."})
        await websocket.close(code=1008)
        return

    try:
        initial_data = json.loads(await websocket.receive_text())
        
        # Create a Credentials object from the session data
        creds_data = websocket.session['credentials']
        credentials = Credentials(
            token=creds_data['token'],
            refresh_token=creds_data['refresh_token'],
            token_uri=creds_data['token_uri'],
            client_id=creds_data['client_id'],
            client_secret=creds_data['client_secret'],
            scopes=creds_data['scopes']
        )
        
        # The credentials object will automatically refresh the token if it's expired.
        # We perform a refresh check here to ensure we have a valid token to start.
        if credentials.expired and credentials.refresh_token:
            log("Access token expired, refreshing...")
            credentials.refresh(a_request()) # a_request from google.auth.transport.requests
            # Update the session with the new token
            websocket.session['credentials']['token'] = credentials.token
            log("Access token refreshed successfully.")

        access_token = credentials.token
        
        urls_to_process = []
        if initial_data.get('urls_file'):
            urls_to_process = await parse_urls_from_payload(initial_data['urls_file'])
        elif initial_data.get('urls_text'):
            urls_to_process = [u.strip() for u in initial_data['urls_text'].split('\n') if u.strip().startswith('http')]
        
        if not urls_to_process:
            raise ValueError("No valid URLs were found.")
            
        log(f"Data parsed. Found {len(urls_to_process)} URLs. Starting processing.")
        
        semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
        async with aiohttp.ClientSession() as session:
            tasks = [process_url_task(session, access_token, url, semaphore) for url in urls_to_process]
            for future in asyncio.as_completed(tasks):
                result = await future
                await websocket.send_json(result)
                
        await websocket.send_json({"type": "done", "message": "All URLs processed."})
        log("All tasks complete. Sent 'done' message.")
        
    except WebSocketDisconnect:
        log("WebSocket connection closed by client.")
    except Exception as e:
        log(f"An error occurred in WebSocket: {e}")
        await websocket.send_json({"type": "critical_error", "message": str(e)})
    finally:
        if not websocket.client_state == 'DISCONNECTED':
            await websocket.close()
        log("WebSocket connection closed.")
