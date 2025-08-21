import json
import xml.etree.ElementTree as ET
import asyncio
import aiohttp
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from google.oauth2 import service_account
from google.auth.transport.requests import Request # <-- THE OFFICIAL TRANSPORT LIBRARY
from typing import Optional

# Configuration
CONCURRENT_REQUEST_LIMIT = 25
REQUEST_TIMEOUT_SECONDS = 30
API_ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
SCOPES = ["https://www.googleapis.com/auth/indexing"]

app = FastAPI()

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

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

@app.get("/", response_class=HTMLResponse)
async def read_root():
    with open('index.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())
@app.get("/tutorial", response_class=HTMLResponse)
async def read_tutorial():
    with open('tutorial.html', 'r', encoding='utf-8') as f: return HTMLResponse(content=f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    log("WebSocket connection accepted.")
    
    try:
        initial_data = json.loads(await websocket.receive_text())
        credentials_info = initial_data['credentials']
        
        log("Authenticating and fetching access token...")
        
        # ### THE FINAL, DEFINITIVE, TRIPLE-CHECKED FIX IS HERE ###
        # This is the modern and correct way to refresh a token.
        
        # 1. Create credentials object with the correct scopes
        credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
        
        # 2. Use the official Request object from google.auth.transport to refresh
        credentials.refresh(Request()) # <-- This is the rock-solid method.
        
        access_token = credentials.token
        if not access_token:
            raise ValueError("Failed to obtain access token from credentials.")
        log("Access token obtained successfully.")
        
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
        await websocket.close()
        log("WebSocket connection closed.")
