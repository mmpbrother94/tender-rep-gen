# Tender Workbook Generator

This app uploads a tender document, extracts tender details, fills the bundled `Tender Synopsis Report (2).xlsx` template, fills the bundled `Bid-No Bid Stratergy Sheet V-1.0.xlsx` template, and lets you download both generated workbooks.

## Supported input formats

- `.pdf`
- `.docx`
- `.txt`
- `.rtf`
- `.png`
- `.jpg`
- `.jpeg`
- `.bmp`
- `.tif`
- `.tiff`
- `.webp`

If a synopsis field is not found in the uploaded document, the generated workbook writes `Not Available`.

## Architecture modes

- Local mode: if `REDIS_URL` is not set, the app keeps the existing single-service background-thread flow.
- Render queue mode: if `REDIS_URL` is set, the web service only handles upload, status, and download. A separate Render worker pulls jobs from Redis, generates the workbooks, and sends the files back to the web service over Render's private network.

## Local run

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
python app.py
```

Then open `http://127.0.0.1:8000`.

Do not open `http://0.0.0.0:8000` in the browser. `0.0.0.0` is only the bind address for the server process.

## Environment variables

- `OPENAI_API_KEY`: required for OCR and OpenAI-based field correction
- `OPENAI_OCR_MODEL`: optional, defaults to `gpt-4o-mini`
- `OPENAI_EXTRACTION_MODEL`: optional, defaults to `gpt-4o-mini`
- `OPENAI_DOCUMENT_MAX_CHARS`: optional cap for combined document text sent for extraction
- `OPENAI_MAX_FILE_BYTES`: optional cap for direct PDF attachment to OpenAI
- `OPENAI_ATTACH_PDF`: optional, defaults to `false`
- `OPENAI_ATTACH_PDF_MAX_FILE_BYTES`: optional cap for full-PDF attachment when enabled
- `APP_DATA_DIR`: writable runtime directory
- `MAX_CONCURRENT_GENERATIONS`: local-only safety limit for in-process generation
- `REDIS_URL`: enables queue mode for Render-style deployment
- `INTERNAL_SERVICE_TOKEN`: shared secret between the Render web service and worker
- `WEB_INTERNAL_HOSTPORT`: internal Render address of the web service for the worker
- `WEB_INTERNAL_BASE_URL`: optional explicit internal base URL for the worker
- `WORKER_REQUEUE_ON_STARTUP`: optional, defaults to `true`
- `WORKER_POLL_TIMEOUT_SECONDS`: optional worker queue wait time
- `PORT`: provided automatically by Render and Railway

## Render deployment

The repository now includes [render.yaml](./render.yaml) for a three-service Render deployment:

- `tender-rep-gen-web`
- `tender-rep-gen-worker`
- `tender-rep-gen-kv`

### Deploy with the Blueprint

1. Push the latest code to GitHub.
2. In Render, open `Blueprints` and create a new Blueprint from this repository.
3. Review the services defined in `render.yaml`.
4. Provide `OPENAI_API_KEY` when Render prompts for secret values.
5. Approve the Blueprint deploy.
6. Wait for these services to become healthy:
   - web service
   - background worker
   - key value
7. Open the web service URL and confirm `/health` returns `{"status":"ok"}`.

### What the Render Blueprint sets up

- A paid `starter` web service with a persistent disk mounted at `/data`
- A paid `starter` background worker
- A paid `starter` Render Key Value instance for queue and job state
- Shared environment wiring between the services

### Why the Render deployment uses paid services

- The web service needs a persistent disk for uploads and generated workbooks.
- The worker needs to call the web service over Render's private network.
- Free web services cannot receive private-network traffic and cannot use persistent disks.
- Free Key Value instances are in-memory only, so queue and job state can be lost on restart.

## Worker flow on Render

1. The web service stores the uploaded file on its disk.
2. The web service creates a job in Redis and enqueues it.
3. The worker pulls the job from Redis.
4. The worker downloads the source file from the web service over Render's private network.
5. The worker generates both workbooks.
6. The worker uploads the generated `.xlsx` files back to the web service.
7. The web service serves the final downloads from its persistent disk.

## Railway note

Railway support still exists for single-service deployment, but the repo is now optimized for Render queue mode because that is the safer path for long-running OCR and workbook generation.

## Security note

Do not commit a real API key into the repository. Keep it only in local env files and in Render or Railway secrets.
