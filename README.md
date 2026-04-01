# Tender Workbook Generator

This app uploads a tender document, extracts tender details, fills the bundled `Tender Synopsis Report (2).xlsx` template, fills the bundled `Bid-No Bid Stratergy Sheet V-1.0.xlsx` template, and lets you download both generated workbooks.

## What changed

- OCR now runs through the OpenAI API instead of EasyOCR.
- The extraction flow still keeps the existing rule-based tender parser as a fallback, but OpenAI now corrects and completes the extracted fields.
- If a field contains extra detail that should not be squeezed into the main value cell, that extra detail is written into the `Remark` column.
- Scanned image uploads and weak PDF pages are sent through OpenAI OCR before field extraction.
- Local `.env` values are now loaded automatically, so `OPENAI_API_KEY` works without exporting it manually in PowerShell.

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

## Output files

- `Tender Synopsis Report (2).xlsx` filled from the tender document
- `Bid-No Bid Stratergy Sheet V-1.0.xlsx` filled with allocation scores, total percentage, and highlighted category

## Prerequisites

- Python 3.13+
- Packages from `requirements.txt`
- `OPENAI_API_KEY` set in the environment

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
- `OPENAI_DOCUMENT_MAX_CHARS`: optional safety cap for the combined document text sent for extraction
- `PORT`: optional locally, provided by Railway in deployment

## Railway deployment

1. Push this project to GitHub.
2. In Railway, create a new project and choose `Deploy from GitHub repo`.
3. Select this repository.
4. In the Railway service `Variables` tab, add:
   - `OPENAI_API_KEY`
   - `OPENAI_OCR_MODEL=gpt-4o-mini`
   - `OPENAI_EXTRACTION_MODEL=gpt-4o-mini`
   - `OPENAI_DOCUMENT_MAX_CHARS=1200000`
5. Railway should install dependencies from `requirements.txt`.
6. If Railway does not auto-detect the start command, set the service `Start Command` to:

```bash
python app.py
```

7. Deploy the service.
8. After deploy, open the generated public URL and confirm `/health` returns `{"status":"ok"}`.

## Notes

- The app listens on `0.0.0.0` and uses the `PORT` environment variable, so it is ready for Railway.
- The repository includes `.env.example`, `requirements.txt`, and `Procfile` to make deployment simpler.
- Do not commit a real API key into the repository. Keep it only in local env files and Railway variables.
