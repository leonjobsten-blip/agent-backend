import os
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException
from openai import OpenAI

app = FastAPI()

api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY non impostata. Esegui: export OPENAI_API_KEY='...'")

client = OpenAI(api_key=api_key)

SYSTEM_PROMPT = """
Sei un assistente contabile per Banana Accounting (Svizzera).
Restituisci SOLO un CSV con intestazione:

Data;Fattura;Descrizione;CtDare;CtAvere;Importo;Moneta;Cod. IVA

Regole:
- Moneta: CHF
- Migliaia: '
- Decimali: .
- Se manca un dato: aggiungi riga '# ERRORE: ...'
"""

@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Carica un PDF (application/pdf)")

    pdf_bytes = await file.read()

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    try:
        uploaded = client.files.create(file=open(tmp_path, "rb"), purpose="assistants")
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "input_text", "text": "Genera il CSV Banana da questo PDF."},
                    {"type": "input_file", "file_id": uploaded.id}
                ]}
            ]
        )
        return {"csv": resp.output_text.strip()}
    finally:
        try:
            os.remove(tmp_path)
        except:
            pass
