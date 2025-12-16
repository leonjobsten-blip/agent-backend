import os
import re
import tempfile
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from openai import OpenAI

app = FastAPI(title="A&F Banana Agent", version="1.0.0")

# --- Configuration ---
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

if not OPENAI_API_KEY:
    # Don't crash import in production environments where env is injected later;
    # but fail clearly on first request.
    print("WARNING: OPENAI_API_KEY is not set. /parse will fail until set.")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# --- IMPORTANT: This prompt is the "punto 2" ---
# It encodes the exact Banana CSV rules + your chart-of-accounts patterns.
SYSTEM_PROMPT = """
Sei un assistente contabile per Banana Accounting (Svizzera) per l'azienda "Acqua & Farina".
Ricevi un PDF di rendiconto/payout (Smood, Uber Eats, Smartbox) e devi restituire SOLO un CSV
pronto da incollare in Banana. NESSUNA spiegazione, nessun markdown, nessun testo extra.

FORMATO CSV OBBLIGATORIO (separatore ;):
Data;Fattura;Descrizione;CtDare;CtAvere;Importo;Moneta;Cod. IVA

REGOLE BANANA (OBBLIGATORIE):
- CtDare e CtAvere DEVONO contenere SOLO numeri di conto (es: 100020,105010,300030,300040,400010,400020).
  NON mettere importi nei campi CtDare/CtAvere.
- Importo: SEMPRE positivo, con decimale punto (.) e separatore migliaia apostrofo (') se presente.
- Moneta: sempre CHF.
- Ogni riga deve avere Data, Descrizione, CtDare, CtAvere, Importo, Moneta compilati.
- Se un dato essenziale manca e non è ricavabile con certezza, NON inventare: restituisci una sola riga:
  # ERRORE: <spiega cosa manca>
  (e nient'altro)

PIANO DEI CONTI / SCHEMI DI REGISTRAZIONE (DA RISPETTARE ESATTAMENTE):

CONTABILIZZAZIONE STANDARD "PIATTAFORME" (tutte):
1) RICAVI (lordo):
   - CtDare = 105010
   - CtAvere = (Smood/Uber -> 300030) oppure (Smartbox -> 300040)
   - Importo = ricavi lordi
   - Cod. IVA:
        * Smood: usa F1 per ricavi 8.1% e F2 per ricavi 2.6% (se presenti, crea 2 righe separate).
        * Smartbox: usa V0.
        * Uber: lascia vuoto.
2) COSTO SERVIZIO / COMMISSIONI:
   - Smood + Uber: CtDare = 400010, CtAvere = 105010
   - Smartbox: CtDare = 400020, CtAvere = 105010
   - Importo = commissioni/fee (positivo)
   - Cod. IVA: lascia vuoto (a meno che il PDF indichi chiaramente IVA su fee; se non chiarissimo, vuoto)
3) INCASSO (payout/netto):
   - CtDare = 100020
   - CtAvere = 105010
   - Importo = netto incassato (ricavi - costi servizio) positivo
   - Cod. IVA: vuoto
   - Data: la data di incasso/accredito indicata nel PDF (non il periodo di vendita)

SMOOD (specifiche):
- Se nel PDF compaiono "Smood Hardware Rent - Printer" e/o "Tablet": trattali come COSTO SERVIZIO (400010/105010).
  Se sono più righe, sommale in un'unica riga "Costo servizio Smood (Hardware Rent)".
- Ricavi Smood: separa in base alle aliquote riportate (2.6% e 8.1%). Se solo una aliquota, una sola riga.
- Descrizioni coerenti: "Smood - <mese>" per ricavi; "Costo servizio Smood" per fee; "Incasso Smood - <mese>" per payout.

UBER (specifiche):
- Descrizioni coerenti: "Uber" per ricavi; "Marketplace fee Uber" per fee; "Incasso Uber - <periodo>" per payout.
- Nessun Cod. IVA.

SMARTBOX (specifiche):
- Per ogni fattura PDN-... crea le 3 righe (ricavo, commissione, incasso) come sopra.
- Descrizione incasso: "Incasso Smartbox fattura nr PDN-xxxxx".

OBIETTIVO: il CSV deve replicare il pattern delle registrazioni già presenti in contabilità (Banana).
"""

# --- Helpers: validate output so Banana doesn't get garbage ---
HEADER = "Data;Fattura;Descrizione;CtDare;CtAvere;Importo;Moneta;Cod. IVA"
ACCOUNT_RE = re.compile(r"^\d{4,10}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
AMOUNT_RE = re.compile(r"^[0-9]{1,3}(?:'[0-9]{3})*(?:\.[0-9]{2})$|^[0-9]+(?:\.[0-9]{2})$")

def _normalize_lines(text: str) -> List[str]:
    # Remove code fences and trim
    text = text.strip()
    text = re.sub(r"^```(?:csv)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() != ""]
    return lines

def _validate_csv(lines: List[str]) -> Optional[str]:
    # Allow error-only output
    if lines and lines[0].startswith("# ERRORE:"):
        return None

    if not lines or lines[0] != HEADER:
        return f"Header mancante o errato. Atteso: {HEADER}"

    for i, ln in enumerate(lines[1:], start=2):
        if ln.startswith("#"):
            return f"Riga {i}: commenti non ammessi nel CSV (usa solo righe dati o un'unica riga # ERRORE)."

        parts = ln.split(";")
        if len(parts) != 8:
            return f"Riga {i}: numero colonne errato ({len(parts)}). Attese 8 colonne."

        date, fatt, desc, ct_dare, ct_avere, imp, moneta, codiva = [p.strip() for p in parts]

        if not DATE_RE.match(date):
            return f"Riga {i}: Data non valida '{date}' (formato richiesto YYYY-MM-DD)."

        if not desc:
            return f"Riga {i}: Descrizione vuota."

        if not ACCOUNT_RE.match(ct_dare) or not ACCOUNT_RE.match(ct_avere):
            return f"Riga {i}: CtDare/CtAvere devono essere numeri di conto. Trovato CtDare='{ct_dare}' CtAvere='{ct_avere}'."

        if moneta != "CHF":
            return f"Riga {i}: Moneta deve essere CHF."

        # Amount must be positive and formatted
        if imp.startswith("-"):
            return f"Riga {i}: Importo deve essere positivo (trovato {imp})."
        if not AMOUNT_RE.match(imp):
            return f"Riga {i}: Importo formato non valido '{imp}'. Usa 1234.56 oppure 1'234.56"

        # Cod IVA can be empty or one of allowed codes
        if codiva and codiva not in {"F1", "F2", "V0", "8.1", "2.6"}:
            return f"Riga {i}: Cod. IVA non ammesso '{codiva}'."

    return None

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    if not client:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY non impostata sul server.")

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Carica un PDF (application/pdf).")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="PDF vuoto.")

    # Store temp file for OpenAI upload
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        uploaded = client.files.create(file=open(tmp_path, "rb"), purpose="assistants")

        resp = client.responses.create(
            model=MODEL,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "input_text", "text": "Estrai i dati e restituisci SOLO il CSV Banana conforme alle regole."},
                    {"type": "input_file", "file_id": uploaded.id},
                ]},
            ],
        )

        raw_text = (resp.output_text or "").strip()
        lines = _normalize_lines(raw_text)
        err = _validate_csv(lines)

        if err:
            # Ask the model once to self-correct (cheap & effective)
            repair = client.responses.create(
                model=MODEL,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": [
                        {"type": "input_text", "text": f"Il CSV non è valido per Banana. Errore: {err}\nCorreggi e restituisci SOLO il CSV valido, senza testo extra."},
                        {"type": "input_text", "text": raw_text},
                    ]},
                ],
            )
            raw_text = (repair.output_text or "").strip()
            lines = _normalize_lines(raw_text)
            err2 = _validate_csv(lines)
            if err2:
                # Return clear diagnostic to help fix prompt/mapping
                raise HTTPException(status_code=422, detail=f"CSV non valido dopo correzione: {err2}")

        return {"csv": "\n".join(lines)}

    except HTTPException:
        raise
    except Exception as e:
        # Provide readable error for the app
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
