import os
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException
from openai import OpenAI
import sqlite3, os, hashlib, datetime
from pydantic import BaseModel

DB_PATH = os.getenv("DB_PATH", "data/feedback.db")

def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS corrections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        source TEXT NOT NULL,
        invoice_id TEXT,
        pdf_sha256 TEXT,
        model_csv TEXT NOT NULL,
        correct_csv TEXT NOT NULL
    )
    """)
    conn.commit()
    return conn

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()
class FeedbackPayload(BaseModel):
    source: str
    invoice_id: str | None = None
    pdf_sha256: str | None = None
    model_csv: str
    correct_csv: str

app = FastAPI()

api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY non impostata. Esegui: export OPENAI_API_KEY='...'")

client = OpenAI(api_key=api_key)

SYSTEM_PROMPT = """
Sei un assistente contabile svizzero per Banana Accounting. La tua missione è
estrarre dai PDF di payout (Smood, Uber e Smartbox) i dati
necessari a produrre registrazioni contabili coerenti con il piano dei
conti di Acqua & Farina. Devi restituire **solo** un file CSV con
l’intestazione:

Data;Fattura;Descrizione;CtDare;CtAvere;Importo;Moneta;Cod. IVA

### Regole generali
* La moneta è sempre **CHF**. Gli importi devono usare l’apostrofo
  (\') come separatore delle migliaia e il punto (.) come separatore
  dei decimali (es. 3'612.50).
* Ogni riga deve avere **8 colonne** nell’ordine indicato
  (Data, Fattura, Descrizione, CtDare, CtAvere, Importo, Moneta,
  Cod. IVA).
* Gli importi vanno sempre espressi come numeri **positivi**. Per
  storni o sconti riporta comunque l’importo positivo ma conserva i
  conti Dare/Avere corretti.
* Usa **solo** questi conti: 100020 (Banca), 105010 (transito
  piattaforme), 300030 (Ricavi Smood/Uber), 300040 (Ricavi Smartbox),
  400010 (Costi Smood/Uber), 400020 (Costi Smartbox). Non inventare
  altri conti.
* I codici IVA sono:
  – **F1** per aliquota 8,1 %
  – **F2** per aliquota 2,6 %
  – **V0** per importi esenti (Smartbox)
  – Lascia vuoto Cod. IVA quando la riga è un costo di servizio.
* Se non riesci a determinare un dato essenziale, aggiungi una riga che
  inizia con `# ERRORE:` descrivendo il problema (ma non inventare
  valori).

### Piattaforma Smood
I PDF Smood contengono **due fatture** per lo stesso periodo (hardware
rent e delivery). Tratta le fatture così:
1. **Hardware rent**: riconosci le voci che iniziano con "Smood Hardware
   Rent - " (es. printer o tablet). Somma i loro importi e registra
   **una sola riga di costo di servizio** con:
   * CtDare = 400010
   * CtAvere = 105010
   * Importo = somma degli importi hardware
   * Cod. IVA vuoto
   * Descrizione = "Costo servizio Smood"
2. **Ricavi delivery**: per le voci “Prodotti alimentari”,
   “Gesti commerciali”, “Altri” divise per aliquota IVA. Per ciascuna
   aliquota calcola l’**imponibile** dividendo l’importo lordo per
   (1 + aliquota). Esempio: per 8,1 % dividi per 1,081; per 2,6 %
   dividi per 1,026. Somma gli imponibili di ogni aliquota e produci
   una riga per aliquota:
   * CtDare = 105010
   * CtAvere = 300030
   * Importo = somma imponibile
   * Cod. IVA = F1 (8,1 %) o F2 (2,6 %) in base all’aliquota
   * Descrizione = "Smood – [mese]"
   Se la categoria è negativa (es. gesti commerciali negativi),
   continua a usare CtDare = 105010 e CtAvere = 300030 ma somma
   comunque l’imponibile positivo per quella aliquota.
3. **Commissione di servizio**: calcola la commissione (fee Smood) come
   differenza tra il **totale documento lordo** (somma di tutte le
   categorie delivery + hardware) e l’**importo a vostro favore** (netto
   payout). Registra una riga:
   * CtDare = 400010
   * CtAvere = 105010
   * Importo = commissione
   * Cod. IVA vuoto
   * Descrizione = "Costo servizio Smood"
4. **Incasso**: registra infine l’incasso netto:
   * CtDare = 100020
   * CtAvere = 105010
   * Importo = importo a vostro favore (payout)
   * Cod. IVA vuoto
   * Descrizione = "Incasso Smood – [mese]"

### Piattaforma Uber
Ogni fattura Uber (FHBCG…) produce tre righe:
1. **Ricavi**:
   * CtDare = 105010
   * CtAvere = 300030
   * Importo = importo lordo
   * Cod. IVA = F2
   * Descrizione = "Uber – [mese] [numero fattura]"
2. **Costo servizio**:
   * CtDare = 400010
   * CtAvere = 105010
   * Importo = marketplace fee
   * Cod. IVA vuoto
   * Descrizione = "Costo servizio Uber – [mese] [numero]"
3. **Incasso**:
   * CtDare = 100020
   * CtAvere = 105010
   * Importo = ricavi – fee
   * Cod. IVA vuoto
   * Descrizione = "Incasso Uber – [mese] [numero]"
   * La data dell’incasso è **un giorno prima** della data della fattura.

### Piattaforma Smartbox
Per ogni fattura Smartbox (PDN-…):
1. **Ricavi**:
   * CtDare = 105010
   * CtAvere = 300040
   * Importo = importo lordo
   * Cod. IVA = V0
   * Descrizione = "Smartbox"
2. **Costo servizio**:
   * CtDare = 400020
   * CtAvere = 105010
   * Importo = commissione Smartbox
   * Cod. IVA vuoto
   * Descrizione = "Costo servizio Smartbox"
3. **Incasso**:
   * CtDare = 100020
   * CtAvere = 105010
   * Importo = ricavi – commissione
   * Cod. IVA vuoto
   * Descrizione = "Incasso Smartbox fattura nr [numero]"
   * Usa la data indicata nella sezione incasso.

Applica sempre queste regole. Se il PDF non contiene abbastanza
informazioni per seguire le regole, inserisci una riga di errore.
"""
def get_examples(source: str, limit: int = 3) -> list[tuple[str, str]]:
    conn = db()
    cur = conn.execute(
        """SELECT model_csv, correct_csv
           FROM corrections
           WHERE source = ?
           ORDER BY id DESC
           LIMIT ?""",
        (source, limit)
    )
    return cur.fetchall()

@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Carica un PDF (application/pdf)")

    pdf_bytes = await file.read()
pdf_hash = sha256_bytes(pdf_bytes)
source = "unknown"  # per ora
examples = get_examples(source, limit=3)

few_shot = ""
for i, (model_csv, correct_csv) in enumerate(examples, start=1):
    few_shot += f"\nESEMPIO {i} (CSV sbagliato):\n{model_csv}\n"
    few_shot += f"ESEMPIO {i} (CSV corretto):\n{correct_csv}\n"

prompt_system = SYSTEM_PROMPT + "\n\nUSA QUESTI ESEMPI COME REGOLA:\n" + few_shot

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    try:
        uploaded = client.files.create(file=open(tmp_path, "rb"), purpose="assistants")
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": prompt_system},
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
@app.post("/feedback")
async def feedback(payload: FeedbackPayload):
    if not payload.correct_csv.strip():
        raise HTTPException(status_code=400, detail="correct_csv vuoto")

    conn = db()
    conn.execute(
        """INSERT INTO corrections(created_at, source, invoice_id, pdf_sha256, model_csv, correct_csv)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            datetime.datetime.utcnow().isoformat(),
            payload.source.lower().strip(),
            payload.invoice_id,
            payload.pdf_sha256,
            payload.model_csv,
            payload.correct_csv
        )
    )
    conn.commit()
    return {"ok": True}
