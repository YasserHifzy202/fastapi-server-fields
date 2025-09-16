# C:\flutterapps\database_update\Backend\fastapi-server-fields\server.py
# =============================================================================
# FastAPI API لتغليف منطق التقسيم والتنظيف وإضافة "Status"
# Endpoints:
#   GET  /health   -> فحص سريع
#   POST /analyze  -> يقرأ ملف Excel ويُرجع {"ops": [...], "care": [...]}
# ملاحظات:
# - كشف التكرار يبقى كما هو.
# - Status إنجليزي فقط:  ERROR / NOTE / OK  مع أسباب واضحة.
# - في الرعاية: أي نقص بواحد من REQUIRED مع وجود intent => ERROR.
#   Medication-only مكتمل => NOTE. غير ذلك => OK (مع سبب).
# - الجرعات الرقمية إذا كانت 0 تعتبر ناقصة.
# =============================================================================

from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, List, Dict, Tuple
import io, re, unicodedata, json
import pandas as pd

# ---------------- Options ----------------
STRICT_USER_ID_ONLY       = True
FALLBACK_TO_INDICATORS    = False
STRIP_NON_SECTION_COLUMNS = True
DROP_LAST_N_ROWS          = 2
ZERO_DATE_VALUE           = ""

# ===================== Helpers / Normalization =====================
def _to_ascii_digits(s: str) -> str:
    if not isinstance(s, str): s = str(s or "")
    s = s.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹","01234567890123456789"))
    return "".join(ch for ch in s if not unicodedata.category(ch).startswith("C"))

def _canon_token(s: Any) -> str:
    s = _to_ascii_digits(str(s or "").strip().lower())
    s = re.sub(r"\s+", " ", s)
    return s.replace("٬","").replace("،","")

def _canon_key(s: str) -> str:
    s = _canon_token(s)
    return re.sub(r"[^\w]+", "", s)

def is_blank(v: Any) -> bool:
    return _canon_token(v) == ""

def _num_or_none(v: Any):
    s = _canon_token(v)
    if not s: return None
    m = re.search(r"-?\d+(?:[.,]\d+)?", s)
    if not m: return None
    try:
        return float(m.group(0).replace(",", "."))
    except:
        return None

# --------- Aliases لرؤوس أساسية ---------
COLUMN_ALIASES = {
    "Flock":  ["Flock","Flock Name","Flock ID","Flock Code","Flock No","القطيع","اسم القطيع"],
    "Date":   ["Date","Record Date","Entry Date","Report Date","Day","Day Date","Date/Time","التاريخ"],
    "User ID":["User ID","Creation User ID","UserID","Created By","Created By ID"],
}
def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: re.sub(r"\s+"," ", str(c)).strip())
    low = {str(c).strip().lower(): c for c in df.columns}
    mapping = {}
    for canon, aliases in COLUMN_ALIASES.items():
        for a in aliases:
            k = a.strip().lower()
            if k in low:
                mapping[low[k]] = canon
                break
    return df.rename(columns=mapping)

# --------- Aliases لأعمدة الدومين ---------
CARE_COLUMN_ALIASES = {
    "Medication":             ["Medicine Name","Drug","Med Name"],
    "Medication Dose":        ["Med Dose","Dose","Dose Qty","Medicine Dose"],
    "Medication Batch":       ["Med Batch","Medicine Batch"],
    "Medication Exp Date":    ["Med Exp Date","Medicine Expiry","Med Expiry Date"],
    "Doses Unit":             ["Dose Unit","Dosing Unit","Units"],
    "Doctor Name":            ["Veterinarian","Vet Name"],
    "Vaccination":            ["Vaccine Given","Vaccination Name"],
    "Vaccine Name":           ["Vaccine"],
    "VaccinevDoze":           ["Vaccine Dose","Vaccine Dose Qty","VaccineDoze","Vaccine Dose (Qty)"],
    "VaccinationBatch":       ["Vaccination Batch","Vaccine Batch"],
    "Vaccination Exp Date":   ["Vacc Exp Date","Vaccine Expiry","Vacc Expiry Date"],
    "Vacc Method":            ["Vaccination Method","Method of Vaccination"],
    "Vacc Type":              ["Vaccination Type"],
}
OP_COLUMN_ALIASES = {
    "Light_Duration (HU)": ["Light Duration (HU)","Light Duration HU","Light Duration"],
    "Light intensity %":   ["Light Intensity %","Light intensity%","Light Intensity%"],
    "Table Eggs Prod":     ["Table Eggs Production","Table Eggs"],
    "Egg Weight Table_Egg":["Egg Weight","Table Egg Weight","Egg Weight Table Egg"],
    "Animal CV Uniformity":["CV Uniformity","Uniformity CV"],
}
def normalize_domain_columns(df: pd.DataFrame) -> pd.DataFrame:
    low = {str(c).strip().lower(): c for c in df.columns}
    ren = {}
    for canon, aliases in {**CARE_COLUMN_ALIASES, **OP_COLUMN_ALIASES}.items():
        if canon in df.columns:
            continue
        for a in aliases:
            a_low = a.strip().lower()
            if a_low in low:
                ren[low[a_low]] = canon
                break
    return df.rename(columns=ren)

# --------- مؤشرات ---------
CARE_INDICATORS = [
    "Medication","Medication Dose","Medication Batch","Medication Exp Date",
    "Vaccination","Vaccine Name","VaccinevDoze","VaccinationBatch",
    "Vaccination Exp Date","Vacc Method","Vacc Type","Doses Unit","Doctor Name",
]
OP_INDICATORS = [
    "Animal Feed Consumed","Water Consumption","Temperature Low","Temperature High",
    "Ammonia Level","Humidity","Light_Duration (HU)","Light intensity %",
    "Animal Feed Inventory","Table Eggs Prod","Supplied Feed","Feed Received (Kg)",
    "Animal Weight","Animal Uniformity","Animal Mortality","Animals Culled",
    "Animal Feed Type Name","Female Feed Type ID","Female Feed Formula ID",
    "Animal Feed Formula Name","House ID","Post Status","Egg Weight Table_Egg",
    "Animal CV Uniformity","Animals Added",
]

# --------- أعمدة تُحذف من القسمين ---------
SHARED_ALWAYS_DROP = [
    "Last Mod Date","Creation User ID","Farm ID","Creation Date","Farm Name",
    "Growout ID","Farm Stage","Growout Name","Pen ID","Flock ID",
    "Transaction Time","Feed Inventory Delivery Status","Flock History","Ref #",
    "Void","Cycle",
]
def drop_columns_by_names(df: pd.DataFrame, names: List[str]) -> pd.DataFrame:
    target = {_canon_key(n) for n in names}
    keep_cols = []
    for c in df.columns:
        if _canon_key(c) in target:
            continue
        keep_cols.append(c)
    return df[keep_cols]

# --------- فصل بالرعاية عبر User ID ---------
CARE_UID_PATTERNS = [
    re.compile(r"layer\s*vet(\s*pm\d*)?$", re.I),
    re.compile(r"^lvetp\d*$", re.I),
    re.compile(r"^lvetr\d*$", re.I),
]
def is_care_user(uid: Any) -> bool:
    u = _canon_token(uid)
    if not u: return False
    return any(rx.search(u) for rx in CARE_UID_PATTERNS) or ("layer vet" in u)

def row_has_any_value(row: pd.Series, cols: List[str]) -> bool:
    for c in cols:
        if c in row.index and str(row.get(c, "")).strip() != "":
            return True
    return False

# --------- تحميل ---------
def load_report_from_excel(content: bytes) -> pd.DataFrame:
    xl = pd.ExcelFile(io.BytesIO(content))
    pick = None
    for s in xl.sheet_names:
        if s.strip().lower() in {"export","ag-grid"}:
            pick = s; break
    if pick is None: pick = xl.sheet_names[0]
    df = xl.parse(pick)
    df = normalize_headers(df).fillna("")
    df = normalize_domain_columns(df)
    if DROP_LAST_N_ROWS and len(df) >= DROP_LAST_N_ROWS:
        df = df.iloc[:-DROP_LAST_N_ROWS, :].copy()
    return df

# --------- توحيد التواريخ ---------
def strip_time_from_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_cols = [c for c in df.columns if "date" in c.lower()]
    for c in date_cols:
        parsed = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
        ok = parsed.notna()
        df.loc[ok, c] = parsed.loc[ok].dt.strftime("%Y-%m-%d")
        df.loc[~ok, c] = ""
        df[c] = df[c].astype(str)
    return df

# --------- كشف أعمدة كلها صفر ---------
def _extract_number_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).map(_to_ascii_digits).str.replace("٬","", regex=False).str.replace("،","", regex=False)
    num = s.str.extract(r'(-?\d+(?:[.,]\d+)?)', expand=False)
    if num is None:
        return pd.Series([pd.NA]*len(s), index=s.index)
    num = num.str.replace(",", ".", regex=False)
    return pd.to_numeric(num, errors='coerce')

PROTECTED_COLS = {"Flock","Date","User ID"}
def drop_all_zero_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    drop_cols = []
    for col in df.columns:
        if col in PROTECTED_COLS:
            continue
        ser = df[col]
        if pd.api.types.is_numeric_dtype(ser):
            vals = ser.dropna()
            if len(vals) and (vals == 0).all():
                drop_cols.append(col)
        else:
            nums = _extract_number_series(ser)
            mask = nums.notna()
            if mask.any() and nums[mask].eq(0).all():
                drop_cols.append(col)
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df, drop_cols

def strip_care_from_operational(op_df: pd.DataFrame) -> pd.DataFrame:
    care_cols = [c for c in CARE_INDICATORS if c in op_df.columns]
    return op_df.drop(columns=care_cols) if care_cols else op_df

def strip_operational_from_care(care_df: pd.DataFrame) -> pd.DataFrame:
    op_cols = [c for c in OP_INDICATORS if c in care_df.columns]
    return care_df.drop(columns=op_cols) if op_cols else care_df

# --------- Split sections ---------
def split_sections(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = df.copy()
    uid_col = "User ID" if "User ID" in df.columns else None

    if uid_col and STRICT_USER_ID_ONLY:
        care_mask = df[uid_col].apply(is_care_user)
    else:
        care_by_uid = df[uid_col].apply(is_care_user) if uid_col else pd.Series(False, index=df.index)
        care_by_ind = df.apply(lambda r: row_has_any_value(r, CARE_INDICATORS), axis=1) if FALLBACK_TO_INDICATORS else pd.Series(False, index=df.index)
        care_mask = care_by_uid | care_by_ind

    care_df = df[care_mask].copy()
    op_df   = df[~care_mask].copy()

    if STRIP_NON_SECTION_COLUMNS:
        op_df   = strip_care_from_operational(op_df)
        care_df = strip_operational_from_care(care_df)

    op_df   = drop_columns_by_names(op_df, SHARED_ALWAYS_DROP)
    care_df = drop_columns_by_names(care_df, SHARED_ALWAYS_DROP)

    # Clean Vaccination Exp Date (bad tokens)
    if "Vaccination Exp Date" in care_df.columns:
        s = care_df["Vaccination Exp Date"].astype(str).str.strip()
        bad_mask = s.str.upper().isin({"#VALUE!","#VALUE","VALUE!","VALUE"}) | s.isin(["0","00/00/0000","0000-00-00"])
        care_df.loc[bad_mask, "Vaccination Exp Date"] = ""

    op_df   = strip_time_from_date_columns(op_df)
    care_df = strip_time_from_date_columns(care_df)

    if ZERO_DATE_VALUE and "Vaccination Exp Date" in care_df.columns:
        z = care_df["Vaccination Exp Date"].astype(str).str.strip()
        mask = (z == "")
        care_df.loc[mask, "Vaccination Exp Date"] = ZERO_DATE_VALUE

    return {"operational": op_df, "care": care_df}

# ===================== Operational Status =====================
OP_REQUIRED_NUMERIC = [
    "Animal Feed Consumed","Water Consumption","Temperature Low",
    "Temperature High","Ammonia Level","Humidity",
    "Light_Duration (HU)","Light intensity %","Animal Feed Inventory",
]
OP_REQUIRED_TEXT = [
    "Animal Feed Type Name","Female Feed Type ID","Female Feed Formula ID",
]
PROD_CODE_RX = re.compile(r"f\d+[pr]", re.I)
def is_production_flock_name(name: Any) -> bool:
    s = _canon_token(name)
    if "clpf" in s: return True
    if "clrf" in s: return False
    m = PROD_CODE_RX.search(s)
    if m: return m.group(0)[-1].lower() == "p"
    return False

OPTIONAL_NOTE_FIELDS = [
    "Animal Mortality","Animals Culled","Supplied Feed","Animal Weight",
    "Animal Uniformity","Egg Weight Table_Egg","Animal CV Uniformity","Animals Added",
]
def _group_has_numeric(rows: List[dict], field: str) -> bool:
    for r in rows:
        if field in r and _num_or_none(r.get(field)) is not None:
            return True
    return False
def _group_has_text(rows: List[dict], field: str) -> bool:
    for r in rows:
        if field in r and not is_blank(r.get(field, "")):
            return True
    return False

def add_operational_status(op_df: pd.DataFrame) -> pd.DataFrame:
    if op_df.empty or ("Flock" not in op_df.columns) or ("Date" not in op_df.columns):
        op_df["Status"] = ""
        return op_df

    op_df["_gkey"] = op_df["Flock"].astype(str).str.lower().str.strip() + "||" + op_df["Date"].astype(str)
    status_map: Dict[str,str] = {}

    for key, g in op_df.groupby("_gkey", sort=False):
        if key.endswith("||"):
            status_map[key] = "ERROR: Date"
            continue
        rows = [dict(r) for _, r in g.iterrows()]
        flock_name = str(g["Flock"].iloc[0])

        missing: List[str] = []
        for col in OP_REQUIRED_NUMERIC:
            if not _group_has_numeric(rows, col):
                missing.append(col)
        for col in OP_REQUIRED_TEXT:
            if not _group_has_text(rows, col):
                missing.append(col)
        if is_production_flock_name(flock_name):
            if not _group_has_numeric(rows, "Table Eggs Prod"):
                missing.append("Table Eggs Prod")

        if missing:
            status_map[key] = "ERROR: " + ", ".join(sorted(set(missing)))
        else:
            present_notes = []
            for col in OPTIONAL_NOTE_FIELDS:
                if col in op_df.columns and (_group_has_text(rows, col) or _group_has_numeric(rows, col)):
                    present_notes.append(col)
            status_map[key] = "NOTE: " + ", ".join(sorted(set(present_notes))) if present_notes else "OK"

    op_df["Status"] = op_df["_gkey"].map(status_map).fillna("OK")
    return op_df.drop(columns=["_gkey"])

# ===================== Care Status =====================
CARE_MED_REQ = [
    "Medication","Medication Dose","Medication Batch","Medication Exp Date","Doctor Name",
]
CARE_VACC_REQ = [
    "Vaccination","Vaccine Name","VaccinevDoze","VaccinationBatch",
    "Vaccination Exp Date","Vacc Method","Vacc Type","Doses Unit","Doctor Name",
]
NUMERICISH_REQUIRED = {"Medication Dose","VaccinevDoze"}  # must be > 0

def _field_present(field: str, value: Any) -> bool:
    if field in NUMERICISH_REQUIRED:
        n = _num_or_none(value)
        return (n is not None) and (n > 0)
    return not is_blank(value)

def _any_present(rows: List[dict], cols: List[str]) -> bool:
    return any(not is_blank(r.get(c, "")) for r in rows for c in cols)

def add_care_status(care_df: pd.DataFrame) -> pd.DataFrame:
    if care_df.empty or ("Flock" not in care_df.columns) or ("Date" not in care_df.columns):
        care_df["Status"] = ""
        return care_df

    care_df["_gkey"] = care_df["Flock"].astype(str).str.lower().str.strip() + "||" + care_df["Date"].astype(str)
    status_map: Dict[str,str] = {}

    for key, g in care_df.groupby("_gkey", sort=False):
        if key.endswith("||"):
            status_map[key] = "ERROR: Date"
            continue

        rows = [dict(r) for _, r in g.iterrows()]

        # intent
        med_intent  = _any_present(rows, ["Medication","Medication Dose","Medication Batch","Medication Exp Date"])
        vacc_intent = _any_present(rows, ["Vaccination","Vaccine Name","VaccinevDoze","VaccinationBatch","Vaccination Exp Date","Vacc Method","Vacc Type"])

        missing_med: List[str]  = []
        missing_vacc: List[str] = []

        if med_intent:
            for col in CARE_MED_REQ:
                if not any(_field_present(col, r.get(col)) for r in rows):
                    missing_med.append(col)
        if vacc_intent:
            for col in CARE_VACC_REQ:
                if not any(_field_present(col, r.get(col)) for r in rows):
                    missing_vacc.append(col)

        # decide
        if (med_intent and missing_med) or (vacc_intent and missing_vacc):
            parts = []
            if missing_med:
                parts.append("Medication → " + ", ".join(missing_med))
            if missing_vacc:
                parts.append("Vaccination → " + ", ".join(missing_vacc))
            status_map[key] = "ERROR: " + " ; ".join(parts)
        else:
            if med_intent and not vacc_intent:
                status_map[key] = "NOTE: Medication only (complete)"
            elif (not med_intent) and vacc_intent:
                status_map[key] = "OK: Vaccination only (complete)"
            elif med_intent and vacc_intent:
                status_map[key] = "OK: Medication + Vaccination complete"
            else:
                status_map[key] = "OK: No care data"

    care_df["Status"] = care_df["_gkey"].map(status_map).fillna("OK: No care data")
    return care_df.drop(columns=["_gkey"])

# ===================== Utilities =====================
def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))

# ===================== FastAPI App =====================
app = FastAPI(title="Poultry Fields API", version="1.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True, "service": "Poultry Fields API"}

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    """
    يستقبل ملف Excel (اسم الحقل 'file') ويعيد JSON:
      { "ops": [...], "care": [...] }
    """
    content = await file.read()

    # 1) load & normalize
    df = load_report_from_excel(content)
    df = strip_time_from_date_columns(df)

    # 2) split
    sections = split_sections(df)

    # 3) drop all-zero columns
    op_df, _   = drop_all_zero_columns(sections["operational"])
    care_df, _ = drop_all_zero_columns(sections["care"])

    # 4) add Status
    op_df   = add_operational_status(op_df)
    care_df = add_care_status(care_df)

    return {
        "ops": df_to_records(op_df),
        "care": df_to_records(care_df),
    }

# run:  python server.py
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
