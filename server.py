# server.py — Poultry Fields API (v1.4.4)

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, List, Dict, Optional
import io, re, unicodedata, json, traceback
import pandas as pd
from datetime import datetime

# ---------------- Options ----------------
STRICT_USER_ID_ONLY       = True
FALLBACK_TO_INDICATORS    = False
STRIP_NON_SECTION_COLUMNS = True
DROP_LAST_N_ROWS          = 2

# أي تاريخ انتهاء أقدم من هذا يعتبر Placeholder (غير صالح)
MIN_VALID_EXP_YEAR        = 2000

# =============== Helpers ===============
def _to_ascii_digits(s: str) -> str:
    if not isinstance(s, str): s = str(s or "")
    s = s.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹","01234567890123456789"))
    return "".join(ch for ch in s if not unicodedata.category(ch).startswith("C"))

def _canon_token(v: Any) -> str:
    s = _to_ascii_digits(str(v or "")).strip().lower()
    s = re.sub(r"\s+"," ", s).replace("٬","").replace("،","")
    return s

def _canon_key(s: str) -> str:
    return re.sub(r"[^\w]+","", _canon_token(s))

def is_blank(v: Any) -> bool:
    return _canon_token(v) == ""

def _num_or_none(v: Any) -> Optional[float]:
    s = _canon_token(v)
    if not s: return None
    m = re.search(r"-?\d+(?:[.,]\d+)?", s)
    if not m: return None
    try: return float(m.group(0).replace(",", "."))
    except: return None

# قيم نصية شائعة للتواريخ غير الصالحة
BAD_DATE_TOKENS = {
    "", "0", "00/00/0000", "0000-00-00",
    "#value!", "#value", "value", "value!",
    # Placeholders شائعة من إكسل
    "1899-11-30", "1899-12-30", "1899-12-31",
    "30/11/1899", "31/12/1899"
}

def _dt_or_none(v: Any):
    """
    يحوّل القيم إلى datetime أو يرجّع None للقيم الفارغة/التالفة/القديمة جدًا.
    """
    s = _canon_token(v)
    if s in BAD_DATE_TOKENS:
        return None

    # Excel serial (مثل 45123 أو 45123.0)
    try:
        n = float(s)
        if 20000 < n < 80000:
            base = datetime(1899, 12, 30)
            dt = base + pd.to_timedelta(int(n), unit="D")
            if dt.year < MIN_VALID_EXP_YEAR:  # حماية إضافية
                return None
            return dt
    except:
        pass

    # إذا التاريخ ISO يبدأ بسنة (YYYY-..)، dayfirst=False
    iso_like = bool(re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?$", s))
    ts = pd.to_datetime(s, errors="coerce", dayfirst=not iso_like)
    if pd.isna(ts):
        return None
    try:
        dt = ts.to_pydatetime()
    except AttributeError:
        dt = ts if isinstance(ts, datetime) else None

    if dt and dt.year < MIN_VALID_EXP_YEAR:
        return None
    return dt

def _date_strict(v: Any) -> str:
    try:
        dt = _dt_or_none(v)
        return "" if dt is None else dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

# --------- Aliases ---------
COLUMN_ALIASES = {
    "Flock":  ["Flock","Flock Name","Flock ID","Flock Code","Flock No","القطيع","اسم القطيع"],
    "Date":   ["Date","Record Date","Entry Date","Report Date","Day","Day Date","Date/Time","التاريخ"],
    "User ID":["User ID","Creation User ID","UserID","Created By","Created By ID"],
}
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

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: re.sub(r"\s+"," ", str(c)).strip())
    low = {str(c).strip().lower(): c for c in df.columns}
    ren = {}
    for canon, aliases in COLUMN_ALIASES.items():
        for a in aliases:
            k = a.strip().lower()
            if k in low:
                ren[low[k]] = canon
                break
    return df.rename(columns=ren)

def normalize_domain_columns(df: pd.DataFrame) -> pd.DataFrame:
    low = {str(c).strip().lower(): c for c in df.columns}
    ren = {}
    for canon, aliases in {**CARE_COLUMN_ALIASES, **OP_COLUMN_ALIASES}.items():
        if canon in df.columns: continue
        for a in aliases:
            a_low = a.strip().lower()
            if a_low in low:
                ren[low[a_low]] = canon
                break
    return df.rename(columns=ren)

# --------- Indicators / Drops ----------
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
SHARED_ALWAYS_DROP = [
    "Last Mod Date","Creation User ID","Farm ID","Creation Date","Farm Name",
    "Growout ID","Farm Stage","Growout Name","Pen ID","Flock ID",
    "Transaction Time","Feed Inventory Delivery Status","Flock History","Ref #",
    "Void","Cycle",
]

def drop_columns_by_names(df: pd.DataFrame, names: List[str]) -> pd.DataFrame:
    target = {_canon_key(n) for n in names}
    keep = [c for c in df.columns if _canon_key(c) not in target]
    return df[keep]

# --------- Load Excel ---------
def load_report_from_excel(content: bytes, filename: Optional[str]) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(io.BytesIO(content))
        pick = None
        for s in xl.sheet_names:
            if s.strip().lower() in {"export","ag-grid"}:
                pick = s; break
        if pick is None: pick = xl.sheet_names[0]
        df = xl.parse(pick)
    except Exception as e:
        name = (filename or "").lower()
        hint = ""
        if name.endswith(".xls"):
            hint = " • Tip: convert this file to .xlsx before uploading."
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read Excel ({filename}): {e}{hint}"
        )

    # normalize + clean
    df = normalize_headers(df).fillna("")
    df = normalize_domain_columns(df)

    # نظّف تواريخ الانتهاء: حوّل قيم placeholder إلى فراغ
    for col in ["Medication Exp Date","Vaccination Exp Date"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: "" if (_dt_or_none(x) is None) else _date_strict(x))

    if DROP_LAST_N_ROWS and len(df) >= DROP_LAST_N_ROWS:
        df = df.iloc[:-DROP_LAST_N_ROWS, :].copy()
    return df

def strip_time_from_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in [c for c in df.columns if "date" in c.lower()]:
        df[c] = df[c].apply(_date_strict)
    return df

# --------- Split sections ---------
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
        if c in row.index and str(row.get(c, "")).strip() != "": return True
    return False

def split_sections(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
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
        op_df   = op_df.drop(columns=[c for c in CARE_INDICATORS if c in op_df.columns], errors="ignore")
        care_df = care_df.drop(columns=[c for c in OP_INDICATORS   if c in care_df.columns], errors="ignore")

    op_df   = drop_columns_by_names(op_df, SHARED_ALWAYS_DROP)
    care_df = drop_columns_by_names(care_df, SHARED_ALWAYS_DROP)

    op_df   = strip_time_from_date_columns(op_df)
    care_df = strip_time_from_date_columns(care_df)

    return {"operational": op_df, "care": care_df}

# --------- Operational Status ---------
OP_REQUIRED_NUMERIC = [
    "Animal Feed Consumed","Water Consumption","Temperature Low",
    "Temperature High","Ammonia Level","Humidity",
    "Light_Duration (HU)","Light intensity %","Animal Feed Inventory",
]
OP_REQUIRED_TEXT = ["Animal Feed Type Name","Female Feed Type ID","Female Feed Formula ID"]
PROD_CODE_RX = re.compile(r"f\d+[pr]", re.I)

def _is_prod_flock(name: Any) -> bool:
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
        if field in r and _num_or_none(r.get(field)) is not None: return True
    return False

def _group_has_text(rows: List[dict], field: str) -> bool:
    for r in rows:
        if field in r and not is_blank(r.get(field, "")): return True
    return False

def add_operational_status(op_df: pd.DataFrame) -> pd.DataFrame:
    if op_df.empty or ("Flock" not in op_df.columns) or ("Date" not in op_df.columns):
        op_df["Status"] = ""; return op_df
    op_df["_gkey"] = op_df["Flock"].astype(str).str.lower().str.strip() + "||" + op_df["Date"].astype(str)
    status_map = {}
    for key, g in op_df.groupby("_gkey", sort=False):
        if key.endswith("||"):
            status_map[key] = "ERROR: Date"; continue
        rows = [dict(r) for _, r in g.iterrows()]
        flock = str(g["Flock"].iloc[0])
        missing = []
        for col in OP_REQUIRED_NUMERIC:
            if not _group_has_numeric(rows, col): missing.append(col)
        for col in OP_REQUIRED_TEXT:
            if not _group_has_text(rows, col): missing.append(col)
        if _is_prod_flock(flock):
            if not _group_has_numeric(rows, "Table Eggs Prod"):
                missing.append("Table Eggs Prod")
        if missing:
            status_map[key] = "ERROR: " + ", ".join(sorted(set(missing)))
        else:
            notes = []
            for col in OPTIONAL_NOTE_FIELDS:
                if col in op_df.columns and (_group_has_text(rows, col) or _group_has_numeric(rows, col)):
                    notes.append(col)
            status_map[key] = "NOTE: " + ", ".join(sorted(set(notes))) if notes else "OK"
    op_df["Status"] = op_df["_gkey"].map(status_map).fillna("OK")
    return op_df.drop(columns=["_gkey"])

# --------- Care Status ---------
CARE_MED_REQ  = ["Medication","Medication Dose","Medication Batch","Medication Exp Date","Doctor Name"]
CARE_VACC_REQ = ["Vaccination","Vaccine Name","VaccinevDoze","VaccinationBatch","Vaccination Exp Date","Vacc Method","Vacc Type","Doctor Name"]
NUMERICISH_REQUIRED = {"Medication Dose","VaccinevDoze"}

ALLOWED_UNITS = {
    "ml": {"ml","ml.","mL","milliliter","milliliters","cc"},
    "mg": {"mg","mg.","milligram","milligrams"},
    "g":  {"g","g.","gram","grams"},
    "kg": {"kg","kg.","kilogram","kilograms"},
    "iu": {"iu","iu.","international unit","international units"},
    "dose": {"dose","doses"},
}
def _canon_unit(v: Any) -> str:
    s = _canon_token(v)
    for k, alts in ALLOWED_UNITS.items():
        if s in alts or s == k: return k
    return s

def _field_present(field: str, value: Any) -> bool:
    # اعتبر تواريخ الانتهاء موجودة فقط إذا كانت صالحة (ليس Placeholder)
    if field in ("Medication Exp Date", "Vaccination Exp Date"):
        return _dt_or_none(value) is not None
    if field in NUMERICISH_REQUIRED:
        n = _num_or_none(value)
        return (n is not None) and (n > 0)
    return not is_blank(value)

def _any_present(rows: List[dict], cols: List[str]) -> bool:
    return any(not is_blank(r.get(c, "")) for r in rows for c in cols)

PLACEHOLDER_BATCH_TOKENS = {
    "", "-", "—", "na", "n/a", "null", "none", "0", "00", "000", "xx", "xxx", "."
}
def _invalid_batch(v: Any) -> bool:
    s = _canon_token(v)
    if s in PLACEHOLDER_BATCH_TOKENS:
        return True
    s2 = re.sub(r"[^a-z0-9]", "", s)
    if not s2:
        return True
    if set(s2) == {"0"}:
        return True
    return len(s2) < 3

def add_care_status(care_df: pd.DataFrame) -> pd.DataFrame:
    if care_df.empty or ("Flock" not in care_df.columns) or ("Date" not in care_df.columns):
        care_df["Status"] = ""
        care_df["StatusReasonCodes"] = [[] for _ in range(len(care_df))]
        return care_df

    care_df["_gkey"] = care_df["Flock"].astype(str).str.lower().str.strip() + "||" + care_df["Date"].astype(str)
    status_map, codes_map = {}, {}

    for key, g in care_df.groupby("_gkey", sort=False):
        if key.endswith("||"):
            status_map[key] = "ERROR: Date"
            codes_map[key] = ["DATE_MISSING"]
            continue

        rows = [dict(r) for _, r in g.iterrows()]
        ref_dt = None
        for r in rows:
            ref_dt = _dt_or_none(r.get("Date"))
            if ref_dt: break

        med_intent  = _any_present(rows, ["Medication","Medication Dose","Medication Batch"])
        vacc_intent = _any_present(rows, ["Vaccination","Vaccine Name","VaccinevDoze","VaccinationBatch","Vacc Method","Vacc Type"])

        missing_med, missing_vacc = [], []
        extra_errors, codes = [], []

        # ===== Medication checks =====
        if med_intent:
            for col in CARE_MED_REQ:
                if not any(_field_present(col, r.get(col)) for r in rows):
                    missing_med.append(col)
                    codes.append("MED_MISSING_"+_canon_key(col).upper())

            if not any(not _invalid_batch(r.get("Medication Batch")) for r in rows):
                if "Medication Batch" not in missing_med: missing_med.append("Medication Batch")
                codes.append("MED_MISSING_BATCH")

            if ref_dt is not None:
                for r in rows:
                    ex = _dt_or_none(r.get("Medication Exp Date"))
                    if ex is not None and ex < ref_dt:
                        extra_errors.append(f"Medication → Expired batch ({ex.date()})")
                        codes.append("MED_EXPIRED")
                        break

        # ===== Vaccination checks =====
        if vacc_intent:
            for col in CARE_VACC_REQ:
                if not any(_field_present(col, r.get(col)) for r in rows):
                    missing_vacc.append(col)
                    codes.append("VACC_MISSING_"+_canon_key(col).upper())

            if not any(not _invalid_batch(r.get("VaccinationBatch")) for r in rows):
                if "VaccinationBatch" not in missing_vacc: missing_vacc.append("VaccinationBatch")
                codes.append("VACC_MISSING_BATCH")

            vacc_dose_any = any((_num_or_none(r.get("VaccinevDoze")) or 0) > 0 for r in rows)
            vacc_has_unit = any(_canon_unit(r.get("Doses Unit")) for r in rows if not is_blank(r.get("Doses Unit")))
            if vacc_dose_any and not vacc_has_unit:
                if "Doses Unit" not in missing_vacc: missing_vacc.append("Doses Unit")
                codes.append("VACC_MISSING_DOSE_UNIT")

            if ref_dt is not None:
                for r in rows:
                    ex = _dt_or_none(r.get("Vaccination Exp Date"))
                    if ex is not None and ex < ref_dt:
                        extra_errors.append(f"Vaccination → Expired batch ({ex.date()})")
                        codes.append("VACC_EXPIRED")
                        break

        if (med_intent and missing_med) or (vacc_intent and missing_vacc) or extra_errors:
            parts = []
            if missing_med:  parts.append("Medication → "  + ", ".join(sorted(set(missing_med))))
            if missing_vacc: parts.append("Vaccination → " + ", ".join(sorted(set(missing_vacc))))
            parts.extend(extra_errors)
            status_map[key] = "ERROR: " + " ; ".join(parts)
        else:
            if med_intent and not vacc_intent:
                status_map[key] = "NOTE: Medication only (complete)"
            elif (not med_intent) and vacc_intent:
                status_map[key] = "NOTE: Vaccination only (complete)"
            elif med_intent and vacc_intent:
                status_map[key] = "OK: Medication + Vaccination complete"
            else:
                status_map[key] = "OK: No care data"

        codes_map[key] = sorted(set(codes))

    care_df["Status"] = care_df["_gkey"].map(status_map).fillna("OK: No care data")
    care_df["StatusReasonCodes"] = care_df["_gkey"].map(codes_map).apply(lambda v: v if isinstance(v, list) else [])
    return care_df.drop(columns=["_gkey"])

# --------- Utilities / API ---------
def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return [] if df is None or df.empty else json.loads(df.to_json(orient="records"))

app = FastAPI(title="Poultry Fields API", version="1.4.4")
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
    try:
        content = await file.read()
        df = load_report_from_excel(content, file.filename)
        df = strip_time_from_date_columns(df)
        sections = split_sections(df)
        op_df   = add_operational_status(sections["operational"])
        care_df = add_care_status(sections["care"])
        return {"ops": df_to_records(op_df), "care": df_to_records(care_df)}
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
