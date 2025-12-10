import os
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ======================
# CONFIGURACIÓN
#=======================

DATABASE_URL = os.environ.get("DATABASE_URL")
PG_TABLE_NAME = os.environ.get("PG_TABLE_NAME")

app = FastAPI()

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# ======================
# MAP AT->PG
# ======================

FIELDS_LIST = [
    "record_id",
    "Startup name",
    "PH1_Constitution_Location",
    "date_sourced"
]

FIELD_MAP = {
    k: k for k in FIELDS_LIST
}

# ======================
# MODELOS DE ENTRADA
#=======================

class AirtablePayload(BaseModel):
    """
    Ejemplo de payload que puedes mandar desde Airtable:
    {
    "id": "recXXXX",
    "fields": {
        "Name": "Carlos",
        "Email": "carlos@example.com"
        }
    }
    """
    id: str
    fields: Dict[str, Any]

def build_values_from_fields(fields: Dict[str, Any]):
    """
    Devuelve una lista de valores en el mismo orden que FIELD_MAP
    """
    return [fields.get(at_name) for at_name in FIELD_MAP.keys()]

def build_insert_query():
    cols = ["airtable_id"] + list(FIELD_MAP.values())
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    return f"""
        INSERT INTO {PG_TABLE_NAME} ({col_list})
        VALUES ({placeholders})
    """

def build_update_query():
    set_clause = ", ".join([f"{col} = %s" for col in FIELD_MAP.values()])
    return f"""
        UPDATE {PG_TABLE_NAME}
        SET {set_clause}
        WHERE airtable_id = %s
    """

# ======================
# ACCESO A POSTGRESQL
# ======================

def find_record_by_id(conn, airtable_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {PG_TABLE_NAME} WHERE airtable_id = %s",
            (airtable_id,)
        )
        row = cur.fetchone()
    return row # devuelve un dict o none

def create_record_in_postgres(conn, airtable_id: str, fields: Dict[str, Any]) -> None:
    values = [airtable_id] + build_values_from_fields(fields)
    query = build_insert_query()

    with conn.cursor() as cur:
        cur.execute(query, values)

def update_record_in_postgres(conn, airtable_id: str, fields: Dict[str, Any]) -> None:
    values = build_values_from_fields(fields) + [airtable_id]
    query = build_update_query()
    with conn.cursor() as cur:
        cur.execute(query, values)

# ======================
# LÓGICA DE SINCRONIZACIÓN
# ======================

def sync_airtable_record(conn, payload: AirtablePayload) -> str:
    """
    Decide si crear o actualizar el registro
    """
    airtable_id = payload.id
    fields = payload.fields

    existing = find_record_by_id(conn, airtable_id)

    if existing is None:
        create_record_in_postgres(conn, airtable_id, fields)
        action = "created record"
    else:
        update_record_in_postgres(conn, airtable_id, fields)
        action = "updated record"
    
    return action

# ======================
# ENDPOINT FASTAPI
# ======================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/airtable-webhook")
def airtable_webhook(payload: AirtablePayload):
    """
    Endpoint que llamas desde Airtable (Automation o Webhook)
    """
    conn = get_conn()
    try:
        action = sync_airtable_record(conn, payload)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    
    return {"success": True, "action": action}
