from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import requests
import uuid
import json
import re
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from copy import deepcopy
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "https://сапфиров.рф",
        "https://www.сапфиров.рф",
        "https://xn--80aerydhd0a.xn--p1ai",
        "https://www.xn--80aerydhd0a.xn--p1ai"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

if not DEEPSEEK_API_KEY:
    raise RuntimeError("DEEPSEEK_API_KEY not found in .env")

SMTP_HOST = "smtp.mail.ru"
SMTP_PORT = 465
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_TO = SMTP_USER

if not SMTP_USER or not SMTP_PASSWORD:
    raise RuntimeError("SMTP_USER or SMTP_PASSWORD not found in .env")

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

chat_logger = logging.getLogger("chat_logger")
chat_logger.setLevel(logging.INFO)
chat_handler = RotatingFileHandler(f"{LOG_DIR}/chat.jsonl", maxBytes=10_485_760, backupCount=5)
chat_handler.setFormatter(logging.Formatter('%(message)s'))
chat_logger.addHandler(chat_handler)

lead_logger = logging.getLogger("lead_logger")
lead_logger.setLevel(logging.INFO)
lead_handler = RotatingFileHandler(f"{LOG_DIR}/leads.jsonl", maxBytes=10_485_760, backupCount=5)
lead_handler.setFormatter(logging.Formatter('%(message)s'))
lead_logger.addHandler(lead_handler)

def log_event(event_type: str, data: dict):
    record = {
        "ts": datetime.now().isoformat(),
        "event": event_type,
        **data
    }
    chat_logger.info(json.dumps(record, ensure_ascii=False))

def log_lead(data: dict):
    record = {
        "ts": datetime.now().isoformat(),
        **data
    }
    lead_logger.info(json.dumps(record, ensure_ascii=False))

def send_email_lead(payload: dict):
    subject = f"Заявка из бота: {payload.get('case_family', 'unknown')}"
    body = payload.get("message", "Нет текста")
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = SMTP_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        log_event("email_sent", {"to": SMTP_TO, "subject": subject})
        return True
    except Exception as e:
        log_event("email_error", {"error": str(e)})
        return False

class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None

ALLOWED_OBJECT_TYPES = {
    "warehouse", "production", "shop", "office", "building", "part_of_building",
    "land_plot", "house", "apartment", "dacha", "garage", "other_real_estate", "unknown"
}
ALLOWED_PROPERTY_SEGMENTS = {"commercial", "residential", "mixed", "land", "unknown"}
ALLOWED_ISSUE_TYPES = {
    "unauthorized_construction", "redevelopment", "reconstruction", "registration_absent",
    "registration_refusal", "document_mismatch", "ownership_problem", "land_problem",
    "bank_problem", "transaction_block", "inheritance_real_estate", "shared_ownership_problem",
    "boundary_problem", "lease_problem", "gas_connection_block", "other_real_estate_issue", "unknown"
}
ALLOWED_GOALS = {
    "registration", "sale", "bank_pledge", "risk_reduction", "deal_preparation",
    "legalization", "court_protection", "ownership_confirmation", "gas_connection", "other", "unknown"
}
ALLOWED_DOCUMENTS = {
    "egrn_extract", "technical_plan", "bti_plan", "project_docs", "refusal_letter",
    "court_docs", "lease_docs", "land_docs", "construction_permit", "none", "other"
}
ALLOWED_YES_NO_UNKNOWN = {"yes", "no", "unknown"}
ALLOWED_CLIENT_ROLE = {"owner", "buyer", "tenant", "lawyer", "representative", "heir", "co_owner", "unknown"}
ALLOWED_RIGHTS_STATUS = {"owned", "leased", "shared", "not_registered", "unknown"}
ALLOWED_CONTEXT = {"bank_refusal", "sale_blocked", "deal_risk", "inspection_risk", "none", "unknown"}
ALLOWED_CASE_FAMILY = {
    "commercial_redevelopment", "commercial_new_building", "commercial_document_mismatch",
    "commercial_bank_block", "residential_house_registration", "residential_dacha_registration",
    "gas_connection_house", "land_boundary_issue", "inheritance_real_estate", "shared_ownership_issue",
    "lease_problem", "other_real_estate_case", "unknown"
}
ALLOWED_CONSULTATION_MODE = {"two_paths", "one_path_plus_check", "document_first", "clarify_first", "reject", "unknown"}

QUESTION_TEXTS = {
    "contact": "Если хотите точный разбор по документам, оставьте телефон. После этого можно будет перейти к следующему шагу: кадастровый номер, выписка ЕГРН, техплан или другие документы по объекту.",
    "email_address": "Укажите ваш email, чтобы я отправил полный разбор:",
}

sessions = {}
MAX_CLARIFICATION_ATTEMPTS = 1

EXTRACTION_PROMPT = """
Ты извлекаешь структурированные данные из сообщения клиента по юридическим вопросам, связанным с недвижимостью.

Задача:
- извлечь только факты, которые явно следуют из сообщения;
- не придумывать данные;
- если данных нет, ставить null;
- отвечать только валидным JSON;
- никаких комментариев, markdown и текста вокруг JSON.

Если сообщение короткое (1–3 слова) и оно похоже на географическое название, населённый пункт, район, СНТ, улицу – помещайте его в location_description.
Если пользователь явно отвечает на предыдущий вопрос о местоположении, то заполняйте location_description.
Не перезаписывайте уже существующие более подробные данные, если новое сообщение не является уточнением.

Если пользователь говорит о несоответствии документов реальному состоянию (техплан, БТИ, ЕГРН) — ставь issue_type = "document_mismatch".
Если пользователь говорит о производственном здании, цехе, складе — ставь object_type = "production" или "warehouse".

Допустимая схема ответа:

{
  "raw_user_problem": null,
  "normalized_problem": null,
  "object_type": null,
  "property_segment": null,
  "issue_type": null,
  "goal": null,
  "location_description": null,
  "region": null,
  "settlement": null,
  "address": null,
  "cadastral_number": null,
  "cadastral_status": null,
  "documents": [],
  "has_refusals_or_disputes": null,
  "client_role": null,
  "contact": null,
  "contact_type": null,
  "property_rights_status": null,
  "land_rights_status": null,
  "bank_or_transaction_context": null
}

Допустимые значения enum:
object_type: warehouse, production, shop, office, building, part_of_building, land_plot, house, apartment, dacha, garage, other_real_estate, unknown
property_segment: commercial, residential, mixed, land, unknown
issue_type: unauthorized_construction, redevelopment, reconstruction, registration_absent, registration_refusal, document_mismatch, ownership_problem, land_problem, bank_problem, transaction_block, inheritance_real_estate, shared_ownership_problem, boundary_problem, lease_problem, gas_connection_block, other_real_estate_issue, unknown
goal: registration, sale, bank_pledge, risk_reduction, deal_preparation, legalization, court_protection, ownership_confirmation, gas_connection, other, unknown
documents: egrn_extract, technical_plan, bti_plan, project_docs, refusal_letter, court_docs, lease_docs, land_docs, construction_permit, none, other
has_refusals_or_disputes: yes, no, unknown
client_role: owner, buyer, tenant, lawyer, representative, heir, co_owner, unknown
contact_type: phone, telegram, unknown
property_rights_status: owned, leased, shared, not_registered, unknown
land_rights_status: owned, leased, not_registered, unknown
bank_or_transaction_context: bank_refusal, sale_blocked, deal_risk, inspection_risk, none, unknown
cadastral_status: provided, absent, unknown, not_applicable
""".strip()

def empty_answers():
    return {
        "raw_user_problem": None,
        "normalized_problem": None,
        "object_type": None,
        "property_segment": None,
        "issue_type": None,
        "goal": None,
        "location_description": None,
        "region": None,
        "settlement": None,
        "address": None,
        "cadastral_number": None,
        "cadastral_status": None,
        "documents": [],
        "has_refusals_or_disputes": None,
        "client_role": None,
        "contact": None,
        "contact_type": None,
        "property_rights_status": None,
        "land_rights_status": None,
        "bank_or_transaction_context": None,
        "construction_date": None,
    }

def empty_consultation_plan():
    return {
        "case_family": "unknown",
        "consultation_mode": "unknown",
        "primary_path": None,
        "secondary_path": None,
        "primary_path_short": None,
        "secondary_path_short": None,
        "need_to_check": [],
        "request_contact_after_reply": False,
    }

def new_session():
    return {
        "state": "intake",
        "lead_category": None,
        "clarification_attempts": 0,
        "consultation_ready": False,
        "form_sent": False,
        "history": [],
        "answers": empty_answers(),
        "consultation_plan": empty_consultation_plan(),
        "final_payload": {},
        "finished": False,
        "nontarget_issued": False,
        "last_question": None,
        "preliminary_sent": False,
    }

def normalize_str(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None

def normalize_enum(value, allowed, default="unknown"):
    value = normalize_str(value)
    if not value:
        return None
    return value if value in allowed else default

def normalize_documents(value):
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        item = normalize_str(item)
        if not item:
            continue
        result.append(item if item in ALLOWED_DOCUMENTS else "other")
    seen = set()
    unique = []
    for item in result:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique

def sanitize_extracted(data):
    data = data or {}
    return {
        "raw_user_problem": normalize_str(data.get("raw_user_problem")),
        "normalized_problem": normalize_str(data.get("normalized_problem")),
        "object_type": normalize_enum(data.get("object_type"), ALLOWED_OBJECT_TYPES),
        "property_segment": normalize_enum(data.get("property_segment"), ALLOWED_PROPERTY_SEGMENTS),
        "issue_type": normalize_enum(data.get("issue_type"), ALLOWED_ISSUE_TYPES),
        "goal": normalize_enum(data.get("goal"), ALLOWED_GOALS),
        "location_description": normalize_str(data.get("location_description")),
        "region": normalize_str(data.get("region")),
        "settlement": normalize_str(data.get("settlement")),
        "address": normalize_str(data.get("address")),
        "cadastral_number": normalize_str(data.get("cadastral_number")),
        "cadastral_status": normalize_enum(data.get("cadastral_status"), {"provided", "absent", "unknown", "not_applicable"}),
        "documents": normalize_documents(data.get("documents")),
        "has_refusals_or_disputes": normalize_enum(data.get("has_refusals_or_disputes"), ALLOWED_YES_NO_UNKNOWN),
        "client_role": normalize_enum(data.get("client_role"), ALLOWED_CLIENT_ROLE),
        "contact": normalize_str(data.get("contact")),
        "contact_type": normalize_enum(data.get("contact_type"), {"phone", "telegram", "unknown"}),
        "property_rights_status": normalize_enum(data.get("property_rights_status"), ALLOWED_RIGHTS_STATUS),
        "land_rights_status": normalize_enum(data.get("land_rights_status"), {"owned", "leased", "not_registered", "unknown"}),
        "bank_or_transaction_context": normalize_enum(data.get("bank_or_transaction_context"), ALLOWED_CONTEXT),
    }

def normalize_json_text(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()
    return text

def safe_parse_json(text: str):
    return json.loads(normalize_json_text(text))

def call_deepseek(messages, timeout=25, max_tokens=1200):
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "deepseek-chat",
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    try:
        response = requests.post(DEEPSEEK_URL, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        raise TimeoutError(f"DeepSeek API timeout after {timeout} seconds")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"DeepSeek request failed: {str(e)}")

def extract_fields(user_message: str, current_answers: dict) -> dict:
    messages = [
        {"role": "system", "content": EXTRACTION_PROMPT},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "current_known_answers": current_answers,
                    "new_user_message": user_message,
                },
                ensure_ascii=False,
            ),
        },
    ]
    raw = call_deepseek(messages, timeout=20, max_tokens=700)
    parsed = safe_parse_json(raw)
    return sanitize_extracted(parsed)

def merge_answers(existing: dict, extracted: dict) -> dict:
    merged = deepcopy(existing)
    for key, value in extracted.items():
        if key == "documents":
            if value:
                if not merged["documents"] or merged["documents"] == ["none"]:
                    merged["documents"] = value
                else:
                    combined = merged["documents"] + value
                    seen = set()
                    unique = []
                    for item in combined:
                        if item not in seen:
                            seen.add(item)
                            unique.append(item)
                    merged["documents"] = unique
            continue
        if value is None:
            continue
        if key in {"raw_user_problem", "normalized_problem", "location_description"}:
            if merged[key] is None:
                merged[key] = value
            elif len(value) > len(merged[key]):
                merged[key] = value
            continue
        if merged[key] is None:
            merged[key] = value
        elif merged[key] == "unknown" and value != "unknown":
            merged[key] = value
    return merged

def looks_like_real_estate(answers: dict) -> bool:
    text = " ".join(
        [
            answers.get("raw_user_problem") or "",
            answers.get("normalized_problem") or "",
            answers.get("location_description") or "",
            answers.get("region") or "",
            answers.get("settlement") or "",
            answers.get("address") or "",
            answers.get("cadastral_number") or "",
        ]
    ).lower()
    non_target_patterns = [
        "собака", "кошка", "животн", "сосед", "шум", "запах", "ремонт телевизор", "медиаплеер",
        "бытовая техника", "рецепт", "суп", "каша", "стирка", "уборка", "игрушка", "живу в квартире"
    ]
    real_estate_keywords = [
        "межевание", "граница участка", "сосед залез", "забор", "смежный участок",
        "наложение границ", "границы не совпадают", "долевая собственность", "второй собственник",
        "лицевой счет", "коммуналка", "не платит коммуналку"
    ]
    if any(k in text for k in real_estate_keywords):
        return True
    if any(p in text for p in non_target_patterns):
        return False
    strong_legal = ["банк", "росреестр", "залог", "отказ", "кредит", "экспертиз", "суд", "иск"]
    if any(k in text for k in strong_legal):
        return True
    legal_keywords = [
        "регистрац", "право", "документ", "сделка", "газ", "переплан", "реконструк",
        "кадастр", "земл", "доля", "наследств", "техплан", "бти", "егрн"
    ]
    object_keywords = [
        "квартир", "дом", "дача", "участок", "склад", "производ", "цех", "офис", "здани",
        "помещен", "недвижим"
    ]
    has_legal = any(k in text for k in legal_keywords)
    has_object = any(k in text for k in object_keywords)
    if has_object and has_legal:
        return True
    if answers.get("object_type") not in (None, "unknown") or answers.get("issue_type") not in (None, "unknown"):
        return True
    return False

def classify_lead(answers: dict) -> str:
    if not looks_like_real_estate(answers):
        return "nontarget"
    if answers.get("contact") and answers.get("location_description") and answers.get("raw_user_problem"):
        return "target"
    return "partial"

def detect_contact(text: str) -> tuple[str | None, str]:
    text = text.strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) == 11 and digits[0] in ("7", "8"):
        return text, "phone"
    if len(digits) == 10:
        return text, "phone"
    return None, "unknown"

def detect_email(text: str) -> str | None:
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    return match.group(0) if match else None

def normalize_location(loc: str) -> str:
    if not loc:
        return "не указана"
    loc = loc.strip().lower()
    if loc.startswith("в "):
        loc = loc[2:].strip()
    if loc.startswith("на "):
        loc = loc[3:].strip()
    if loc:
        loc = loc[0].upper() + loc[1:]
    return loc

def determine_case_family(answers: dict) -> str:
    property_segment = answers.get("property_segment") or "unknown"
    object_type = answers.get("object_type") or "unknown"
    issue_type = answers.get("issue_type") or "unknown"
    goal = answers.get("goal") or "unknown"
    
    if issue_type == "shared_ownership_problem":
        return "shared_ownership_issue"
    if issue_type == "boundary_problem" or issue_type == "land_problem":
        return "land_boundary_issue"
    
    if object_type in ("warehouse", "production", "shop", "office", "building", "part_of_building"):
        if issue_type in ("bank_problem", "transaction_block") or goal == "bank_pledge":
            return "commercial_bank_block"
        if issue_type == "document_mismatch" or "расхожд" in str(answers.get("raw_user_problem", "")).lower():
            return "commercial_document_mismatch"
    
    if object_type == "house" and issue_type == "unauthorized_construction" and goal == "registration":
        return "residential_house_registration"
    if object_type in ("warehouse", "production", "shop", "office", "building"):
        if issue_type in ("redevelopment", "reconstruction"):
            return "commercial_redevelopment"
        if issue_type in ("unauthorized_construction", "registration_absent") or goal in ("registration", "legalization"):
            return "commercial_new_building"
    if property_segment == "residential":
        if goal == "gas_connection" or issue_type == "gas_connection_block":
            return "gas_connection_house"
        if object_type == "dacha":
            return "residential_dacha_registration"
        if object_type == "house" and issue_type in ("registration_absent", "ownership_problem", None):
            return "residential_house_registration"
        if issue_type == "inheritance_real_estate":
            return "inheritance_real_estate"
    if issue_type == "lease_problem":
        return "lease_problem"
    if looks_like_real_estate(answers):
        return "other_real_estate_case"
    return "unknown"

def build_consultation_plan(case_family: str) -> dict:
    plans = {
        "commercial_redevelopment": {
            "case_family": case_family,
            "consultation_mode": "two_paths",
            "primary_path": "Административное узаконивание",
            "secondary_path": "Судебное узаконивание",
            "primary_path_short": "Если изменения можно легализовать через техническую подготовку, документы и согласование, вопрос решается без суда.",
            "secondary_path_short": "Если объект уже используется в изменённом виде и обычный путь не проходит, применяется судебный порядок через экспертизу.",
            "need_to_check": [
                "право на объект и землю",
                "что именно изменено по сравнению с документами",
                "есть ли техдокументация",
                "были ли отказы банка, Росреестра или администрации",
            ],
            "request_contact_after_reply": True,
        },
        "commercial_new_building": {
            "case_family": case_family,
            "consultation_mode": "two_paths",
            "primary_path": "Оформление в обычном порядке",
            "secondary_path": "Судебное оформление права",
            "primary_path_short": "Если объект и земля позволяют пройти стандартную процедуру, сначала проверяется возможность оформить его без суда.",
            "secondary_path_short": "Если объект уже построен и обычный путь не работает, право оформляется через судебный порядок и экспертизу.",
            "need_to_check": [
                "статус земли",
                "разрешительная и исходная документация",
                "можно ли пройти обычную регистрацию",
                "фактические параметры объекта",
            ],
            "request_contact_after_reply": True,
        },
        "commercial_document_mismatch": {
            "case_family": case_family,
            "consultation_mode": "two_paths",
            "primary_path": "Приведение документов в соответствие",
            "secondary_path": "Судебное узаконивание фактического состояния",
            "primary_path_short": "Если расхождения можно устранить через техническую и юридическую подготовку, начинают с этого пути.",
            "secondary_path_short": "Если объект давно существует в изменённом виде и обычное оформление не проходит, используют судебный порядок.",
            "need_to_check": [
                "какие именно расхождения есть",
                "что отражено в ЕГРН и техдокументах",
                "есть ли препятствия для сделки или залога",
                "статус земли",
            ],
            "request_contact_after_reply": True,
        },
        "commercial_bank_block": {
            "case_family": case_family,
            "consultation_mode": "two_paths",
            "primary_path": "Подготовка объекта под требования банка",
            "secondary_path": "Судебный сценарий узаконивания",
            "primary_path_short": "Если проблему можно убрать через документы и техподготовку, сначала приводят объект в порядок под банковскую проверку.",
            "secondary_path_short": "Если расхождения существенные и документально быстро не исправляются, вопрос решают через суд и экспертизу.",
            "need_to_check": [
                "причина отказа банка",
                "что именно не устроило в объекте",
                "есть ли техплан, БТИ, выписка ЕГРН",
                "совпадает ли объект с документами",
            ],
            "request_contact_after_reply": True,
        },
        "residential_house_registration": {
            "case_family": case_family,
            "consultation_mode": "one_path_plus_check",
            "primary_path": "Регистрация дома в обычном порядке",
            "secondary_path": "Судебное оформление права (запасной сценарий)",
            "primary_path_short": "Техплан → подача документов в Росреестр → регистрация права собственности.",
            "secondary_path_short": "Если обычная регистрация не проходит из-за земли или параметров дома, может потребоваться судебный порядок.",
            "need_to_check": [
                "статус участка",
                "вид разрешённого использования земли",
                "есть ли документы на участок",
                "можно ли пройти регистрацию без суда",
            ],
            "request_contact_after_reply": True,
        },
        "residential_dacha_registration": {
            "case_family": case_family,
            "consultation_mode": "one_path_plus_check",
            "primary_path": "Упрощённая регистрация дома",
            "secondary_path": "Проверка препятствий и альтернативный сценарий",
            "primary_path_short": "Техплан → подача в Росреестр по дачной амнистии или в общем порядке.",
            "secondary_path_short": "Если по участку или дому есть ограничения, сначала нужно разобраться, что мешает.",
            "need_to_check": [
                "статус участка",
                "СНТ, земли населённых пунктов или иное",
                "есть ли документы на землю",
                "подходит ли дом под упрощённую регистрацию",
            ],
            "request_contact_after_reply": True,
        },
        "gas_connection_house": {
            "case_family": case_family,
            "consultation_mode": "one_path_plus_check",
            "primary_path": "Оформление дома для подключения газа",
            "secondary_path": "Проверка, что мешает обычной регистрации",
            "primary_path_short": "Технический план → регистрация права → подключение газа.",
            "secondary_path_short": "Если обычный путь не проходит, нужно понять, проблема в земле, характеристиках дома или документах.",
            "need_to_check": [
                "статус участка",
                "есть ли документы на дом и землю",
                "что именно требует газовая служба",
                "можно ли зарегистрировать дом без суда",
            ],
            "request_contact_after_reply": True,
        },
        "land_boundary_issue": {
            "case_family": case_family,
            "consultation_mode": "document_first",
            "primary_path": "Кадастровая и документальная проверка",
            "secondary_path": "Судебное урегулирование спора",
            "primary_path_short": "Поднимают документы, межевой план, сведения ЕГРН и фактические границы.",
            "secondary_path_short": "Если конфликт по границам не решается документально, вопрос уходит в судебный порядок.",
            "need_to_check": [
                "выписка ЕГРН",
                "межевой план",
                "кто смежный собственник",
                "в чём именно конфликт",
            ],
            "request_contact_after_reply": True,
        },
        "inheritance_real_estate": {
            "case_family": case_family,
            "consultation_mode": "document_first",
            "primary_path": "Оформление права без суда",
            "secondary_path": "Судебное подтверждение прав",
            "primary_path_short": "Если комплект наследственных документов достаточный, сначала проверяют возможность оформления права в обычном порядке.",
            "secondary_path_short": "Если есть пропуск сроков, спор или проблема с документами, может понадобиться судебный порядок.",
            "need_to_check": [
                "кто наследник",
                "есть ли открытое наследственное дело",
                "какие документы на объект",
                "в чём препятствие к оформлению",
            ],
            "request_contact_after_reply": True,
        },
        "shared_ownership_issue": {
            "case_family": case_family,
            "consultation_mode": "document_first",
            "primary_path": "Документальная проверка прав и режима собственности",
            "secondary_path": "Судебный порядок урегулирования спора",
            "primary_path_short": "Сначала нужно понять, как оформлены доли и что именно мешает решить вопрос без суда.",
            "secondary_path_short": "Если между собственниками конфликт и документально вопрос не решается, нужен судебный сценарий.",
            "need_to_check": [
                "кто собственники",
                "как оформлены доли",
                "какой объект",
                "в чём именно спор",
            ],
            "request_contact_after_reply": True,
        },
        "lease_problem": {
            "case_family": case_family,
            "consultation_mode": "document_first",
            "primary_path": "Проверка договора и статуса прав на объект",
            "secondary_path": "Судебная защита права",
            "primary_path_short": "Сначала анализируют, как оформлены права на объект и землю, и какую роль играет договор аренды.",
            "secondary_path_short": "Если вопрос нельзя решить по документам и переговорам, дальше смотрят судебный вариант.",
            "need_to_check": [
                "договор аренды",
                "кто собственник объекта и земли",
                "в чём юридическое препятствие",
                "какая конечная цель клиента",
            ],
            "request_contact_after_reply": True,
        },
        "other_real_estate_case": {
            "case_family": case_family,
            "consultation_mode": "clarify_first",
            "primary_path": "Предварительный правовой разбор",
            "secondary_path": None,
            "primary_path_short": "По таким вопросам сначала нужно уточнить базовые данные по объекту и цели, чтобы понять рабочий сценарий.",
            "secondary_path_short": None,
            "need_to_check": [
                "что за объект",
                "где находится",
                "в чём проблема",
                "какая цель клиента",
            ],
            "request_contact_after_reply": True,
        },
        "unknown": {
            "case_family": "unknown",
            "consultation_mode": "reject",
            "primary_path": None,
            "secondary_path": None,
            "primary_path_short": None,
            "secondary_path_short": None,
            "need_to_check": [],
            "request_contact_after_reply": False,
        },
    }
    return deepcopy(plans.get(case_family, plans["unknown"]))

def object_label(answers: dict) -> str:
    object_map = {
        "warehouse": "складском объекте",
        "production": "производственном объекте",
        "shop": "торговом объекте",
        "office": "офисном объекте",
        "building": "здании",
        "part_of_building": "части здания",
        "land_plot": "земельном участке",
        "house": "жилом доме",
        "apartment": "квартире",
        "dacha": "даче",
        "garage": "гараже",
        "other_real_estate": "объекте недвижимости",
        "unknown": "объекте недвижимости",
    }
    return object_map.get(answers.get("object_type") or "unknown", "объекте недвижимости")

def issue_label(answers: dict) -> str:
    issue_map = {
        "unauthorized_construction": "самовольной постройкой",
        "redevelopment": "перепланировкой",
        "reconstruction": "реконструкцией",
        "registration_absent": "отсутствием регистрации",
        "registration_refusal": "отказом в регистрации",
        "document_mismatch": "расхождением между фактом и документами",
        "ownership_problem": "правами на объект",
        "land_problem": "правами на землю",
        "bank_problem": "проблемой с банком",
        "transaction_block": "блокировкой сделки",
        "inheritance_real_estate": "наследственным вопросом по недвижимости",
        "shared_ownership_problem": "вопросом по долевой собственности",
        "boundary_problem": "границами участка",
        "lease_problem": "арендными отношениями",
        "gas_connection_block": "оформлением для подключения газа",
        "other_real_estate_issue": "юридическим вопросом по недвижимости",
        "unknown": "юридическим вопросом по недвижимости",
    }
    return issue_map.get(answers.get("issue_type") or "unknown", "юридическим вопросом по недвижимости")

def build_question_response(message_for_user: str, options: list = None, session=None):
    if session:
        session["last_question"] = message_for_user
    response = {
        "status": "question",
        "message_for_user": message_for_user,
    }
    if options:
        response["options"] = options
    return response

def build_nontarget_response(answers: dict):
    return {
        "status": "final",
        "lead_category": "nontarget",
        "client_summary": "Запрос не относится к юридическим вопросам, связанным с недвижимостью.",
        "preliminary_assessment": "Я помогаю с юридическими вопросами по недвижимости: регистрация, узаконивание, кредит под залог, сделки. Если ваш вопрос об этом – уточните, пожалуйста, подробнее.",
        "what_to_check": [],
        "recommended_next_step": "not_in_scope",
        "final_message_for_user": "Я помогаю с юридическими вопросами по недвижимости: регистрация, узаконивание, кредит под залог, сделки. Если ваш вопрос об этом – уточните, пожалуйста, подробнее.",
        "lead_card": {
            "raw_user_problem": answers.get("raw_user_problem") or "",
            "object_type": answers.get("object_type") or "unknown",
            "property_segment": answers.get("property_segment") or "unknown",
            "issue_type": answers.get("issue_type") or "unknown",
            "location_description": answers.get("location_description") or "",
            "contact": answers.get("contact") or "",
            "notes": "Запрос вне профиля.",
        },
        "form_payload": None,
    }

def user_signaled_no_more_info(user_message: str) -> bool:
    text = user_message.lower().strip()
    phrases = [
        "это всё",
        "это всё что я знаю",
        "больше ничего не знаю",
        "всё что могу сказать",
        "больше информации нет",
        "ничего больше нет",
        "все данные",
        "всё, что есть",
    ]
    return any(phrase in text for phrase in phrases)

def data_is_enough_for_preliminary_conclusion(answers: dict) -> bool:
    obj = answers.get("object_type")
    issue = answers.get("issue_type")
    if obj is None or obj == "unknown" or issue is None or issue == "unknown":
        return False
    location = answers.get("location_description")
    if location is None or location == "" or location == "не указана":
        return False
    
    is_commercial = (answers.get("property_segment") == "commercial" or
                     obj in ("warehouse", "production", "shop", "office", "building"))
    if not is_commercial:
        return True
    
    case_family = determine_case_family(answers)
    if case_family in ("commercial_redevelopment", "commercial_document_mismatch"):
        has_land_or_cadastral = (answers.get("land_rights_status") not in (None, "unknown") or
                                  answers.get("cadastral_number") not in (None, ""))
        if not has_land_or_cadastral:
            return False
        has_docs = (answers.get("documents") and
                    len(answers.get("documents", [])) > 0 and
                    "none" not in answers.get("documents", []))
        has_refusals = answers.get("has_refusals_or_disputes") not in (None, "unknown")
        return has_docs or has_refusals
    else:
        return (answers.get("land_rights_status") not in (None, "unknown") or
                answers.get("cadastral_number") not in (None, ""))

def data_is_enough_for_full_analysis(answers: dict) -> bool:
    return (answers.get("contact") is not None and
            answers.get("object_type") not in (None, "unknown") and
            answers.get("issue_type") not in (None, "unknown"))

def build_preliminary_conclusion_response(answers: dict, plan: dict) -> dict:
    obj = object_label(answers)
    issue = issue_label(answers)
    location = normalize_location(answers.get("location_description") or "")
    if location == "не указана":
        location_str = "неизвестной локации"
    else:
        location_str = f"в {location}"
    problem_desc = f"{obj} без правоустанавливающих документов" if answers.get("issue_type") in ("unauthorized_construction", "registration_absent") else f"{issue} на {obj}"
    primary = plan.get("primary_path_short") or "требуется индивидуальный разбор"
    secondary = plan.get("secondary_path_short")
    
    case_family = plan.get("case_family") or determine_case_family(answers)
    
    if case_family == "shared_ownership_issue":
        risk = "Конфликт между собственниками и невозможность решить вопрос без суда"
        factor = "Как оформлены доли и есть ли согласие/конфликт между собственниками"
    elif case_family == "land_boundary_issue":
        risk = "Закрепление неверной границы и спор с соседом"
        factor = "Есть ли межевание и совпадают ли данные ЕГРН с фактической границей"
    elif case_family == "commercial_bank_block":
        risk = "Банк не примет объект в залог до устранения расхождений"
        factor = "Что именно не совпадает в документах и можно ли исправить это без суда"
    elif case_family in ("residential_house_registration", "residential_dacha_registration"):
        risk = "Отказ в регистрации при неподходящих параметрах участка или дома"
        factor = "Статус участка, ВРИ, наличие документов на землю и техплана"
    elif case_family == "gas_connection_house":
        risk = "Газ не подключат без регистрации дома"
        factor = "Можно ли зарегистрировать дом в обычном порядке без суда"
    else:
        if answers.get("issue_type") == "unauthorized_construction":
            risk = "Риск сноса или отказа во внесудебной регистрации"
        elif answers.get("land_rights_status") == "not_registered":
            risk = "Отсутствие прав на землю делает объект самовольной постройкой"
        elif answers.get("bank_or_transaction_context") == "bank_refusal":
            risk = "Банк не примет объект в залог без приведения документов в порядок"
        else:
            risk = "Риск длительных судебных разбирательств и дополнительных расходов"
        
        if answers.get("land_rights_status") == "owned":
            factor = "Наличие права собственности на землю упрощает судебный путь"
        elif answers.get("cadastral_number"):
            factor = "Наличие кадастрового номера позволяет быстрее подготовить техплан"
        elif answers.get("has_refusals_or_disputes") == "yes":
            factor = "Уже были отказы – нужно переходить к судебному порядку"
        else:
            factor = "Точный сценарий зависит от статуса земли и наличия разрешительной документации"
    
    message = (
        f"**Что я понял:**\n"
        f"Речь идёт о {obj} {location_str}. Проблема — {issue}.\n\n"
        f"**Ключевая проблема:** {problem_desc}\n\n"
        f"**Возможны 2 сценария:**\n"
        f"1️⃣ {primary}\n"
    )
    if secondary:
        message += f"2️⃣ {secondary}\n\n"
    else:
        message += "\n"
    message += (
        f"**Ключевой риск:** {risk}\n\n"
        f"**От чего зависит решение:** {factor}\n\n"
        "Дальше всё упирается в документы.\n"
        "Могу быстро разобрать ваш объект и сказать:\n"
        "— можно ли узаконить без суда\n"
        "— или сразу нужно идти в суд\n\n"
        "Оставьте телефон или Telegram — разберу ситуацию и дам конкретный план действий."
    )
    return {
        "status": "question",
        "message_for_user": message,
        "preliminary": True,
    }

def plan_next_step(answers: dict, history: list, force_finalize: bool = False) -> dict:
    history_text = ""
    for entry in history[-5:]:
        user = entry.get("user", "")
        history_text += f"Клиент: {user}\n"
    
    force_instruction = ""
    if force_finalize:
        force_instruction = "Пользователь сказал, что это всё, что он знает. Если не хватает одного критичного факта — задай его. Если данных достаточно — дай preliminary.\n"
    
    prompt = f"""Ты — юридический эксперт по недвижимости. На основе текущих фактов и истории диалога реши, достаточно ли данных для предварительного разбора.

Текущие факты (answers):
{json.dumps(answers, ensure_ascii=False, indent=2)}

История диалога (последние сообщения):
{history_text}

{force_instruction}

Инструкция:
- Если данных достаточно, чтобы понять суть проблемы, объект, локацию и ключевые обстоятельства — верни ready=true и message — это краткий предварительный разбор.
- Если данных не хватает — верни ready=false и message — это один конкретный уточняющий вопрос (максимум 15 слов).
- В предварительном разборе (ready=true) должна быть структура:
  * Что я понял (одна короткая фраза)
  * В чём проблема (одно предложение)
  * Сценарии (1-2 варианта, без лишних слов)
  * Главный риск (одна фраза)
  * От чего зависит решение (одна фраза)
- Ни в коем случае не используй слова: "мягкий CTA", "CTA", "оставьте контакт для связи", "ниже варианты", "структура ответа", "блок", "продающий".
- Вместо прямого призыва оставить контакт, заверши разбор естественным переходом к следующему шагу, например: "Если хотите, могу дальше посмотреть документы и сказать точный путь" или "Для уточнения плана действий можно передать контакт".
- Формулируй ответ естественно, без служебных маркеров. Не пиши "**Что я понял**" и подобные заголовки — просто текст.
- Ответ должен быть только JSON вида: {{"ready": true/false, "message": "текст"}}

Отвечай JSON."""
    
    messages = [
        {"role": "system", "content": "Ты — аналитик. Отвечаешь только JSON. Никаких пояснений, только JSON."},
        {"role": "user", "content": prompt}
    ]
    try:
        raw = call_deepseek(messages, timeout=30, max_tokens=800)
        parsed = safe_parse_json(raw)
        ready = parsed.get("ready", False)
        message = parsed.get("message", "")
        if not isinstance(ready, bool):
            ready = False
        if not isinstance(message, str):
            message = ""
        forbidden = ["мягкий cta", "cta", "оставьте контакт для связи", "ниже варианты", "структура ответа", "блок", "продающий"]
        msg_lower = message.lower()
        for phrase in forbidden:
            if phrase in msg_lower:
                message = message.replace(phrase, "").strip()
                if not message:
                    message = "Для дальнейшего анализа можно оставить контакт."
                break
        return {"ready": ready, "message": message}
    except Exception as e:
        log_event("plan_next_step_error", {"error": str(e)})
        return {"ready": False, "message": ""}

def build_next_document_request(answers: dict, plan: dict) -> str:
    case_family = plan.get("case_family", "unknown")
    
    if case_family in ("commercial_bank_block", "commercial_document_mismatch"):
        return "Чтобы перейти к точному разбору, пришлите техплан, БТИ, выписку ЕГРН или текст отказа банка, если он у вас есть."
    elif case_family in ("commercial_redevelopment", "commercial_new_building"):
        return "Чтобы определить точный путь, пришлите кадастровый номер, документы на землю, техплан или старый план БТИ, если они есть."
    elif case_family in ("residential_house_registration", "residential_dacha_registration", "gas_connection_house"):
        return "Чтобы перейти к точному разбору, пришлите кадастровый номер участка, документы на землю, год постройки дома или техплан, если он уже есть."
    elif case_family == "land_boundary_issue":
        return "Чтобы точно понять ситуацию по границе, пришлите выписку ЕГРН, межевой план или кадастровый номер участка, если они есть."
    elif case_family == "shared_ownership_issue":
        return "Чтобы перейти к точному разбору, пришлите выписку ЕГРН, сведения о долях и, если есть, данные по долгу или лицевому счёту."
    elif case_family == "inheritance_real_estate":
        return "Чтобы перейти к точному разбору, пришлите документы на объект и данные по наследственному делу, если они уже есть."
    else:
        return "Для точного разбора пришлите, пожалуйста, кадастровый номер, выписку ЕГРН, техплан или другой ключевой документ по объекту."

def generate_legal_analysis_with_cta(answers: dict, plan: dict) -> str:
    obj = object_label(answers)
    issue = issue_label(answers)
    goal = answers.get("goal") or "не указана"
    location = normalize_location(answers.get("location_description") or answers.get("region") or "не указана")
    cadastral = answers.get("cadastral_number") or "не указан"
    land_rights = answers.get("land_rights_status") or "не указано"
    has_refusals = answers.get("has_refusals_or_disputes") or "не указано"
    bank_context = answers.get("bank_or_transaction_context") or "не указано"
    raw_problem = answers.get("raw_user_problem") or ""

    case_family = plan.get("case_family", "unknown")

    # Определяем case-specific риски
    risk_map = {
        "commercial_bank_block": (
            "• Банк не примет объект в залог\n"
            "• Сделка может не состояться\n"
            "• Потребуется срочная переделка документов"
        ),
        "commercial_document_mismatch": (
            "• Росреестр может отказать в регистрации\n"
            "• Проблемы при продаже объекта\n"
            "• Несоответствие может всплыть при любой проверке"
        ),
        "commercial_redevelopment": (
            "• Риск признания реконструкции самовольной\n"
            "• Штрафы и предписания от администрации\n"
            "• Сложности с узакониванием изменений"
        ),
        "residential_house_registration": (
            "• Отказ в регистрации права собственности\n"
            "• Невозможность подключить газ и коммуникации\n"
            "• Ограничения в распоряжении домом"
        ),
        "residential_dacha_registration": (
            "• Отказ в регистрации из-за несоответствия параметров\n"
            "• Проблемы при продаже или дарении\n"
            "• Невозможность оформить дом официально"
        ),
        "gas_connection_house": (
            "• Газ не подключат без зарегистрированного права\n"
            "• Дополнительные согласования и расходы\n"
            "• Затягивание сроков подключения"
        ),
        "land_boundary_issue": (
            "• Спор с соседом может перерасти в судебный\n"
            "• Наложение границ по документам\n"
            "• Риск закрепления неверной границы"
        ),
        "shared_ownership_issue": (
            "• Конфликт между собственниками\n"
            "• Блокировка любой сделки с недвижимостью\n"
            "• Судебные споры о порядке пользования"
        ),
        "inheritance_real_estate": (
            "• Пропуск срока принятия наследства\n"
            "• Споры между наследниками\n"
            "• Отказ нотариуса в выдаче свидетельства"
        ),
    }
    risk_block = risk_map.get(case_family, (
        "• Юридические риски, связанные с конкретной ситуацией\n"
        "• Возможные отказы госорганов\n"
        "• Дополнительные судебные издержки"
    ))

    # Определяем case-specific действия
    action_map = {
        "commercial_bank_block": (
            "Анализируем причину отказа банка\n"
            "Приводим техническую документацию в порядок\n"
            "При необходимости идём в суд для узаконивания"
        ),
        "commercial_document_mismatch": (
            "Корректируем техплан и подаём в Росреестр\n"
            "Обновляем данные в ЕГРН\n"
            "Либо готовим иск о признании права"
        ),
        "commercial_redevelopment": (
            "Готовим техплан и проектную документацию\n"
            "Проверяем допустимость изменений\n"
            "Согласовываем или идём в суд"
        ),
        "residential_house_registration": (
            "Заказываем техплан у кадастрового инженера\n"
            "Подаём документы в Росреестр\n"
            "При отказе — готовим иск в суд"
        ),
        "residential_dacha_registration": (
            "Готовим техплан\n"
            "Подаём уведомление или заявление по дачной амнистии\n"
            "Регистрируем право в Росреестре"
        ),
        "gas_connection_house": (
            "Оформляем право собственности на дом\n"
            "Готовим техплан и подаём на регистрацию\n"
            "После регистрации подключаем газ"
        ),
        "land_boundary_issue": (
            "Анализируем выписку ЕГРН и межевой план\n"
            "Пытаемся решить с соседом мирно\n"
            "При отказе — судебное установление границ"
        ),
        "shared_ownership_issue": (
            "Анализируем доли и документы\n"
            "Вырабатываем досудебную стратегию\n"
            "При конфликте — иск в суд"
        ),
        "inheritance_real_estate": (
            "Проверяем наследственное дело\n"
            "Восстанавливаем сроки, если пропущены\n"
            "При споре — судебный порядок"
        ),
    }
    action_block = action_map.get(case_family, (
        "Анализируем документы\n"
        "Выбираем оптимальный путь\n"
        "Готовим необходимые заявления или иск"
    ))

    prompt = f"""Ты — практикующий юрист по недвижимости в России. Сделай чёткий, конкретный разбор. Без цитат сообщения клиента, без повторного запроса контакта.

Данные:
- Объект: {obj}
- Проблема: {issue}
- Цель: {goal}
- Локация: {location}
- Кадастровый номер: {cadastral}
- Статус земли: {land_rights}
- Отказы/споры: {has_refusals}
- Банк/сделка: {bank_context}

Специфика кейса: {case_family}

Риски (конкретно для этого типа ситуации):
{risk_block}

Что можно сделать (конкретные шаги для этого кейса):
{action_block}

Требования:
1. Пиши коротко, по делу, как практик.
2. Не используй лишнее ** (только заголовки).
3. Избегай фраз «оставьте контакт» — контакт уже есть.
4. В блоке «Что можно сделать» используй предложенный action_block (можно немного переформулировать).
5. Добавь блок «Как я могу помочь»:
   - Проверить документы и дать точный сценарий.
   - Организовать технический план через кадастрового инженера.
   - При необходимости вести суд до регистрации.
   - Поэтапная оплата без 100% предоплаты.

Структура ответа:

**Текущая ситуация**
(2–3 предложения, без воды)

**Риски**
(используй risk_block)

**Что можно сделать**
(используй action_block)

**Как я могу помочь**
(короткий продающий блок)

**Вывод**
(без повтора контакта, просто итог)

Стиль: уверенный, экспертный, без заигрываний."""
    
    messages = [
        {"role": "system", "content": "Ты — практикующий юрист по недвижимости. Отвечаешь чётко, без цитат и повторного запроса контакта."},
        {"role": "user", "content": prompt}
    ]
    try:
        analysis = call_deepseek(messages, timeout=90, max_tokens=1800)
        if len(analysis.strip()) < 500:
            log_event("analysis_error", {"error": "Response too short", "length": len(analysis), "stage": "full_analysis"})
            return build_fallback_final_response(answers, plan)
        if len(analysis) > 3500:
            cut_point = analysis.rfind('.', 0, 3500)
            if cut_point == -1:
                cut_point = 3500
            analysis = analysis[:cut_point+1] + "\n\n(текст сокращён для краткости)"
        return analysis
    except (TimeoutError, RuntimeError, Exception) as e:
        log_event("analysis_error", {"error": str(e), "stage": "full_analysis"})
        return build_fallback_final_response(answers, plan)

def build_fallback_final_response(answers: dict, plan: dict) -> str:
    obj = object_label(answers)
    issue = issue_label(answers)
    location = normalize_location(answers.get("location_description") or answers.get("region") or "не указана")
    if location == "не указана":
        location_str = "неизвестной локации"
    else:
        location_str = f"в {location}"

    situation = f"Объект: {obj} {location_str}. Проблема: {issue}."

    risks = []
    issue_type = answers.get("issue_type")
    if issue_type == "unauthorized_construction":
        risks.append("• Риск сноса или отказа во внесудебной регистрации")
        risks.append("• Невозможность продажи или залога до узаконивания")
        risks.append("• Штрафы за самовольное строительство")
    elif issue_type == "document_mismatch":
        risks.append("• Блокировка сделок из-за расхождений в документах")
        risks.append("• Проблемы с налоговой и кадастровой стоимостью")
    elif issue_type == "registration_absent":
        risks.append("• Отсутствие права собственности юридически")
        risks.append("• Нельзя подключить газ или электричество")
    else:
        risks.append("• Риск длительных судебных разбирательств")
        risks.append("• Дополнительные расходы")
    risks_text = "\n".join(risks)

    primary = plan.get("primary_path_short") or "требуется индивидуальный разбор"
    secondary = plan.get("secondary_path_short")
    scenarios = f"**Что можно сделать**\n- Административный путь: {primary}\n"
    if secondary:
        scenarios += f"- Судебный путь: {secondary}\n"
    else:
        scenarios += "- Судебный путь: экспертиза → иск → регистрация\n"

    help_block = """**Как я могу помочь**
- Проверю ваши документы и скажу точный сценарий (суд или без суда)
- Организую технический план через своего кадастрового инженера
- При необходимости проведу суд до регистрации права
- Поэтапная оплата — без 100% предоплаты"""

    conclusion = "Дальше всё упирается в документы. Если хотите, разберу ваш объект и скажу точный путь с шансами и сроками."

    final_message = f"""**Текущая ситуация**
{situation}

**Риски**
{risks_text}

{scenarios}
{help_block}

**Вывод**
{conclusion}"""
    return final_message

def build_final_response(answers: dict, lead_category: str, plan: dict):
    if data_is_enough_for_full_analysis(answers):
        final_message = generate_legal_analysis_with_cta(answers, plan)
    else:
        final_message = build_fallback_final_response(answers, plan)
    raw_problem = answers.get("raw_user_problem") or ""
    court_needed = "да" if answers.get("issue_type") in ("unauthorized_construction", "document_mismatch") else "нет"
    risk_level = "высокий" if any(word in raw_problem.lower() for word in ["снос", "нарушен", "самовольн"]) else "средний"
    return {
        "status": "final",
        "lead_category": lead_category,
        "client_summary": f"Речь идёт о {object_label(answers)} в {normalize_location(answers.get('location_description') or answers.get('region') or 'неуточнённом месте')}, где вопрос связан с {issue_label(answers)}.",
        "preliminary_assessment": "Ситуация относится к юридическим вопросам по недвижимости.",
        "possible_paths": [plan.get("primary_path_short"), plan.get("secondary_path_short")],
        "what_to_check": plan.get("need_to_check", [])[:4],
        "recommended_next_step": "callback",
        "final_message_for_user": final_message,
        "lead_card": {
            "raw_user_problem": answers.get("raw_user_problem") or "",
            "normalized_problem": answers.get("normalized_problem") or "",
            "object_type": answers.get("object_type") or "unknown",
            "property_segment": answers.get("property_segment") or "unknown",
            "issue_type": answers.get("issue_type") or "unknown",
            "goal": answers.get("goal") or "unknown",
            "location_description": answers.get("location_description") or "",
            "cadastral_number": answers.get("cadastral_number") or "",
            "cadastral_status": answers.get("cadastral_status") or "unknown",
            "documents": answers.get("documents") or [],
            "has_refusals_or_disputes": answers.get("has_refusals_or_disputes") or "unknown",
            "contact": answers.get("contact") or "",
            "case_family": plan.get("case_family") or "unknown",
            "consultation_mode": plan.get("consultation_mode") or "unknown",
            "court_needed": court_needed,
            "risk_level": risk_level,
            "notes": "",
        },
        "form_payload": build_form_payload(answers, plan),
    }

def build_form_payload(answers: dict, plan: dict):
    raw_contact = answers.get("contact") or ""
    digits = re.sub(r"\D", "", raw_contact)
    if digits.startswith("7") and len(digits) == 11:
        contact = "8" + digits[1:]
    elif digits.startswith("8") and len(digits) == 11:
        contact = digits
    else:
        contact = raw_contact
    obj = object_label(answers)
    raw_location = answers.get("location_description") or answers.get("region") or "не указана"
    location = normalize_location(raw_location)
    issue = issue_label(answers)
    goal = answers.get("goal") or "не указана"
    land_rights = answers.get("land_rights_status") or "не указано"
    has_refusals = answers.get("has_refusals_or_disputes") or "не указано"
    bank_context = answers.get("bank_or_transaction_context") or "не указано"
    cadastral = answers.get("cadastral_number") or "не указан"
    court_needed = "да" if answers.get("issue_type") in ("unauthorized_construction", "document_mismatch") else "нет"
    raw_problem = answers.get("raw_user_problem") or ""
    risk_level = "высокий" if any(word in raw_problem.lower() for word in ["снос", "нарушен", "самовольн"]) else "средний"
    summary = (
        f"Клиент сообщил о проблеме с {obj}.\n"
        f"Локация: {location}\n"
        f"Суть вопроса: {issue}\n"
        f"Цель: {goal}\n"
        f"Кадастровый номер: {cadastral}\n"
        f"Земля: {land_rights}\n"
        f"Отказы/споры: {has_refusals}\n"
        f"Банк/залог/сделка: {bank_context}\n"
        f"Суд необходим: {court_needed}\n"
        f"Уровень риска: {risk_level}\n"
        f"Дополнительно: {raw_problem}"
    )
    message = (
        f"Заявка из бота.\n"
        f"Проблема: {raw_problem}\n"
        f"Нормализовано: {answers.get('normalized_problem') or ''}\n"
        f"Локация: {location}\n"
        f"Кадастровый номер: {cadastral}\n"
        f"Объект: {answers.get('object_type') or 'unknown'}\n"
        f"Сегмент: {answers.get('property_segment') or 'unknown'}\n"
        f"Тип вопроса: {answers.get('issue_type') or 'unknown'}\n"
        f"Цель: {goal}\n"
        f"Земля: {land_rights}\n"
        f"Отказы/споры: {has_refusals}\n"
        f"Банк/сделка: {bank_context}\n"
        f"Сценарий: {plan.get('case_family') or 'unknown'}\n"
        f"Суд необходим: {court_needed}\n"
        f"Уровень риска: {risk_level}\n"
        f"Контакт: {contact}\n"
        f"---\n"
        f"Человеко-читаемая сводка:\n{summary}"
    )
    return {
        "phone": contact,
        "message": message,
        "subject": "Заявка из бота",
        "lead_summary": summary,
        "case_family": plan.get("case_family") or "unknown",
        "consultation_mode": plan.get("consultation_mode") or "unknown",
    }

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/chat")
async def chat(body: ChatRequest):
    user_message = body.message.strip()
    session_id = (body.session_id or "").strip()
    if not user_message:
        return JSONResponse(status_code=400, content={"error": "Empty message"})
    if not session_id:
        session_id = str(uuid.uuid4())
    if session_id not in sessions:
        sessions[session_id] = new_session()
    session = sessions[session_id]
    log_event("user_message", {
        "session_id": session_id,
        "message": user_message,
        "state": session.get("state")
    })
    if session.get("finished"):
        return {
            "session_id": session_id,
            "reply": {
                "status": "final",
                "final_message_for_user": "Контакт уже получен. Следующий шаг — разбор документов по объекту.",
                "form_payload": None,
            }
        }
    answers = session["answers"]
    try:
        extracted = extract_fields(user_message, answers)
    except Exception as e:
        log_event("extract_error", {"error": str(e)})
        extracted = {}
    answers = merge_answers(answers, extracted)
    
    last_question = (session.get("last_question") or "").lower()
    if any(word in last_question for word in ["земель", "участок", "документы", "владеете", "правоустанавливающие"]):
        user_lower = user_message.lower()
        if any(word in user_lower for word in ["да", "+", "есть", "конечно", "имеется", "в собственности", "принадлежит", "собственность"]):
            answers["land_rights_status"] = "owned"
            log_event("heuristic_land", {"value": "owned", "trigger": user_message})
        elif "нет" in user_lower:
            answers["land_rights_status"] = "not_registered"
            log_event("heuristic_land", {"value": "not_registered", "trigger": user_message})
    
    if "построен" in last_question or "когда" in last_question or "построй" in last_question:
        time_words = ["пол года", "год", "месяц", "неделя", "дней", "202", "2023", "2024", "2025", "построен", "возведён", "сдан", "построили", "возвели"]
        if any(word in user_message.lower() for word in time_words):
            answers["construction_date"] = "известно"
            log_event("heuristic_construction_date", {"value": "known", "trigger": user_message})
    
    session["answers"] = answers
    session["history"].append({"user": user_message, "extracted": extracted})
    lead_category = classify_lead(answers)
    session["lead_category"] = lead_category
    if lead_category == "nontarget":
        if session["clarification_attempts"] < MAX_CLARIFICATION_ATTEMPTS:
            session["clarification_attempts"] += 1
            session["nontarget_issued"] = True
            return {
                "session_id": session_id,
                "reply": build_question_response(
                    "Я помогаю с юридическими вопросами по недвижимости: регистрация, узаконивание, кредит под залог, сделки. Если ваш вопрос об этом – уточните, пожалуйста, подробнее.",
                    session=session
                )
            }
        response = build_nontarget_response(answers)
        return {"session_id": session_id, "reply": response}
    
    if data_is_enough_for_full_analysis(answers):
        case_family = determine_case_family(answers)
        plan = build_consultation_plan(case_family)
        session["consultation_plan"] = plan
        response = build_final_response(answers, lead_category, plan)
        if response.get("form_payload"):
            send_email_lead(response["form_payload"])
        log_lead({
            "session_id": session_id,
            "lead_category": response["lead_category"],
            "case_family": response["lead_card"]["case_family"],
            "object_type": response["lead_card"]["object_type"],
            "issue_type": response["lead_card"]["issue_type"],
            "location": response["lead_card"]["location_description"],
            "contact": response["lead_card"]["contact"],
            "summary": response["lead_card"].get("notes", ""),
            "form_sent": True
        })
        session["finished"] = True
        return {"session_id": session_id, "reply": response}
    
    if not session.get("preliminary_sent"):
        plan_result = plan_next_step(answers, session["history"], force_finalize=user_signaled_no_more_info(user_message))
        if plan_result.get("ready") and plan_result.get("message"):
            session["preliminary_sent"] = True
            return {
                "session_id": session_id,
                "reply": {
                    "status": "question",
                    "message_for_user": plan_result["message"],
                    "preliminary": True
                }
            }
        elif not plan_result.get("ready") and plan_result.get("message"):
            return {
                "session_id": session_id,
                "reply": build_question_response(plan_result["message"], session=session)
            }
        else:
            default_question = "Уточните, пожалуйста, где находится объект и в чём суть проблемы?"
            return {
                "session_id": session_id,
                "reply": build_question_response(default_question, session=session)
            }
    else:
        if not answers.get("contact"):
            contact, ctype = detect_contact(user_message)
            if contact:
                answers["contact"] = contact
                answers["contact_type"] = ctype
                session["answers"] = answers
                if data_is_enough_for_full_analysis(answers):
                    case_family = determine_case_family(answers)
                    plan = build_consultation_plan(case_family)
                    session["consultation_plan"] = plan
                    response = build_final_response(answers, lead_category, plan)
                    if response.get("form_payload"):
                        send_email_lead(response["form_payload"])
                    log_lead({
                        "session_id": session_id,
                        "lead_category": response["lead_category"],
                        "case_family": response["lead_card"]["case_family"],
                        "object_type": response["lead_card"]["object_type"],
                        "issue_type": response["lead_card"]["issue_type"],
                        "location": response["lead_card"]["location_description"],
                        "contact": response["lead_card"]["contact"],
                        "summary": response["lead_card"].get("notes", ""),
                        "form_sent": True
                    })
                    session["finished"] = True
                    return {"session_id": session_id, "reply": response}
                else:
                    if session.get("consultation_plan", {}).get("case_family"):
                        plan = session["consultation_plan"]
                    else:
                        case_family = determine_case_family(answers)
                        plan = build_consultation_plan(case_family)
                        session["consultation_plan"] = plan
                    next_request = build_next_document_request(answers, plan)
                    return {
                        "session_id": session_id,
                        "reply": build_question_response(next_request, session=session)
                    }
            else:
                return {
                    "session_id": session_id,
                    "reply": build_question_response(QUESTION_TEXTS["contact"], session=session)
                }
        else:
            if data_is_enough_for_full_analysis(answers):
                case_family = determine_case_family(answers)
                plan = build_consultation_plan(case_family)
                session["consultation_plan"] = plan
                response = build_final_response(answers, lead_category, plan)
                if response.get("form_payload"):
                    send_email_lead(response["form_payload"])
                log_lead({
                    "session_id": session_id,
                    "lead_category": response["lead_category"],
                    "case_family": response["lead_card"]["case_family"],
                    "object_type": response["lead_card"]["object_type"],
                    "issue_type": response["lead_card"]["issue_type"],
                    "location": response["lead_card"]["location_description"],
                    "contact": response["lead_card"]["contact"],
                    "summary": response["lead_card"].get("notes", ""),
                    "form_sent": True
                })
                session["finished"] = True
                return {"session_id": session_id, "reply": response}
            else:
                if session.get("consultation_plan", {}).get("case_family"):
                    plan = session["consultation_plan"]
                else:
                    case_family = determine_case_family(answers)
                    plan = build_consultation_plan(case_family)
                    session["consultation_plan"] = plan
                next_request = build_next_document_request(answers, plan)
                return {
                    "session_id": session_id,
                    "reply": build_question_response(next_request, session=session)
                }
