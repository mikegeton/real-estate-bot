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
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

if not DEEPSEEK_API_KEY:
    raise RuntimeError("DEEPSEEK_API_KEY not found in .env")

# Настройка SMTP (Mail.ru) – данные берутся из .env
SMTP_HOST = "smtp.mail.ru"
SMTP_PORT = 465
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_TO = SMTP_USER  # можно заменить на другой ящик, если нужно

if not SMTP_USER or not SMTP_PASSWORD:
    raise RuntimeError("SMTP_USER or SMTP_PASSWORD not found in .env")

# Настройка логирования
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
    """Отправка заявки на email через SMTP Mail.ru"""
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

STEP_ORDER = ["raw_user_problem", "location_description", "cadastral_number_optional", "land_rights", "contact"]

QUESTION_TEXTS = {
    "raw_user_problem": "Кратко опишите проблему по недвижимости: что за объект, в чём юридическая сложность и какая цель.",
    "location_description": "Где находится объект? Можно в свободной форме: область, район, город, СНТ, деревня или адрес.",
    "cadastral_number_optional": "Если есть кадастровый номер — пришлите. Если нет, так и напишите: «нет».",
    "land_rights": "Есть ли у вас правоустанавливающие документы на земельный участок? (в собственности, аренда, нет документов)",
    "contact": "Я уже понимаю ситуацию – здесь, скорее всего, потребуется судебное узаконивание. Могу оценить шансы и сказать, как пройти без сноса. Пришлите телефон или Telegram – разберу точнее.",
}

sessions = {}
MAX_CLARIFICATION_ATTEMPTS = 1
MAX_STEP_ATTEMPTS = 2

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
        "step_attempts": {},
        "finished": False,
        "nontarget_issued": False,
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


def call_deepseek(messages):
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "deepseek-chat",
        "messages": messages,
        "temperature": 0.1,
    }
    response = requests.post(DEEPSEEK_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


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
    raw = call_deepseek(messages)
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

    telegram_match = re.search(r'@[\w_]{3,}', text)
    if telegram_match:
        return telegram_match.group(0), "telegram"

    digits = re.sub(r"\D", "", text)

    if len(digits) == 11 and digits[0] in ("7", "8"):
        return text, "phone"

    if len(digits) == 10:
        return text, "phone"

    return None, "unknown"


def detect_cadastral_status(text: str) -> tuple[str | None, str | None]:
    raw = text.strip().lower()

    cadastral_match = re.search(r'\b\d{1,4}:\d{1,4}:\d{1,10}:\d{1,10}\b', text)
    if cadastral_match:
        return cadastral_match.group(0), "provided"

    negative_patterns = [
        "кадастрового номера нет",
        "кадастровый номер отсутствует",
        "нет кадастрового номера",
        "кадастра нет",
        "нет",
        "не знаю",
    ]

    if raw in negative_patterns or any(p in raw for p in negative_patterns):
        return None, "absent"

    return None, None


def heuristic_location_detection(text: str) -> bool:
    text = text.strip().lower()
    location_markers = ["город", "деревня", "посёлок", "район", "снт", "ул", "проспект", "шоссе", "бульвар", "переулок"]
    words = text.split()
    if len(words) <= 5 and any(marker in text for marker in location_markers):
        return True
    return False


def normalize_location(loc: str) -> str:
    if not loc:
        return "не указана"
    loc = loc.strip().lower()
    if loc.startswith("в "):
        loc = loc[2:].strip()
    if loc.startswith("на "):
        loc = loc[3:].strip()
    if loc.startswith("в "):
        loc = loc[2:].strip()
    if loc:
        loc = loc[0].upper() + loc[1:]
    return loc


def determine_case_family(answers: dict) -> str:
    property_segment = answers.get("property_segment") or "unknown"
    object_type = answers.get("object_type") or "unknown"
    issue_type = answers.get("issue_type") or "unknown"
    goal = answers.get("goal") or "unknown"

    if object_type == "house" and issue_type == "unauthorized_construction" and goal == "registration":
        return "residential_house_registration"

    if object_type in ("warehouse", "production", "shop", "office", "building"):
        if issue_type in ("bank_problem", "transaction_block") or goal == "bank_pledge":
            return "commercial_bank_block"
        if issue_type == "document_mismatch":
            return "commercial_document_mismatch"
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
        if issue_type == "shared_ownership_problem":
            return "shared_ownership_issue"

    if issue_type in ("boundary_problem", "land_problem"):
        return "land_boundary_issue"

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
            "secondary_path": "Проверка препятствий и альтернативный правовой сценарий",
            "primary_path_short": "Если участок и параметры дома позволяют, обычно начинают с технического плана и подачи документов на регистрацию.",
            "secondary_path_short": "Если обычная регистрация не проходит из-за земли, параметров дома или документов, сначала нужно понять, что именно мешает.",
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
            "secondary_path": "Проверка препятствий по земле и документам",
            "primary_path_short": "Если объект и земля подходят под упрощённый порядок, обычно начинают с технического плана и регистрации права.",
            "secondary_path_short": "Если по участку, параметрам дома или исходным документам есть ограничения, сначала нужно понять, можно ли идти обычным путём.",
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
            "primary_path_short": "Если дом можно зарегистрировать в обычном порядке, обычно начинают с технического плана и оформления права.",
            "secondary_path_short": "Если обычный путь не проходит, нужно понять, проблема в земле, характеристиках дома или комплекте документов.",
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
            "primary_path_short": "В таких вопросах сначала поднимают документы, межевой план, сведения ЕГРН и фактические границы.",
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


def build_quick_expert_reply_v2(answers: dict, plan: dict) -> str:
    obj_type = answers.get("object_type")
    issue_type = answers.get("issue_type")
    goal = answers.get("goal")
    property_segment = answers.get("property_segment")
    mode = plan.get("consultation_mode", "unknown")

    greetings = [
        "Понял, спасибо за объяснение.",
        "Ясно, типичная ситуация.",
        "Понял, давайте разбираться.",
        "Хорошо, я понял суть."
    ]
    intro = random.choice(greetings)

    if issue_type == "unauthorized_construction" and obj_type == "house":
        return (
            f"{intro}\n\n"
            "У вас незарегистрированное строение, при этом нарушен отступ от границы (менее 3 м).\n\n"
            "Такие объекты обычно не проходят стандартную регистрацию. Два пути:\n"
            "1️⃣ Если нарушение не критично и соседи не против – можно попробовать судебное узаконивание.\n"
            "2️⃣ Если нарушение существенное – потребуется экспертиза и суд.\n\n"
            "Ключевой момент: насколько критично нарушение и есть ли претензии соседей.\n"
            "Где находится объект?"
        )

    if obj_type in ("warehouse", "production", "shop", "office", "building") and issue_type == "document_mismatch":
        return (
            f"{intro}\n\n"
            "У вас коммерческий объект, где факт не совпадает с документами.\n\n"
            "Обычно два пути:\n"
            "1️⃣ Если расхождения можно устранить через новый техплан – без суда.\n"
            "2️⃣ Если объект давно эксплуатируется с изменениями – нужен суд и экспертиза.\n\n"
            "Что проверить: документы на землю, техплан, были ли отказы.\n"
            "Где находится объект?"
        )

    if goal == "bank_pledge" or issue_type == "bank_problem":
        return (
            f"{intro}\n\n"
            "Банк отказал в кредите под залог из-за расхождений.\n\n"
            "Сценарии:\n"
            "1️⃣ Привести документы в соответствие (техплан, ЕГРН) – без суда.\n"
            "2️⃣ Узаконить изменения через суд с экспертизой.\n\n"
            "Что проверить: причину отказа, техплан, землю.\n"
            "Где находится объект?"
        )

    if issue_type in ("registration_absent", "unauthorized_construction") or goal in ("registration", "legalization"):
        if obj_type in ("warehouse", "production", "shop", "office", "building"):
            return (
                f"{intro}\n\n"
                "Объект не зарегистрирован или есть неузаконенные изменения.\n\n"
                "Два варианта:\n"
                "1️⃣ Если всё соответствует нормам – регистрация через техплан и Росреестр.\n"
                "2️⃣ Если есть нарушения – суд и экспертиза.\n\n"
                "Что проверить: документы на землю, техплан, были ли отказы.\n"
                "Где находится объект?"
            )
        else:
            return (
                f"{intro}\n\n"
                "Нужно зарегистрировать объект или узаконить изменения.\n\n"
                "Обычно начинают с техплана и подачи документов.\n"
                "Если есть препятствия – потребуется дополнительный разбор.\n\n"
                "Где находится объект?"
            )

    if obj_type in ("house", "dacha") or property_segment == "residential":
        if goal == "gas_connection":
            return (
                f"{intro}\n\n"
                "Нужно зарегистрировать дом для газа.\n\n"
                "Газовые службы требуют право собственности. Обычно начинают с техплана и Росреестра.\n\n"
                "Где находится объект?"
            )
        else:
            return (
                f"{intro}\n\n"
                "Речь о жилом доме, который нужно оформить.\n\n"
                "Обычно готовят техплан и подают на регистрацию.\n"
                "Если есть препятствия по земле или параметрам – нужен разбор.\n\n"
                "Где находится объект?"
            )

    if mode == "two_paths":
        return (
            f"Понял. Речь о {object_label(answers)}, проблема — {issue_label(answers)}.\n\n"
            f"Два основных сценария:\n"
            f"1️⃣ {plan['primary_path_short']}\n"
            f"2️⃣ {plan['secondary_path_short']}\n\n"
            "Что проверить: право на землю, техдокументацию, отказы.\n"
            "Уточните, где находится объект?"
        )
    elif mode == "one_path_plus_check":
        return (
            f"Понял. Речь о {object_label(answers)}.\n\n"
            f"Базовый путь: {plan['primary_path_short']}\n\n"
            "Если не сработает – потребуется проверка (земля, документы).\n"
            "Где находится объект?"
        )
    else:
        return (
            f"Понял. Ситуация связана с {object_label(answers)} ({issue_label(answers)}).\n\n"
            "Чтобы дать оценку, нужно знать:\n"
            "• где объект;\n"
            "• документы на землю;\n"
            "• были ли отказы.\n\n"
            "Где находится объект?"
        )


def build_question_response(step: str, answers: dict, custom_message: str | None = None):
    return {
        "status": "question",
        "step": step,
        "message_for_user": custom_message or QUESTION_TEXTS[step],
        "collected": {
            "raw_user_problem": answers.get("raw_user_problem"),
            "location_description": answers.get("location_description"),
            "cadastral_number": answers.get("cadastral_number"),
            "cadastral_status": answers.get("cadastral_status"),
            "contact": answers.get("contact"),
            "object_type": answers.get("object_type"),
            "property_segment": answers.get("property_segment"),
            "issue_type": answers.get("issue_type"),
            "goal": answers.get("goal"),
        },
    }


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
    contact = answers.get("contact")

    prompt = f"""Ты — практикующий юрист по недвижимости в России.

На основе данных клиента сделай структурированный юридический разбор.

Данные:
- Тип объекта: {obj}
- Проблема: {issue}
- Цель: {goal}
- Локация: {location}
- Кадастровый номер: {cadastral}
- Статус земли: {land_rights}
- Отказы/споры: {has_refusals}
- Банк/сделка: {bank_context}
- Исходное сообщение клиента: {raw_problem}

Типовой сценарий: {plan.get('primary_path_short', 'не указан')}

Требования к ответу:
- Объём: 1500–2500 символов (максимум 3000 для сложных кейсов).
- Структура:
  **Текущая ситуация**
  **Риски**
  **Стратегия защиты**
  **Основной сценарий решения**
  **Альтернативный сценарий**
  **Вывод**

Стиль: как опытный юрист, без воды, понятным языком, без академических ссылок, опирайся на практику.

"""
    if contact:
        prompt += """
После вывода добавь 2–3 предложения с предложением конкретной помощи (например: составление иска, заказ техплана, сопровождение в суде, регистрация права). Не проси контакт повторно, так как он уже есть.
Пример: "Если хотите, я могу подготовить исковое заявление для вашей ситуации. Или заказать технический план. Просто напишите, и мы начнём."
"""
    else:
        prompt += """
После вывода добавь мягкий призыв оставить контакт (телефон или Telegram) для детального разбора и подготовки плана действий. Например:
"Если хотите, чтобы я разобрал ваш кейс глубже и подготовил точный план действий под ваши документы, оставьте, пожалуйста, телефон или Telegram — я свяжусь с вами."
"""

    messages = [
        {"role": "system", "content": "Ты — опытный юрист по недвижимости. Отвечаешь кратко, по делу, структурированно. Не выдумываешь факты. Используешь понятный русский язык."},
        {"role": "user", "content": prompt}
    ]
    try:
        analysis = call_deepseek(messages)
        if len(analysis) > 3500:
            cut_point = analysis.rfind('.', 0, 3500)
            if cut_point == -1:
                cut_point = 3500
            analysis = analysis[:cut_point+1] + "\n\n(текст сокращён для краткости, но полный разбор будет в заявке)"
        return analysis
    except Exception as e:
        return build_fallback_final_response(answers, plan)


def build_fallback_final_response(answers: dict, plan: dict) -> str:
    obj = object_label(answers)
    issue = issue_label(answers)
    raw_location = answers.get("location_description") or answers.get("region") or "неуточнённом месте"
    location = normalize_location(raw_location)
    plan_lines = []
    if plan.get("primary_path_short"):
        plan_lines.append(f"• {plan['primary_path_short']}")
    if plan.get("secondary_path_short"):
        plan_lines.append(f"• {plan['secondary_path_short']}")
    plan_text = "\n".join(plan_lines) if plan_lines else "• Требуется индивидуальный разбор."
    docs_needed = []
    if answers.get("cadastral_status") != "provided":
        docs_needed.append("кадастровый номер или техплан")
    if not docs_needed:
        docs_needed.append("правоустанавливающие документы на объект и землю")
    docs_text = ", ".join(docs_needed)

    if answers.get("contact"):
        final_message = (
            f"Спасибо. Ситуация понятна: {obj} в {location}, вопрос — {issue}.\n\n"
            f"**Что можно сделать:**\n{plan_text}\n\n"
            f"**Что понадобится:** {docs_text}.\n\n"
            "Контакт уже получен. В ближайшее время юрист свяжется с вами."
        )
    else:
        final_message = (
            f"Спасибо. Ситуация понятна: {obj} в {location}, вопрос — {issue}.\n\n"
            f"**Что можно сделать:**\n{plan_text}\n\n"
            f"**Что понадобится:** {docs_text}.\n\n"
            "Без анализа конкретных документов точный сценарий назвать нельзя. "
            "Оставьте, пожалуйста, телефон или Telegram — я свяжусь с вами, разберу документы и скажу, какой путь оптимален именно для вас."
        )
    return final_message


def build_final_response(answers: dict, lead_category: str, plan: dict):
    use_llm_analysis = False
    if lead_category == "target" and answers.get("contact"):
        obj_type = answers.get("object_type")
        issue_type = answers.get("issue_type")
        if obj_type not in (None, "unknown") and issue_type not in (None, "unknown"):
            use_llm_analysis = True

    if use_llm_analysis:
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


def get_next_step(session: dict):
    answers = session["answers"]

    if not answers.get("raw_user_problem"):
        return "raw_user_problem"

    if not session["consultation_ready"]:
        return "location_description"

    if not answers.get("location_description"):
        return "location_description"

    if answers.get("cadastral_status") is None:
        return "cadastral_number_optional"

    if answers.get("land_rights_status") is None:
        case_family = session.get("consultation_plan", {}).get("case_family", "unknown")
        object_type = answers.get("object_type")
        property_segment = answers.get("property_segment")

        ask_for_land = False
        if case_family.startswith("commercial_"):
            ask_for_land = True
        elif case_family in ("land_boundary_issue", "gas_connection_house", "residential_house_registration", "residential_dacha_registration"):
            ask_for_land = True
        elif object_type in ("warehouse", "production", "shop", "office", "building", "land_plot"):
            ask_for_land = True
        elif property_segment == "commercial":
            ask_for_land = True

        if ask_for_land:
            return "land_rights"

    if not answers.get("contact"):
        return "contact"

    return None


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
        "state": session.get("state"),
        "consultation_ready": session.get("consultation_ready")
    })

    if session.get("finished"):
        return {
            "session_id": session_id,
            "reply": {
                "status": "final",
                "final_message_for_user": "Вы уже оставили контакт. Юрист свяжется с вами в ближайшее время.",
                "form_payload": None,
            }
        }

    answers = session["answers"]

    if session.get("nontarget_issued") and not session.get("consultation_ready"):
        text_lower = user_message.lower()
        strong_legal = ["банк", "росреестр", "залог", "отказ", "кредит", "экспертиз", "суд", "иск", "регистрац", "документ"]
        if not any(w in text_lower for w in strong_legal):
            session["clarification_attempts"] += 1
            response = {
                "status": "question",
                "step": "raw_user_problem",
                "message_for_user": "Я помогаю с юридическими вопросами по недвижимости: регистрация, узаконивание, кредит под залог, сделки. Если ваш вопрос об этом – уточните, пожалуйста, подробнее.",
                "collected": {
                    "raw_user_problem": answers.get("raw_user_problem"),
                    "location_description": answers.get("location_description"),
                    "contact": answers.get("contact"),
                },
            }
            return {"session_id": session_id, "reply": response}

    direct_contact, contact_type = detect_contact(user_message)
    if direct_contact and not answers.get("contact"):
        answers["contact"] = direct_contact
        answers["contact_type"] = contact_type

    cadastral_number, cadastral_status = detect_cadastral_status(user_message)
    if cadastral_status and answers.get("cadastral_status") is None:
        answers["cadastral_status"] = cadastral_status
        if cadastral_number:
            answers["cadastral_number"] = cadastral_number

    if heuristic_location_detection(user_message) and not answers.get("location_description"):
        answers["location_description"] = user_message

    if not answers.get("land_rights_status") and user_message:
        text_lower = user_message.lower()
        if any(word in text_lower for word in ["собственность", "в собственности", "земля в собственности", "да собственность"]):
            answers["land_rights_status"] = "owned"
        elif any(word in text_lower for word in ["аренда", "в аренде"]):
            answers["land_rights_status"] = "leased"
        elif any(word in text_lower for word in ["нет документов", "не оформлена", "нет права"]):
            answers["land_rights_status"] = "not_registered"

    try:
        extracted = extract_fields(user_message, answers)
    except requests.RequestException as e:
        return JSONResponse(status_code=502, content={"error": f"DeepSeek request failed: {str(e)}"})
    except json.JSONDecodeError:
        return JSONResponse(status_code=502, content={"error": "Model returned invalid JSON"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Unexpected server error: {str(e)}"})

    answers = merge_answers(answers, extracted)
    session["answers"] = answers
    session["history"].append({"user": user_message, "extracted": extracted})

    lead_category = classify_lead(answers)
    session["lead_category"] = lead_category

    if lead_category == "nontarget":
        if session["clarification_attempts"] < MAX_CLARIFICATION_ATTEMPTS:
            session["clarification_attempts"] += 1
            session["nontarget_issued"] = True
            response = {
                "status": "question",
                "step": "raw_user_problem",
                "message_for_user": "Я помогаю с юридическими вопросами по недвижимости: регистрация, узаконивание, кредит под залог, сделки. Если ваш вопрос об этом – уточните, пожалуйста, подробнее.",
                "collected": {
                    "raw_user_problem": answers.get("raw_user_problem"),
                    "location_description": answers.get("location_description"),
                    "contact": answers.get("contact"),
                },
            }
            return {"session_id": session_id, "reply": response}

        response = build_nontarget_response(answers)
        return {"session_id": session_id, "reply": response}

    if session.get("nontarget_issued"):
        session["nontarget_issued"] = False

    if not answers.get("raw_user_problem"):
        answers["raw_user_problem"] = user_message

    case_family = determine_case_family(answers)
    plan = build_consultation_plan(case_family)
    session["consultation_plan"] = plan

    if not session["consultation_ready"]:
        session["consultation_ready"] = True
        session["state"] = "consultation"
        reply_text = build_quick_expert_reply_v2(answers, plan)
        log_event("bot_reply", {
            "session_id": session_id,
            "step": "location_description",
            "reply_preview": reply_text[:200]
        })
        return {
            "session_id": session_id,
            "reply": build_question_response("location_description", answers, reply_text)
        }

    next_step = get_next_step(session)

    if next_step:
        attempts = session["step_attempts"].get(next_step, 0) + 1
        session["step_attempts"][next_step] = attempts

        if attempts > MAX_STEP_ATTEMPTS and next_step != "contact":
            if next_step == "location_description":
                answers["location_description"] = "Не указано"
            elif next_step == "cadastral_number_optional":
                answers["cadastral_status"] = "unknown"
            elif next_step == "land_rights":
                answers["land_rights_status"] = "unknown"

            session["state"] = "request_contact"
            return {
                "session_id": session_id,
                "reply": build_question_response(
                    "contact",
                    answers,
                    "Мы не смогли распознать некоторые данные. Пришлите, пожалуйста, телефон или Telegram для связи, и я передам заявку юристу."
                )
            }

        if next_step == "cadastral_number_optional":
            session["state"] = "consultation"
            return {
                "session_id": session_id,
                "reply": build_question_response("cadastral_number_optional", answers)
            }

        if next_step == "land_rights":
            session["state"] = "consultation"
            session["step_attempts"]["land_rights"] = session["step_attempts"].get("land_rights", 0) + 1
            return {
                "session_id": session_id,
                "reply": build_question_response("land_rights", answers)
            }

        if next_step == "contact":
            session["state"] = "request_contact"
            return {
                "session_id": session_id,
                "reply": build_question_response("contact", answers)
            }

        if next_step == "location_description":
            session["state"] = "consultation"
            return {
                "session_id": session_id,
                "reply": build_question_response("location_description", answers)
            }

    session["state"] = "final"
    response = build_final_response(answers, classify_lead(answers), plan)
    session["final_payload"] = response.get("form_payload") or {}

    # Отправляем заявку на email через SMTP (вместо Formspree)
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
