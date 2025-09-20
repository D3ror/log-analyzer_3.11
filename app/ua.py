# app/ua.py
import re
from ua_parser import user_agent_parser

BOT_KEYWORDS = re.compile(
    r"bot|crawl|spider|slurp|bingpreview|facebookexternalhit|wget|curl", re.I
)


def parse_ua(ua_string: str) -> str:
    parsed = user_agent_parser.Parse(ua_string or "")
    return parsed["user_agent"]["family"] or ""


def is_bot_ua(ua_string: str) -> bool:
    if not ua_string:
        return False
    if BOT_KEYWORDS.search(ua_string):
        return True
    fam = parse_ua(ua_string)
    known_bots = {"Googlebot", "Bingbot", "YandexBot", "Baiduspider", "DuckDuckBot"}
    return fam in known_bots
