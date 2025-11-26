import re

DENY_PATTERNS = re.compile(r"\b(insert|update|delete|drop|alter|create)\b", re.I)

def validate_sql(sql):
 if DENY_PATTERNS.search(sql):
  return False
 return True
