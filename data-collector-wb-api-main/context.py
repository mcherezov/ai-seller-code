from contextvars import ContextVar

# Контекстная переменная для режима генерации
current_mode = ContextVar("current_mode", default="default")