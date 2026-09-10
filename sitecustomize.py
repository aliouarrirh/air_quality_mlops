"""
Patch protobuf 3.x pour compatibilite avec dbt qui utilise always_print_fields_with_no_presence
(parametre ajoute dans protobuf 4.x).
"""
try:
    import inspect
    from google.protobuf import json_format
    if "always_print_fields_with_no_presence" not in inspect.signature(json_format.MessageToJson).parameters:
        import functools
        _orig = json_format.MessageToJson
        @functools.wraps(_orig)
        def _patched(*args, **kwargs):
            kwargs.pop("always_print_fields_with_no_presence", None)
            return _orig(*args, **kwargs)
        json_format.MessageToJson = _patched
except Exception:
    pass
