import re
def fill_template(js_obj, **params):
    if isinstance(js_obj, dict):
        return {k.format(**params): fill_template(v, **params) for k, v in js_obj.items()}
    elif isinstance(js_obj, list):
        return [fill_template(v, **params) for v in js_obj]
    elif isinstance(js_obj, str):
        # check if only replacement
        if re.search(r'{[^{}]+}', js_obj):
            return params[js_obj[1:-1]]
        # format as string by default
        return js_obj.format(**params)
    else:
        return js_obj
