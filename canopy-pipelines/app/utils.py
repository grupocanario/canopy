def parse_number(number_str):
    # removes leginility dots
    number_str = number_str.replace('.', '')
    # replaces comma with dot for float
    number_str = number_str.replace(',', '.')

    try:
        num = float(number_str)
        return num
    except ValueError:
        return None


def without_keys(d, keys):
    return {k: v for k, v in d.items() if k not in keys}
