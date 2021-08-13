_slug_translate_table = {
    ord(char): u""
    for char in [
        '"',
        "#",
        "$",
        "%",
        "&",
        "+",
        ",",
        "/",
        ":",
        ";",
        "=",
        "?",
        "@",
        "[",
        "\\",
        "]",
        "^",
        "`",
        "{",
        "|",
        "}",
        "~",
        "'",
    ]
}


def slugify(text):
    text = text.translate(_slug_translate_table)
    text = u"_".join(text.split())
    return text.lower()
