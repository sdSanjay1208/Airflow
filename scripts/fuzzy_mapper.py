from rapidfuzz import process, fuzz

def fuzzy_match(value, lookup_list, threshold=80):
    if not value:
        return None

    match, score, _ = process.extractOne(
        value,
        lookup_list,
        scorer=fuzz.token_sort_ratio
    )

    return match if score >= threshold else None
