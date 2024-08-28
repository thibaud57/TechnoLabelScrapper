import re

from fuzzywuzzy import fuzz


def find_best_match(label_name, results, threshold=70):
    best_match = None
    best_score = 0
    for result in results:
        score = fuzz.token_sort_ratio(label_name.lower(), result['name'].lower())
        if score > best_score and score >= threshold:
            best_score = score
            best_match = result
    return best_match


def find_demo_email(description):
    if description:
        lower_desc = ' '.join(description.lower().split())
        demo_match = re.search(r'\b([a-zA-Z0-9._%+-]*demo[s]?[a-zA-Z0-9._%+-]*@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b',
                               lower_desc)
        if demo_match:
            return demo_match.group(1)
        demo_email_match = re.search(r'\bdemo.*?([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', lower_desc)
        if demo_email_match:
            return demo_email_match.group(1)
    return None

    # all_emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', description)
    # if all_emails:
    #     return all_emails[0]


def format_title_case(text):
    if not text:
        return ''
    words = text.lower().split()
    title_case = [word.capitalize() for word in words]
    if title_case:
        title_case[0] = title_case[0].capitalize()
        return ' '.join(title_case)


def extract_number(value: str) -> int:
    match = re.search(r'\d+', value)
    return int(match.group()) if match else 0
