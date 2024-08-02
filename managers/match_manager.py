import re

from fuzzywuzzy import fuzz


class MatchManager:
    @staticmethod
    def find_best_match(label_name, results, threshold=70):
        best_match = None
        best_score = 0
        for result in results:
            score = fuzz.token_sort_ratio(label_name.lower(), result['name'].lower())
            if score > best_score and score >= threshold:
                best_score = score
                best_match = result
        return best_match

    @staticmethod
    def find_demo_email(description):
        if description:
            lower_desc = ' '.join(description.lower().split())
            demo_match = re.search(r'demo.*?([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', lower_desc)
            return demo_match.group(1) if demo_match else None
        return None
        # all_emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', description)
        # if all_emails:
        #     return all_emails[0]
