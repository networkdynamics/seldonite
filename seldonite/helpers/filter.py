
def contains_keywords(article, keywords):
    if any(keyword in article.title for keyword in keywords):
        return True
    elif any(keyword in article.text for keyword in keywords):
        return True
    else:
        return False