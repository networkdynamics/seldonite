
def contains_keywords(article, keywords):
    if any(keyword in article.title for keyword in keywords):
        return True
    elif any(keyword in article.content for keyword in keywords):
        return True
    else:
        return False