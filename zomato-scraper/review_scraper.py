import os
import json
import re
from urllib.parse import urlparse
import requests
import pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh;'
                         ' Intel Mac OS X 10_15_4)'
                         ' AppleWebKit/537.36 (KHTML, like Gecko)'
                         ' Chrome/83.0.4103.97 Safari/537.36'}


def parse_html(text):
    try:
        return BeautifulSoup(text, 'lxml')
    except FeatureNotFound:
        return BeautifulSoup(text, 'html.parser')


def clean_reviews(html_text):
    """ Cleans and collect the review from the html """

    reviews = []
    scripts = html_text.find_all('script', type='application/ld+json')
    for script in scripts:
        if not script.string:
            continue
        try:
            parsed = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        if isinstance(parsed, dict):
            if isinstance(parsed.get('reviews'), list):
                reviews = parsed['reviews']
                break
            if isinstance(parsed.get('review'), list):
                reviews = parsed['review']
                break

    data = []
    for review in reviews:
        rating = review.get('reviewRating', {})
        data.append((
            review.get('author'),
            review.get('url'),
            review.get('description'),
            rating.get('ratingValue')
        ))
    return data


def clean_reviews_from_preloaded_state(page_text):
    """Extract review rows from window.__PRELOADED_STATE__ payload."""
    match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\("(.*)"\);', page_text, re.S)
    if not match:
        return []

    try:
        decoded = bytes(match.group(1), 'utf-8').decode('unicode_escape')
        state = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []

    reviews_obj = state.get('entities', {}).get('REVIEWS', {})
    if not isinstance(reviews_obj, dict):
        return []

    rows = []
    for review in reviews_obj.values():
        if not isinstance(review, dict):
            continue
        rows.append((
            review.get('userName'),
            review.get('reviewUrl'),
            review.get('reviewText') or review.get('reviewTextSm') or "",
            review.get('ratingV2')
        ))
    return rows


def save_df(file_name, df):
    """ Save the dataframe """
    
    if not os.path.exists("Reviews"):
        os.makedirs("Reviews")
    df.to_csv(f"Reviews/{file_name}.csv", index=False)


def sanitize_file_name(name, fallback="Unknown Restaurant"):
    if not name:
        name = fallback
    name = re.sub(r'[<>:"/\\|?*]', '', name).strip()
    return name or fallback


def restaurant_name_from_url(url):
    path = urlparse(url).path.strip("/")
    slug = path.split("/")[-1] if path else ""
    slug = slug.replace("-1", "")
    return slug.replace("-", " ").title().strip() or "Unknown Restaurant"


def get_reviews(url, max_reviews, sort='popular', save=True, save_empty=False):
    """ Get all Reviews from the passed url """
    
    global headers
    
    # Setiing variables for the scraping
    max_reviews = max_reviews//5
    if sort == 'popular':
        sort = '&sort=rd'
    elif sort == 'new':
        sort = '&sort=dd'
    
    reviews = []
    prev_data = None
    rn = ""
    fallback_name = restaurant_name_from_url(url)

    # Collecting the reviews
    for i in range(1, max_reviews):
        link = url+f"/reviews?page={i}{sort}"
        webpage = requests.get(link, headers=headers, timeout=5)
        html_text = parse_html(webpage.text)
        title = html_text.title.text if html_text.title else ""
        if title:
            rn = title
        data = clean_reviews(html_text)
        if not data:
            data = clean_reviews_from_preloaded_state(webpage.text)
        if prev_data == data:
            break
        reviews.extend(data)
        prev_data = data
    
    # Creating the DataFrame
    if "User Reviews" in rn:
        restaurant_name = rn.split("User Reviews", 1)[0].strip(" -|")
    elif "|" in rn:
        restaurant_name = rn.split("|", 1)[0].strip()
    else:
        restaurant_name = rn.strip()

    if restaurant_name.lower().startswith("reviews of "):
        restaurant_name = restaurant_name[11:].strip()

    restaurant_name = sanitize_file_name(restaurant_name, fallback=fallback_name)
    columns = ['Author', 'Review URL', 'Description', 'Rating']
    review_df = pd.DataFrame(reviews, columns=columns)
    
    # Saving the df
    if save and (save_empty or not review_df.empty):
        save_df(restaurant_name, review_df)
    elif save and review_df.empty:
        print(f"[reviews] No reviews found for: {restaurant_name}")
    
    return review_df


if __name__ == "__main__":
    get_reviews("https://www.zomato.com/bangalore/meghana-foods-marathahalli-bangalore", 70, sort='new')
