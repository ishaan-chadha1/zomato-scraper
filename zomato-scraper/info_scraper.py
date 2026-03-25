import json
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


def get_info(url):
    """ Get Information about the restaurant from URL """
    
    global headers
    webpage = requests.get(url, headers=headers, timeout=3)
    html_text = parse_html(webpage.text)
    scripts = html_text.find_all('script', type='application/ld+json')
    info = {}
    for script in scripts:
        if not script.string:
            continue
        try:
            parsed = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and item.get('@type') == 'Restaurant':
                    info = item
                    break
            if info:
                break
        elif isinstance(parsed, dict) and parsed.get('@type') == 'Restaurant':
            info = parsed
            break

    address = info.get('address', {})
    geo = info.get('geo', {})
    aggregate_rating = info.get('aggregateRating', {})
    
    data = (
        info.get('@type'), info.get('name'), info.get('url'), info.get('openingHours'),
        address.get('streetAddress'), address.get('addressLocality'),
        address.get('addressRegion'), address.get('postalCode'),
        address.get('addressCountry'),
        geo.get('latitude'), geo.get('longitude'),
        info.get('telephone'), info.get('priceRange'), info.get('paymentAccepted'),
        info.get('image'), info.get('servesCuisine'),
        aggregate_rating.get('ratingValue'), aggregate_rating.get('ratingCount')
    )
    return data


def save_df(file_name, df):
    """ Save the dataframe """
    
    df.to_csv(file_name, index=False)
    

def get_restaurant_info(url_list, save=True, file_name="Restaurants.csv"):
    """ Get Restaurant Information from all urls passed """

    # Collecting the data
    data = []
    for url in url_list:
        data.append(get_info(url))
        
    # Creating the DataFrame
    columns = ['Type', 'Name', 'URL', 'Opening_Hours',
               'Street', 'Locality', 'Region', 'PostalCode', 'Country',
               'Latitude', 'Longitude', 'Phone',
               'Price_Range', 'Payment_Methods',
               'Image_URL', 'Cuisine', 'Rating', 'Rating_Count']
    info_df = pd.DataFrame(data, columns=columns)
    
    # Save the df
    if save:
        save_df(file_name, info_df)
        
    return info_df


if __name__ == "__main__":
    urls = ["https://www.zomato.com/bangalore/voosh-thalis-bowls-1-bellandur-bangalore",
            "https://www.zomato.com/bangalore/flying-kombucha-itpl-main-road-whitefield-bangalore",
            "https://www.zomato.com/bangalore/matteo-coffea-indiranagar"]
    get_restaurant_info(urls)
