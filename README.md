# Zomato-Scraper
A script that can scrape Restaurant information, menu items, and reviews using the restaurant link.

### Zomato

Zomato (/zoʊmɑːtoʊ/) is an Indian multinational restaurant aggregator and food delivery company founded by Pankaj Chaddah and Deepinder Goyal in 2008.

Zomato provides information, menus and user-reviews of restaurants as well as food delivery options from partner restaurants in select cities. 
The service is available in over 24 countries and in more than 10,000 cities. 

### Details of the Scraped data
This scraped dataset contains many attributes of the restaurant, such as:

- Name, Weblink, Open Hours, Address, Phone no, Price Range, Cuisine type.
- The Rating, Number of Ratings.
- Reviews - You can specify the max number of reviews to scrape for.
- Menu items - Description, Price and Tags.

### Ideas on how to use the data
- The Information can be used to form a catalogue of all restaurant in a given area.
- Create Geographical maps with the most famous cusine in a geo-location.
- The Menu items can be analysed to find which items are popular and gain insights to help to manage a restaurant.
- The Review data can be used to build a classification or sentiment-analysis algorithm.

### Interactive top-down scraper (recommended)
This project now supports an interactive workflow:

1. Discover and print the top N restaurants by rating (default 50)
2. Choose a rank range (example: `1-10`, `1-5,8,12-15`)
3. Scrape menu + reviews for only the selected ranks
4. Optionally scrape another range from the same discovered top list (no rediscovery needed)

Run from repo root:

```bash
./run.sh bangalore 50 6 50 35
```

Arguments:
- `city` (default `bangalore`)
- `top-count` (default `50`)
- `concurrency` (default `6`)
- `max-reviews` (default `50`)
- `max-scrolls` (default `35`)

Main generated outputs:
- `zomato-scraper/Top_50_Restaurants.csv`
- `zomato-scraper/Selected_Restaurants_<timestamp>.csv`
- `zomato-scraper/Interactive_Run_Summary_<timestamp>.csv`
- `zomato-scraper/Restaurants.csv`
- `zomato-scraper/Menu/*.csv`
- `zomato-scraper/Reviews/*.csv`
