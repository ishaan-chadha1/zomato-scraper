import csv
import time
from playwright.sync_api import sync_playwright

def scrape_zomato_3pass():
    # Your list of areas to scrape
    areas = ['Indiranagar', 'Koramangala', 'Whitefield']
    
    # CSS Selectors
    CARD_SELECTOR = "div.jumbo-tracker" 
    
    # GLOBAL DICTIONARY: This holds absolute unique restaurants across ALL areas
    global_unique_restaurants = {}
    
    # Open the area-specific CSV file
    with open('zomato_restaurants_3pass.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Area', 'Restaurant Name', 'Rating', 'Cuisine', 'Price', 'Link'])

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False) 
            context = browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            # --- HELPER FUNCTION: Handles the scrolling and extraction ---
            def scroll_and_extract(current_area, pass_name):
                print(f"  -> Starting scroll for {pass_name}...")
                previous_item_count = 0
                retries = 0
                max_retries = 3

                while retries < max_retries:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(500)
                    
                    page.evaluate("window.scrollBy(0, -400)")
                    page.wait_for_timeout(2500) 

                    current_item_count = page.locator(CARD_SELECTOR).count()
                    print(f"    Loaded {current_item_count} restaurants so far...")

                    if current_item_count == previous_item_count:
                        retries += 1
                    else:
                        retries = 0
                        previous_item_count = current_item_count

                print(f"  -> Extracting data for {pass_name}...")
                scraped_data = page.evaluate("""
                    (selector) => {
                        const results = [];
                        const cards = document.querySelectorAll(selector);
                        
                        cards.forEach(card => {
                            const nameEl = card.querySelector('h4');
                            const name = nameEl ? nameEl.innerText.trim() : 'N/A';
                            
                            const ratingEl = card.querySelector('div[class*="rating"]');
                            const rating = ratingEl ? ratingEl.innerText.trim() : 'N/A';
                            
                            const pTags = card.querySelectorAll('p');
                            const cuisine = pTags.length > 0 ? pTags[0].innerText.trim() : 'N/A';
                            const price = pTags.length > 1 ? pTags[1].innerText.trim() : 'N/A';
                            
                            const linkEl = card.querySelector('a');
                            const link = linkEl ? linkEl.href : 'N/A';
                            
                            results.push({ name, rating, cuisine, price, link });
                        });
                        
                        return results;
                    }
                """, CARD_SELECTOR)
                
                return scraped_data
            # --- END HELPER FUNCTION ---


            for area in areas:
                print(f"\n================ Processing area: {area} ================")
                
                # Area-specific dictionary
                unique_restaurants = {}

                try:
                    # 1. RELOAD THE BASE URL 
                    print("Loading clean Zomato homepage...")
                    page.goto('https://www.zomato.com/bangalore/restaurants', wait_until='domcontentloaded')
                    page.wait_for_timeout(2000)

                    # 2. Locate the location input box
                    try:
                        location_input = page.get_by_placeholder("Bengaluru")
                        location_input.wait_for(state='attached', timeout=2000)
                    except:
                        location_input = page.locator('input').first

                    location_input.click()
                    page.keyboard.press('Meta+A') 
                    page.keyboard.press('Backspace')
                    location_input.fill(area)

                    # 3. Wait for suggestions and select
                    print("Waiting for dropdown suggestions...")
                    page.wait_for_timeout(2000) 
                    page.keyboard.press('ArrowDown') 
                    page.wait_for_timeout(500)       
                    page.keyboard.press('Enter')     
                    
                except Exception as e:
                    print(f"Failed to input location for {area}. Error: {e}")
                    continue

                print("Loading area page...")
                page.wait_for_timeout(4000)


                # ==========================================
                # PASS 1: DEFAULT (POPULARITY)
                # ==========================================
                print("\n--- Pass 1: Default (Popularity) ---")
                pass1_data = scroll_and_extract(area, "Pass 1")
                
                for item in pass1_data:
                    if item['link'] != 'N/A':
                        unique_restaurants[item['link']] = item


                # ==========================================
                # PASS 2: RATING (HIGH TO LOW)
                # ==========================================
                print("\n--- Pass 2: Rating (High to Low) ---")
                try:
                    page.evaluate("window.scrollTo(0, 0)")
                    page.wait_for_timeout(1000)
                    
                    page.locator('text=Filters').first.click()
                    page.wait_for_timeout(1000)
                    
                    page.locator('text=Rating: High to Low').click()
                    page.wait_for_timeout(500)
                    
                    page.locator('text=Apply').click()
                    print("Filter applied. Waiting for old grid to clear...")
                    
                    # SMART WAIT: Wait until Zomato wipes the old items
                    for _ in range(15):
                        if page.locator(CARD_SELECTOR).count() < 100:
                            break
                        page.wait_for_timeout(1000)
                        
                    page.wait_for_timeout(3000) # Give the new cards a moment to render
                    
                    pass2_data = scroll_and_extract(area, "Pass 2")
                    for item in pass2_data:
                        if item['link'] != 'N/A':
                            unique_restaurants[item['link']] = item 
                
                except Exception as e:
                    print(f"Failed during Pass 2: {e}")


                # ==========================================
                # PASS 3: COST (HIGH TO LOW)
                # ==========================================
                print("\n--- Pass 3: Cost (High to Low) ---")
                try:
                    page.evaluate("window.scrollTo(0, 0)")
                    page.wait_for_timeout(1000)
                    
                    page.locator('text=Filters').first.click()
                    page.wait_for_timeout(1000)
                    
                    page.locator('text=Cost: High to Low').click()
                    page.wait_for_timeout(500)
                    
                    page.locator('text=Apply').click()
                    print("Filter applied. Waiting for old grid to clear...")
                    
                    # SMART WAIT: Wait until Zomato wipes the old items
                    for _ in range(15):
                        if page.locator(CARD_SELECTOR).count() < 100:
                            break
                        page.wait_for_timeout(1000)
                        
                    page.wait_for_timeout(3000) 
                    
                    pass3_data = scroll_and_extract(area, "Pass 3")
                    for item in pass3_data:
                        if item['link'] != 'N/A':
                            unique_restaurants[item['link']] = item
                
                except Exception as e:
                    print(f"Failed during Pass 3: {e}")


                # ==========================================
                # FINAL STEP: WRITE TO AREA CSV & UPDATE GLOBAL LIST
                # ==========================================
                print(f"\nWriting {len(unique_restaurants)} unique records for {area} to area CSV...")
                for link, item in unique_restaurants.items():
                    # Write to the per-area CSV
                    writer.writerow([area, item['name'], item['rating'], item['cuisine'], item['price'], link])
                    
                    # Update our Global Dictionary for the Master File
                    item['area'] = area # Keep track of the area it was found in
                    global_unique_restaurants[link] = item
                
                # Sleep between areas
                time.sleep(3)

            browser.close()
            print("\nArea-specific scraping complete! File saved: zomato_restaurants_3pass.csv")

    # ==========================================
    # GLOBAL DEDUPLICATION DUMP
    # ==========================================
    print(f"\nCreating Master CSV... Found {len(global_unique_restaurants)} TOTAL unique restaurants across all areas.")
    
    with open('zomato_master_unique.csv', mode='w', newline='', encoding='utf-8') as master_file:
        master_writer = csv.writer(master_file)
        master_writer.writerow(['Area Found', 'Restaurant Name', 'Rating', 'Cuisine', 'Price', 'Link'])
        
        for link, item in global_unique_restaurants.items():
            master_writer.writerow([item['area'], item['name'], item['rating'], item['cuisine'], item['price'], link])
            
    print("Master deduplicated file saved: zomato_master_unique.csv")

if __name__ == "__main__":
    scrape_zomato_3pass()