import csv
import time
import random
import os
from playwright.sync_api import sync_playwright

def scroll_and_extract(page, current_area, pass_name):
    CARD_SELECTOR = "div.jumbo-tracker"
    
    # === THE POPUP SQUASHER ===
    # Hit Escape a few times to kill standard modals before scrolling
    page.keyboard.press('Escape')
    page.wait_for_timeout(500)
    page.keyboard.press('Escape')
    page.wait_for_timeout(500)
    
    # Try to explicitly click any generic close buttons if they exist
    try:
        page.locator('section[role="dialog"] button').first.click(timeout=1000)
    except:
        pass
    # ==========================

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


def run_pipeline():
    CARD_SELECTOR = "div.jumbo-tracker"
    
    if not os.path.exists('areas.txt'):
        print("Error: areas.txt not found! Please create it and add your locations.")
        return

    if not os.path.exists('completed_areas.log'):
        open('completed_areas.log', 'w').close()

    with open('completed_areas.log', 'r') as f:
        completed_areas = set(line.strip() for line in f)

    with open('areas.txt', 'r') as f:
        all_areas = [line.strip() for line in f if line.strip()]

    pending_areas = [a for a in all_areas if a not in completed_areas]
    
    print(f"Total areas: {len(all_areas)} | Completed: {len(completed_areas)} | Pending: {len(pending_areas)}")
    
    if not pending_areas:
        print("All areas in areas.txt have already been scraped!")
        return

    area_file = 'zomato_restaurants_3pass.csv'
    master_file = 'zomato_master_unique.csv'
    
    if not os.path.exists(area_file):
        with open(area_file, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(['Area', 'Restaurant Name', 'Rating', 'Cuisine', 'Price', 'Link'])

    global_seen_links = set()
    if os.path.exists(master_file):
        with open(master_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) 
            for row in reader:
                if len(row) >= 6:
                    global_seen_links.add(row[5])
    else:
        with open(master_file, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(['Area Found', 'Restaurant Name', 'Rating', 'Cuisine', 'Price', 'Link'])

    BATCH_SIZE = 25
    
    for i in range(0, len(pending_areas), BATCH_SIZE):
        chunk = pending_areas[i:i+BATCH_SIZE]
        print(f"\n🚀 Launching browser for batch of {len(chunk)} areas...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            for area in chunk:
                print(f"\n================ Processing area: {area} ================")
                unique_restaurants = {}

                try:
                    print("Loading clean Zomato homepage...")
                    page.goto('https://www.zomato.com/bangalore/restaurants', wait_until='domcontentloaded')
                    page.wait_for_timeout(2000)

                    try:
                        location_input = page.get_by_placeholder("Bengaluru")
                        location_input.wait_for(state='attached', timeout=2000)
                    except:
                        location_input = page.locator('input').first

                    location_input.click()
                    page.keyboard.press('Meta+A') 
                    page.keyboard.press('Backspace')
                    location_input.fill(area)

                    print("Waiting for dropdown suggestions...")
                    # INCREASED WAIT TIME: Gives Zomato's API more time to load the dropdown
                    page.wait_for_timeout(3500) 
                    page.keyboard.press('ArrowDown') 
                    page.wait_for_timeout(500)       
                    page.keyboard.press('Enter')     
                    
                except Exception as e:
                    print(f"Failed to input location for {area}. Skipping to next...")
                    continue 

                print("Loading area page...")
                page.wait_for_timeout(4000)

                # PASS 1: DEFAULT
                print("\n--- Pass 1: Default (Popularity) ---")
                pass1_data = scroll_and_extract(page, area, "Pass 1")
                for item in pass1_data:
                    if item['link'] != 'N/A':
                        unique_restaurants[item['link']] = item

                # PASS 2: RATING
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
                    
                    for _ in range(15):
                        if page.locator(CARD_SELECTOR).count() < 100:
                            break
                        page.wait_for_timeout(1000)
                        
                    page.wait_for_timeout(3000) 
                    pass2_data = scroll_and_extract(page, area, "Pass 2")
                    for item in pass2_data:
                        if item['link'] != 'N/A':
                            unique_restaurants[item['link']] = item 
                except Exception as e:
                    print(f"Failed during Pass 2: {e}")

                # PASS 3: COST
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
                    
                    for _ in range(15):
                        if page.locator(CARD_SELECTOR).count() < 100:
                            break
                        page.wait_for_timeout(1000)
                        
                    page.wait_for_timeout(3000) 
                    pass3_data = scroll_and_extract(page, area, "Pass 3")
                    for item in pass3_data:
                        if item['link'] != 'N/A':
                            unique_restaurants[item['link']] = item
                except Exception as e:
                    print(f"Failed during Pass 3: {e}")

                # 4. WRITE DATA ON THE FLY
                print(f"\nWriting {len(unique_restaurants)} records to CSVs...")
                
                with open(area_file, 'a', newline='', encoding='utf-8') as af, \
                     open(master_file, 'a', newline='', encoding='utf-8') as mf:
                    
                    area_writer = csv.writer(af)
                    master_writer = csv.writer(mf)
                    
                    for link, item in unique_restaurants.items():
                        area_writer.writerow([area, item['name'], item['rating'], item['cuisine'], item['price'], link])
                        
                        if link not in global_seen_links:
                            master_writer.writerow([area, item['name'], item['rating'], item['cuisine'], item['price'], link])
                            global_seen_links.add(link)

                with open('completed_areas.log', 'a') as f:
                    f.write(area + '\n')
                    
                sleep_time = random.uniform(4.5, 8.5)
                print(f"✅ Finished {area}. Taking a human-like break for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

            browser.close()
            print("\n🧹 Batch complete. Flushing browser memory and restarting...")

    print("\n🎉 SCRAPING PIPELINE FULLY COMPLETE! 🎉")

if __name__ == "__main__":
    run_pipeline()