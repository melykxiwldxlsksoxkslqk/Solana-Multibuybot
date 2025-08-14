import asyncio
import logging
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
import os

logger = logging.getLogger(__name__)

async def get_kolscan_wallets():
    """
    Fetches wallets by navigating to the kolscan.io leaderboard, clicking each trader's
    profile, extracting the wallet address from the URL, and then navigating back.
    """
    leaderboard_url = "https://kolscan.io/leaderboard"
    wallets = []
    browser = None
    try:
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            logger.info(f"Navigating to leaderboard: {leaderboard_url}")
            await page.goto(leaderboard_url, timeout=90000, wait_until='domcontentloaded')

            trader_link_selector = 'div.leaderboard_leaderboardUser__8OZpJ > a'
            await page.wait_for_selector(trader_link_selector, timeout=60000)
            
            trader_links = await page.query_selector_all(trader_link_selector)
            logger.info(f"Found {len(trader_links)} traders on the leaderboard.")

            for i in range(len(trader_links)):
                # Re-fetch links on each iteration to avoid stale element reference
                all_links = await page.query_selector_all(trader_link_selector)
                if i >= len(all_links):
                    logger.warning("Stale element reference detected. Breaking loop.")
                    break
                
                link = all_links[i]
                name = await link.inner_text()
                
                try:
                    logger.info(f"Processing trader #{i+1}: {name}. Clicking profile...")
                    await link.click()
                    await page.wait_for_url("**/account/**", timeout=30000)
                    
                    address = page.url.split('/')[-1]
                    
                    if 'leaderboard' not in address and name:
                        wallets.append({"name": name.strip(), "address": address})
                        logger.info(f"SUCCESSFULLY SCRAPED: {name.strip()} -> {address}")
                    else:
                        logger.warning(f"Failed to get a valid account URL for trader {name}. URL was: {page.url}")

                    logger.info(f"Navigating back to leaderboard from {page.url}")
                    await page.goto(leaderboard_url, timeout=60000, wait_until='domcontentloaded')
                    await page.wait_for_selector(trader_link_selector, timeout=60000)

                except PlaywrightTimeoutError as e:
                    logger.error(f"Timeout processing trader #{i+1} ({name}). Skipping. Error: {e}")
                    await page.goto(leaderboard_url, timeout=60000)
                except Exception as e:
                    logger.error(f"An unexpected error occurred for trader #{i+1}: {e}", exc_info=True)
                    await page.goto(leaderboard_url, timeout=60000)
                        
    except Exception as e:
        logger.error(f"A critical failure occurred during scraping: {e}", exc_info=True)
        # In case of a major failure, save debug info
        if 'page' in locals() and page and not page.is_closed():
            debug_dir = 'debug'
            os.makedirs(debug_dir, exist_ok=True)
            screenshot_path = os.path.join(debug_dir, 'kolscan_timeout_screenshot.png')
            html_path = os.path.join(debug_dir, 'kolscan_timeout_page.html')
            await page.screenshot(path=screenshot_path, full_page=True)
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(await page.content())
            logger.info(f"Saved debug info to {debug_dir}")
    finally:
        if browser:
            await browser.close()
            logger.info("Browser closed.")
            
    logger.info(f"Scraping complete. Successfully collected {len(wallets)} wallets.")
    return wallets 