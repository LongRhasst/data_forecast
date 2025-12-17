import asyncio
import json
import logging
import argparse
import re
import random
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from tqdm.asyncio import tqdm
import aiohttp

# Setup logging
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tiki_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class TikiPlaywrightScraper:
    def __init__(self, search_term, max_products=10, max_reviews=30, headless=False):
        """
        Scraper sử dụng Playwright để lấy dữ liệu từ Tiki.
        
        Args:
            search_term: Từ khóa tìm kiếm
            max_products: Số lượng sản phẩm tối đa
            max_reviews: Số lượng review tối đa cho mỗi sản phẩm
            headless: Chạy browser ẩn hay không
        """
        self.search_term = search_term
        self.max_products = max_products
        self.max_reviews = max_reviews
        self.headless = headless
        self.output_file = f"tiki_{re.sub(r'[^a-z0-9_]+', '', search_term.lower())}.json"
        self.products_data = []
        self.state_file = "tiki_state.json"
        
    async def _save_cookies(self, context):
        """Lưu cookies và storage state để duy trì session"""
        try:
            await context.storage_state(path=self.state_file)
            logging.info(f"Đã lưu session vào {self.state_file}")
        except Exception as e:
            logging.warning(f"Không thể lưu session: {e}")
    
    async def _load_cookies(self):
        """Kiểm tra xem có file state đã lưu không"""
        return Path(self.state_file).exists()
    
    async def _human_like_delay(self, min_sec=1, max_sec=3):
        """Tạo delay ngẫu nhiên giống người dùng thật"""
        delay = random.uniform(min_sec, max_sec)
        await asyncio.sleep(delay)
    
    async def _simulate_human_behavior(self, page):
        """Mô phỏng hành vi người dùng: di chuyển chuột, scroll tự nhiên"""
        try:
            # Random mouse movements
            for _ in range(random.randint(2, 4)):
                x = random.randint(100, 1500)
                y = random.randint(100, 800)
                await page.mouse.move(x, y)
                await self._human_like_delay(0.2, 0.5)
            
            # Scroll tự nhiên từng đoạn nhỏ
            scroll_steps = random.randint(3, 5)
            for _ in range(scroll_steps):
                scroll_amount = random.randint(200, 500)
                await page.evaluate(f'window.scrollBy({{top: {scroll_amount}, behavior: "smooth"}})')
                await self._human_like_delay(0.5, 1.2)
            
            # Scroll back lên một chút
            if random.random() > 0.5:
                await page.evaluate(f'window.scrollBy({{top: -{random.randint(100, 300)}, behavior: "smooth"}})')
                await self._human_like_delay(0.3, 0.8)
                
        except Exception as e:
            logging.warning(f'Lỗi khi simulate human behavior: {e}')
    
    async def scrape(self):
        """Hàm chính để scrape dữ liệu"""
        async with async_playwright() as p:
            try:
                browser = await p.firefox.launch(
                    headless=self.headless,
                    firefox_user_prefs={
                        'dom.webdriver.enabled': False,
                        'useAutomationExtension': False,
                    }
                )
                logging.info("✅ Đang sử dụng Firefox")
            except Exception as e:
                logging.error(f"❌ Không thể khởi động Firefox: {e}")
                raise
            
            # Load state nếu có
            has_saved_state = await self._load_cookies()
            
            if has_saved_state:
                logging.info("Tìm thấy session đã lưu. Đang load...")
                context = await browser.new_context(
                    storage_state=self.state_file,
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
                    locale='vi-VN',
                    timezone_id='Asia/Ho_Chi_Minh'
                )
            else:
                logging.info("Không tìm thấy session. Tạo session mới...")
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
                    locale='vi-VN',
                    timezone_id='Asia/Ho_Chi_Minh'
                )
            
            # Anti-detection script
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
            """)
            
            page = await context.new_page()
            
            try:
                # 1. Tìm kiếm sản phẩm qua API
                logging.info(f"🔍 Đang tìm kiếm: {self.search_term}")
                logging.info("📡 Sử dụng Tiki API để tìm kiếm...")
                products = await self._search_products(page)
                
                if not products:
                    logging.error("❌ Không tìm thấy sản phẩm nào!")
                    return
                
                logging.info(f"✅ Tìm thấy {len(products)} sản phẩm")
                
                await self._save_cookies(context)
                
                # 2. Lấy chi tiết và reviews cho từng sản phẩm
                logging.info(f"Tìm thấy {len(products)} sản phẩm. Đang lấy chi tiết...")
                for idx, product in enumerate(tqdm(products, desc="Scraping products")):
                    try:
                        # Delay ngẫu nhiên giữa các sản phẩm
                        if idx > 0:
                            await self._human_like_delay(3, 6)
                        
                        await self._scrape_product_details(page, product)
                        self.products_data.append(product)
                        
                        # Lưu định kỳ
                        if (idx + 1) % 5 == 0:
                            self._save_data()
                            await self._save_cookies(context)
                    except Exception as e:
                        logging.error(f"Lỗi khi scrape sản phẩm {product.get('name', 'Unknown')}: {e}")
                        continue
                
                # Lưu lần cuối
                self._save_data()
                await self._save_cookies(context)
                logging.info(f"Hoàn thành! Dữ liệu đã lưu vào {self.output_file}")
                
            finally:
                await browser.close()
    
    async def _search_products_api(self):
        """Tìm kiếm sản phẩm qua Tiki API - nhanh và ổn định hơn"""
        products = []
        
        # API endpoint của Tiki
        api_url = "https://tiki.vn/api/v2/products"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Referer': f'https://tiki.vn/search?q={self.search_term.replace(" ", "+")}',
            'x-guest-token': 'default'
        }
        
        params = {
            'limit': min(self.max_products, 40),  # Tiki giới hạn 40/request
            'include': 'advertisement',
            'aggregations': '2',
            'q': self.search_term
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Parse dữ liệu từ API
                        items = data.get('data', [])
                        
                        for item in items[:self.max_products]:
                            try:
                                product = {
                                    'id': item.get('id'),
                                    'name': item.get('name', ''),
                                    'link': f"https://tiki.vn/{item.get('url_path', '')}" if item.get('url_path') else f"https://tiki.vn/product-p{item.get('id')}.html",
                                    'price': item.get('price', 0),
                                    'original_price': item.get('original_price', 0),
                                    'discount': item.get('discount_rate', 0),
                                    'rating': item.get('rating_average', 0),
                                    'review_count': item.get('review_count', 0),
                                    'quantity_sold': item.get('quantity_sold', {}).get('value', 0),
                                    'image': item.get('thumbnail_url', ''),
                                    'badges': item.get('badges_new', []),
                                    'seller': item.get('seller', {}).get('name', ''),
                                    'brand': item.get('brand_name', ''),
                                    'specifications': item.get('specifications', [])
                                }
                                
                                products.append(product)
                                logging.info(f"✅ Tìm thấy: {product['name'][:50]}...")
                                
                            except Exception as e:
                                logging.warning(f"Lỗi khi parse sản phẩm: {e}")
                                continue
                    else:
                        logging.error(f"API trả về status {response.status}")
                        
        except Exception as e:
            logging.error(f"Lỗi khi gọi API: {e}")
        
        return products
    
    async def _search_products(self, page):
        """Tìm kiếm sản phẩm - sử dụng API trước, fallback về scraping nếu cần"""
        # Thử API trước (nhanh hơn)
        products = await self._search_products_api()
        
        if products:
            return products
        
        # Fallback: scrape HTML nếu API fail
        logging.warning("API không hoạt động, chuyển sang scrape HTML...")
        search_url = f"https://tiki.vn/search?q={self.search_term.replace(' ', '+')}"
        
        try:
            await page.goto(search_url, wait_until='networkidle', timeout=60000)
        except:
            await page.goto(search_url, wait_until='domcontentloaded', timeout=60000)
        
        await self._human_like_delay(2, 4)
        
        products = []
        try:
            await page.wait_for_selector('.product-item', timeout=10000)
            product_items = await page.query_selector_all('.product-item')
            
            for item in product_items[:self.max_products]:
                try:
                    product = {}
                    
                    link_elem = await item.query_selector('a')
                    if link_elem:
                        href = await link_elem.get_attribute('href')
                        product['link'] = f"https://tiki.vn{href}" if not href.startswith('http') else href
                    
                    name_elem = await item.query_selector('.name')
                    if name_elem:
                        product['name'] = (await name_elem.inner_text()).strip()
                    
                    price_elem = await item.query_selector('.price-discount__price')
                    if price_elem:
                        product['price'] = (await price_elem.inner_text()).strip()
                    
                    if product.get('link'):
                        products.append(product)
                        logging.info(f"Tìm thấy: {product.get('name', 'Unknown')}")
                        
                except Exception as e:
                    logging.warning(f"Lỗi khi parse sản phẩm: {e}")
                    continue
            
        except Exception as e:
            logging.error(f"Lỗi khi scrape: {e}")
        
        return products
    
    async def _get_product_details_api(self, product_id):
        """Lấy chi tiết sản phẩm qua API - nhanh và đầy đủ hơn"""
        api_url = f"https://tiki.vn/api/v2/products/{product_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Referer': f'https://tiki.vn/product-p{product_id}.html',
            'x-guest-token': 'default'
        }
        
        params = {
            'platform': 'web',
            'version': '3'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        details = {
                            'description': data.get('description', ''),
                            'short_description': data.get('short_description', ''),
                            'specifications': [],
                            'brand': {},
                            'categories': data.get('categories', {}),
                            'images': data.get('images', []),
                            'current_seller': data.get('current_seller', {}),
                            'stock_item': data.get('stock_item', {}),
                            'warranty_info': data.get('warranty_info', ''),
                            'return_and_exchange_policy': data.get('return_and_exchange_policy', '')
                        }
                        
                        # Parse specifications
                        specs_list = data.get('specifications', [])
                        if specs_list:
                            for spec_group in specs_list:
                                attributes = spec_group.get('attributes', [])
                                for attr in attributes:
                                    details['specifications'].append({
                                        'name': attr.get('name', ''),
                                        'value': attr.get('value', '')
                                    })
                        
                        # Brand info
                        brand_data = data.get('brand', {})
                        if brand_data:
                            details['brand'] = {
                                'id': brand_data.get('id'),
                                'name': brand_data.get('name', '')
                            }
                        
                        return details
                    else:
                        logging.warning(f"API trả về status {response.status} cho product {product_id}")
                        return None
                        
        except Exception as e:
            logging.error(f"Lỗi khi gọi API chi tiết sản phẩm: {e}")
            return None
    
    async def _scrape_product_details(self, page, product):
        """Lấy chi tiết sản phẩm - ưu tiên API, fallback HTML nếu cần"""
        product_id = product.get('id')
        
        if product_id:
            # Thử lấy từ API trước
            logging.info(f"📡 Lấy chi tiết qua API cho product {product_id}...")
            details = await self._get_product_details_api(product_id)
            
            if details:
                # Merge details vào product
                product.update(details)
                
                # Lấy reviews (có thể có API riêng)
                product['reviews'] = await self._get_reviews_api(product_id)
                return
        
        # Fallback: Scrape HTML nếu API fail hoặc không có product_id
        logging.warning(f"⚠️ API không hoạt động, scrape HTML cho {product.get('name', 'Unknown')[:50]}...")
        
        try:
            await page.goto(product['link'], wait_until='networkidle', timeout=60000)
            await self._human_like_delay(2, 4)
            
            # Lấy mô tả
            try:
                desc_elem = await page.query_selector('.ToggleContent__Wrapper-sc-fbuwol-0, .content')
                if desc_elem:
                    product['description'] = (await desc_elem.inner_text()).strip()
            except:
                product['description'] = ""
            
            # Lấy thông số kỹ thuật
            try:
                specs = []
                spec_rows = await page.query_selector_all('.ProductInfo__TableSpecs-sc-1j7z4jf-0 tr')
                for row in spec_rows:
                    cells = await row.query_selector_all('td')
                    if len(cells) >= 2:
                        specs.append({
                            'name': (await cells[0].inner_text()).strip(),
                            'value': (await cells[1].inner_text()).strip()
                        })
                product['specifications'] = specs
            except:
                product['specifications'] = []
            
            # Scroll xuống phần reviews
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await self._human_like_delay(1, 2)
            
            # Lấy reviews từ HTML
            product['reviews'] = await self._scrape_reviews(page)
            
        except Exception as e:
            logging.error(f"Lỗi khi lấy chi tiết sản phẩm: {e}")
            product['description'] = ""
            product['specifications'] = []
            product['reviews'] = []
    
    async def _get_reviews_api(self, product_id):
        """Lấy reviews qua API"""
        reviews = []
        api_url = f"https://tiki.vn/api/v2/reviews"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
            'Accept': 'application/json, text/plain, */*',
            'Referer': f'https://tiki.vn/product-p{product_id}.html',
            'x-guest-token': 'default'
        }
        
        params = {
            'product_id': product_id,
            'limit': min(self.max_reviews, 20),  # Tiki thường limit 20/page
            'sort': 'score|desc,id|desc,stars|all',
            'page': 1
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        review_data = data.get('data', [])
                        
                        for review_item in review_data[:self.max_reviews]:
                            try:
                                review = {
                                    'id': review_item.get('id'),
                                    'title': review_item.get('title', ''),
                                    'content': review_item.get('content', ''),
                                    'rating': review_item.get('rating', 0),
                                    'author': review_item.get('created_by', {}).get('name', 'Anonymous'),
                                    'time': review_item.get('created_at', ''),
                                    'helpful_count': review_item.get('thank_count', 0),
                                    'images': review_item.get('images', []),
                                    'timeline': review_item.get('timeline', {}),
                                    'customer_reviewed': review_item.get('customer_reviewed', {})
                                }
                                reviews.append(review)
                            except Exception as e:
                                logging.warning(f"Lỗi parse review: {e}")
                                continue
                        
                        logging.info(f"✅ Lấy được {len(reviews)} reviews từ API")
                    else:
                        logging.warning(f"Review API trả về status {response.status}")
                        
        except Exception as e:
            logging.error(f"Lỗi khi lấy reviews API: {e}")
        
        return reviews
    
    async def _scrape_reviews(self, page):
        """Lấy reviews từ HTML (fallback)"""
        reviews = []
        try:
            # Click vào tab đánh giá
            try:
                review_tab = await page.query_selector('div[role="tab"]:has-text("Đánh giá")')
                if review_tab:
                    await review_tab.click()
                    await self._human_like_delay(1, 2)
            except:
                pass
            
            # Đợi reviews load
            try:
                await page.wait_for_selector('[data-view-id="pdp_review_list"]', timeout=5000)
            except PlaywrightTimeout:
                logging.info("Không tìm thấy reviews cho sản phẩm này")
                return reviews
            
            review_count = 0
            review_items = await page.query_selector_all('[data-view-id="pdp_review_item"]')
            
            for item in review_items:
                if review_count >= self.max_reviews:
                    break
                
                try:
                    review = {}
                    
                    author_elem = await item.query_selector('[data-view-id="pdp_review_author"]')
                    if author_elem:
                        review['author'] = (await author_elem.inner_text()).strip()
                    
                    rating_elem = await item.query_selector('[data-view-id="pdp_review_rating"]')
                    if rating_elem:
                        stars = await rating_elem.query_selector_all('svg')
                        review['rating'] = len(stars)
                    
                    time_elem = await item.query_selector('[data-view-id="pdp_review_time"]')
                    if time_elem:
                        review['time'] = (await time_elem.inner_text()).strip()
                    
                    content_elem = await item.query_selector('[data-view-id="pdp_review_content"]')
                    if content_elem:
                        review['content'] = (await content_elem.inner_text()).strip()
                    
                    helpful_elem = await item.query_selector('[data-view-id="pdp_review_helpful"]')
                    if helpful_elem:
                        review['helpful'] = (await helpful_elem.inner_text()).strip()
                    
                    reviews.append(review)
                    review_count += 1
                    
                except Exception as e:
                    logging.warning(f"Lỗi khi parse review: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"Lỗi khi lấy reviews: {e}")
        
        return reviews
    
    def _save_data(self):
        """Lưu dữ liệu vào file JSON"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.products_data, f, ensure_ascii=False, indent=2)
            logging.info(f"Đã lưu {len(self.products_data)} sản phẩm vào {self.output_file}")
        except Exception as e:
            logging.error(f"Lỗi khi lưu file: {e}")


async def main():
    parser = argparse.ArgumentParser(description='Tiki Scraper sử dụng Playwright')
    parser.add_argument('-k', '--keyword', default='iPhone', help='Từ khóa tìm kiếm')
    parser.add_argument('-n', '--num', type=int, default=10, help='Số lượng sản phẩm')
    parser.add_argument('-r', '--reviews', type=int, default=20, help='Số lượng reviews tối đa mỗi sản phẩm')
    parser.add_argument('--headless', action='store_true', help='Chạy browser ẩn')
    
    args = parser.parse_args()
    
    scraper = TikiPlaywrightScraper(
        search_term=args.keyword,
        max_products=args.num,
        max_reviews=args.reviews,
        headless=args.headless
    )
    
    await scraper.scrape()


if __name__ == "__main__":
    asyncio.run(main())
