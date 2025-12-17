import asyncio
import json
import logging
import argparse
import re
import random
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from tqdm.asyncio import tqdm

# Setup logging
import sys
# Force UTF-8 encoding for console output
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shopee_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class ShopeePlaywrightScraper:
    def __init__(self, search_term, max_products=10, max_reviews=30, headless=False):
        """
        Scraper sử dụng Playwright để lấy dữ liệu từ Shopee.
        
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
        self.output_file = f"shopee_{re.sub(r'[^a-z0-9_]+', '', search_term.lower())}.json"
        self.products_data = []
        self.cookies_file = "shopee_cookies.json"
        self.state_file = "shopee_state.json"
        
    async def _save_cookies(self, context):
        """Lưu cookies và storage state để duy trì đăng nhập"""
        try:
            await context.storage_state(path=self.state_file)
            logging.info(f"Đã lưu session vào {self.state_file}")
        except Exception as e:
            logging.warning(f"Không thể lưu session: {e}")
    
    async def _load_cookies(self):
        """Kiểm tra xem có file cookies/state đã lưu không"""
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
            viewport_height = page.viewport_size['height']
            current_scroll = 0
            scroll_steps = random.randint(3, 6)
            
            for _ in range(scroll_steps):
                scroll_amount = random.randint(200, 500)
                current_scroll += scroll_amount
                await page.evaluate(f'window.scrollTo({{top: {current_scroll}, behavior: "smooth"}})')
                await self._human_like_delay(0.5, 1.5)
            
            # Scroll back lên một chút (giống người xem lại)
            if random.random() > 0.5:
                await page.evaluate(f'window.scrollBy({{top: -{random.randint(100, 300)}, behavior: "smooth"}})')
                await self._human_like_delay(0.3, 0.8)
                
        except Exception as e:
            logging.warning(f'Lỗi khi simulate human behavior: {e}')
    
    async def _check_login_required(self, page):
        """Kiểm tra xem trang có yêu cầu đăng nhập hoặc xác minh không"""
        try:
            page_url = page.url.lower()
            
            # Kiểm tra URL cho các trang yêu cầu xác thực
            blocked_patterns = [
                '/verify/traffic',  # Trang xác minh traffic/captcha
                '/verify/error',
                'login',
                'captcha'
            ]
            
            if any(pattern in page_url for pattern in blocked_patterns):
                return True
                
            return False
        except:
            return False
    
    async def _handle_login(self, page):
        """Xử lý đăng nhập hoặc xác minh captcha"""
        page_url = page.url
        
        if '/verify/traffic' in page_url or '/verify/error' in page_url:
            logging.warning("=" * 70)
            logging.warning("🤖 SHOPEE YÊU CẦU XÁC MINH (CAPTCHA/TRAFFIC VERIFICATION)")
            logging.warning("=" * 70)
            logging.warning("Shopee phát hiện hoạt động bất thường và yêu cầu xác minh.")
            logging.warning("")
            logging.warning("📋 HƯỚNG DẪN:")
            logging.warning("1. Trong cửa sổ browser, hoàn thành CAPTCHA hoặc xác minh")
            logging.warning("2. Đợi cho đến khi được chuyển về trang bình thường")
            logging.warning("3. Quay lại console này và nhấn Enter")
            logging.warning("=" * 70)
        else:
            logging.warning("=" * 60)
            logging.warning("🔐 SHOPEE YÊU CẦU ĐĂNG NHẬP")
            logging.warning("=" * 60)
            logging.warning("Vui lòng đăng nhập vào Shopee trong cửa sổ browser.")
            logging.warning("Sau khi đăng nhập thành công, nhấn Enter ở console này...")
            logging.warning("=" * 60)
            
            # Chỉ chuyển về trang chủ nếu không phải trang verify
            if 'verify' not in page_url:
                try:
                    await page.goto("https://shopee.vn", wait_until='domcontentloaded', timeout=30000)
                except:
                    pass
        
        # Đợi người dùng hoàn thành
        input("\n>>> Nhấn Enter sau khi hoàn thành: ")
        
        logging.info("✅ Tiếp tục scraping...")
        await page.wait_for_timeout(3000)
    
    async def scrape(self):
        """Hàm chính để scrape dữ liệu"""
        async with async_playwright() as p:
            # Sử dụng Firefox để tránh bị phát hiện
            try:
                browser = await p.firefox.launch(
                    headless=self.headless,
                    firefox_user_prefs={
                        'dom.webdriver.enabled': False,
                        'useAutomationExtension': False,
                        'privacy.trackingprotection.enabled': True,
                        'geo.enabled': True,
                        'geo.provider.use_corelocation': True,
                        'geo.prompt.testing': True,
                        'geo.prompt.testing.allow': True
                    }
                )
                logging.info("✅ Đang sử dụng Firefox")
            except Exception as e:
                logging.error(f"❌ Không thể khởi động Firefox: {e}")
                raise
            
            # Kiểm tra xem có state đã lưu không
            has_saved_state = await self._load_cookies()
            
            if has_saved_state:
                logging.info("Tìm thấy session đã lưu. Đang load...")
                context = await browser.new_context(
                    storage_state=self.state_file,
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
                    locale='vi-VN',
                    timezone_id='Asia/Ho_Chi_Minh',
                    geolocation={'latitude': 10.8231, 'longitude': 106.6297},
                    permissions=['geolocation']
                )
            else:
                logging.info("Không tìm thấy session. Tạo session mới...")
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
                    locale='vi-VN',
                    timezone_id='Asia/Ho_Chi_Minh',
                    geolocation={'latitude': 10.8231, 'longitude': 106.6297},
                    permissions=['geolocation']
                )
            
            # Thêm script để ẩn dấu hiệu automation cho Firefox
            await context.add_init_script("""
                // Xóa webdriver flag
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });
                
                // Override các thuộc tính automation
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    get: () => 1
                });
                
                // Giả mạo permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Override battery API
                if (navigator.getBattery) {
                    navigator.getBattery = () => Promise.resolve({
                        charging: true,
                        chargingTime: 0,
                        dischargingTime: Infinity,
                        level: 1.0,
                        addEventListener: () => {},
                        removeEventListener: () => {}
                    });
                }
            """)
            
            page = await context.new_page()
            
            try:
                # 1. Tìm kiếm sản phẩm
                logging.info(f"Đang tìm kiếm: {self.search_term}")
                products = await self._search_products(page)
                
                # Kiểm tra nếu cần đăng nhập hoặc xác minh
                max_retries = 3
                retry_count = 0
                
                while await self._check_login_required(page) and retry_count < max_retries:
                    await self._handle_login(page)
                    # Thử tìm kiếm lại sau khi xác minh/đăng nhập
                    products = await self._search_products(page)
                    retry_count += 1
                    
                    if not await self._check_login_required(page):
                        break
                    
                    if retry_count >= max_retries:
                        logging.error("❌ Vẫn không thể truy cập sau nhiều lần thử. Vui lòng thử lại sau.")
                        return
                
                if not products:
                    logging.error("Không tìm thấy sản phẩm nào!")
                    return
                
                # Lưu session sau khi xác nhận có thể truy cập
                await self._save_cookies(context)
                
                # 2. Lấy chi tiết và reviews cho từng sản phẩm
                logging.info(f"Tìm thấy {len(products)} sản phẩm. Đang lấy chi tiết...")
                for idx, product in enumerate(tqdm(products, desc="Scraping products")):
                    try:
                        # Delay ngẫu nhiên giữa các sản phẩm (quan trọng!)
                        if idx > 0:
                            await self._human_like_delay(3, 7)
                        
                        await self._scrape_product_details(page, product)
                        self.products_data.append(product)
                        
                        # Lưu session và data định kỳ
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
    
    async def _search_products(self, page):
        """Tìm kiếm và lấy danh sách sản phẩm"""
        search_url = f"https://shopee.vn/search?keyword={self.search_term.replace(' ', '%20')}&sortBy=sales"
        
        try:
            await page.goto(search_url, wait_until='networkidle', timeout=60000)
        except:
            await page.goto(search_url, wait_until='domcontentloaded', timeout=60000)
        
        # Delay tự nhiên sau khi load trang
        await self._human_like_delay(2, 4)
        
        # Simulate hành vi người dùng
        await self._simulate_human_behavior(page)
        
        # Kiểm tra lại nếu bị redirect về trang đăng nhập
        if await self._check_login_required(page):
            logging.warning("Trang yêu cầu đăng nhập!")
            return []
        
        products = []
        try:
            # Đợi container sản phẩm xuất hiện
            await page.wait_for_selector('.shopee-search-item-result__items', timeout=10000)
            
            # Lấy thông tin các sản phẩm
            product_cards = await page.query_selector_all('.shopee-search-item-result__item')
            
            for card in product_cards[:self.max_products]:
                try:
                    product = {}
                    
                    # Link sản phẩm
                    link_elem = await card.query_selector('a')
                    if link_elem:
                        href = await link_elem.get_attribute('href')
                        product['link'] = f"https://shopee.vn{href}" if href.startswith('/') else href
                    
                    # Tên sản phẩm
                    name_elem = await card.query_selector('[data-sqe="name"]')
                    if name_elem:
                        product['name'] = await name_elem.inner_text()
                    
                    # Giá
                    price_elem = await card.query_selector('.fxMUzH, .JRplV8')
                    if price_elem:
                        product['price'] = await price_elem.inner_text()
                    
                    # Rating
                    rating_elem = await card.query_selector('.rES4jh')
                    if rating_elem:
                        product['rating'] = await rating_elem.inner_text()
                    
                    # Số lượng đã bán
                    sold_elem = await card.query_selector('.CTxYvB')
                    if sold_elem:
                        product['sold'] = await sold_elem.inner_text()
                    
                    # Hình ảnh
                    img_elem = await card.query_selector('img')
                    if img_elem:
                        product['image'] = await img_elem.get_attribute('src')
                    
                    # Location
                    location_elem = await card.query_selector('.mAKokq')
                    if location_elem:
                        product['location'] = await location_elem.inner_text()
                    
                    if product.get('link'):
                        products.append(product)
                        
                except Exception as e:
                    logging.warning(f"Lỗi khi parse sản phẩm: {e}")
                    continue
            
        except Exception as e:
            logging.error(f"Lỗi khi tìm kiếm sản phẩm: {e}")
        
        return products
    
    async def _scrape_product_details(self, page, product):
        """Lấy chi tiết sản phẩm và reviews"""
        try:
            await page.goto(product['link'], wait_until='networkidle', timeout=60000)
            
            # Delay tự nhiên
            await self._human_like_delay(2, 4)
            
            # Kiểm tra nếu gặp trang xác minh
            if await self._check_login_required(page):
                logging.warning(f"⚠️ Gặp trang xác minh khi truy cập {product.get('name', 'Unknown')}")
                await self._handle_login(page)
                # Thử load lại trang sản phẩm
                await page.goto(product['link'], wait_until='domcontentloaded', timeout=60000)
                await self._human_like_delay(2, 3)
            
            # Simulate hành vi xem sản phẩm
            await self._simulate_human_behavior(page)
            
            # Lấy mô tả
            try:
                desc_elem = await page.query_selector('.nq5KNw, [data-sqe="description"]')
                if desc_elem:
                    product['description'] = await desc_elem.inner_text()
            except:
                product['description'] = ""
            
            # Lấy category
            try:
                breadcrumb = await page.query_selector('.breadcrumb')
                if breadcrumb:
                    product['category'] = await breadcrumb.inner_text()
            except:
                product['category'] = ""
            
            # Scroll xuống phần reviews
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(2000)
            
            # Lấy reviews
            product['reviews'] = await self._scrape_reviews(page)
            
        except Exception as e:
            logging.error(f"Lỗi khi lấy chi tiết sản phẩm: {e}")
            product['description'] = ""
            product['category'] = ""
            product['reviews'] = []
    
    async def _scrape_reviews(self, page):
        """Lấy reviews của sản phẩm"""
        reviews = []
        try:
            # Đợi phần reviews xuất hiện
            try:
                await page.wait_for_selector('.product-ratings__list, .shopee-product-rating', timeout=5000)
            except PlaywrightTimeout:
                logging.info("Không tìm thấy reviews cho sản phẩm này")
                return reviews
            
            review_count = 0
            page_num = 1
            max_pages = (self.max_reviews // 6) + 1  # Mỗi trang thường có ~6 reviews
            
            while review_count < self.max_reviews and page_num <= max_pages:
                # Lấy reviews trong trang hiện tại
                review_items = await page.query_selector_all('.shopee-product-rating__main, .product-rating-item')
                
                for item in review_items:
                    if review_count >= self.max_reviews:
                        break
                    
                    try:
                        review = {}
                        
                        # Tên người đánh giá
                        author_elem = await item.query_selector('.shopee-product-rating__author-name, .author-name')
                        if author_elem:
                            review['author'] = await author_elem.inner_text()
                        
                        # Rating (số sao)
                        stars_elem = await item.query_selector('.shopee-product-rating__rating, .rating-stars')
                        if stars_elem:
                            stars_html = await stars_elem.inner_html()
                            review['rating'] = stars_html.count('icon-rating-solid--active')
                        
                        # Thời gian
                        time_elem = await item.query_selector('.shopee-product-rating__time, .time')
                        if time_elem:
                            review['time'] = await time_elem.inner_text()
                        
                        # Nội dung review
                        content_elem = await item.query_selector('.shopee-product-rating__content, .review-content')
                        if content_elem:
                            review['content'] = await content_elem.inner_text()
                        
                        # Phản hồi từ shop
                        seller_reply_elem = await item.query_selector('.shopee-product-rating__shop-reply, .seller-reply')
                        if seller_reply_elem:
                            review['seller_reply'] = await seller_reply_elem.inner_text()
                        else:
                            review['seller_reply'] = ""
                        
                        # Số lượt thích
                        like_elem = await item.query_selector('.shopee-product-rating__like-count, .like-count')
                        if like_elem:
                            like_text = await like_elem.inner_text()
                            review['likes'] = like_text
                        else:
                            review['likes'] = "0"
                        
                        reviews.append(review)
                        review_count += 1
                        
                    except Exception as e:
                        logging.warning(f"Lỗi khi parse review: {e}")
                        continue
                
                # Thử click nút next page
                if review_count < self.max_reviews:
                    try:
                        next_button = await page.query_selector('.shopee-icon-button--right, .product-rating-overview__page-next')
                        if next_button:
                            is_disabled = await next_button.get_attribute('disabled')
                            if not is_disabled:
                                await next_button.click()
                                await page.wait_for_timeout(2000)
                                page_num += 1
                            else:
                                break
                        else:
                            break
                    except:
                        break
                else:
                    break
                    
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
    parser = argparse.ArgumentParser(description='Shopee Scraper sử dụng Playwright')
    parser.add_argument('-k', '--keyword', default='Raspberry pi', help='Từ khóa tìm kiếm')
    parser.add_argument('-n', '--num', type=int, default=10, help='Số lượng sản phẩm')
    parser.add_argument('-r', '--reviews', type=int, default=30, help='Số lượng reviews tối đa mỗi sản phẩm')
    parser.add_argument('--headless', action='store_true', help='Chạy browser ẩn')
    
    args = parser.parse_args()
    
    scraper = ShopeePlaywrightScraper(
        search_term=args.keyword,
        max_products=args.num,
        max_reviews=args.reviews,
        headless=args.headless
    )
    
    await scraper.scrape()


if __name__ == "__main__":
    asyncio.run(main())