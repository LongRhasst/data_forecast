import asyncio
import json
import logging
from pathlib import Path
from tiki_data import TikiPlaywrightScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

async def run_batch_scraping():
    """Chạy thu thập dữ liệu hàng loạt theo keywords"""
    
    # Đọc file keywords
    keywords_file = Path(__file__).parent / 'search_keywork.json'
    with open(keywords_file, 'r', encoding='utf-8') as f:
        keywords_data = json.load(f)
    
    # Cấu hình cho từng nhóm
    configs = {
        'phone': {'max_products': 50, 'sleep': 30},
        'clothing': {'max_products': 50, 'sleep': 30},
        'motorcycle': {'max_products': 10, 'sleep': 30},
        'laptop': {'max_products': 10, 'sleep': 30}
    }
    
    all_keywords = []
    
    # Parse keywords
    for item in keywords_data:
        # Phone keywords
        if 'phone' in item:
            for keyword in item['phone']:
                all_keywords.append({
                    'keyword': keyword,
                    'category': 'phone',
                    'max_products': configs['phone']['max_products'],
                    'sleep': configs['phone']['sleep']
                })
        
        # Clothing keywords
        if 'clothing' in item:
            for category, keywords in item['clothing'].items():
                for keyword in keywords:
                    all_keywords.append({
                        'keyword': keyword,
                        'category': 'clothing',
                        'max_products': configs['clothing']['max_products'],
                        'sleep': configs['clothing']['sleep']
                    })
        
        # Motorcycle keywords
        if 'motorcycle' in item:
            for keyword in item['motorcycle']:
                all_keywords.append({
                    'keyword': keyword,
                    'category': 'motorcycle',
                    'max_products': configs['motorcycle']['max_products'],
                    'sleep': configs['motorcycle']['sleep']
                })
        
        # Laptop keywords
        if 'laptop' in item:
            for keyword in item['laptop']:
                all_keywords.append({
                    'keyword': keyword,
                    'category': 'laptop',
                    'max_products': configs['laptop']['max_products'],
                    'sleep': configs['laptop']['sleep']
                })
    
    logging.info(f"🚀 Bắt đầu thu thập dữ liệu cho {len(all_keywords)} keywords")
    logging.info(f"📊 Tổng quan:")
    logging.info(f"   - Phone: {len([k for k in all_keywords if k['category'] == 'phone'])} keywords x 50 sản phẩm")
    logging.info(f"   - Clothing: {len([k for k in all_keywords if k['category'] == 'clothing'])} keywords x 50 sản phẩm")
    logging.info(f"   - Motorcycle: {len([k for k in all_keywords if k['category'] == 'motorcycle'])} keywords x 10 sản phẩm")
    logging.info(f"   - Laptop: {len([k for k in all_keywords if k['category'] == 'laptop'])} keywords x 10 sản phẩm")
    
    # Chạy scraping cho từng keyword
    for idx, kw_info in enumerate(all_keywords, 1):
        keyword = kw_info['keyword']
        category = kw_info['category']
        max_products = kw_info['max_products']
        sleep_time = kw_info['sleep']
        
        logging.info(f"\n{'='*80}")
        logging.info(f"📦 [{idx}/{len(all_keywords)}] Đang thu thập: '{keyword}' (Category: {category})")
        logging.info(f"   ├─ Số sản phẩm: {max_products}")
        logging.info(f"   └─ Sleep sau khi hoàn thành: {sleep_time}s")
        logging.info(f"{'='*80}\n")
        
        try:
            # Tạo scraper với cấu hình phù hợp
            scraper = TikiPlaywrightScraper(
                search_term=keyword,
                max_products=max_products,
                max_reviews=20,  # Giữ nguyên 20 reviews mỗi sản phẩm
                headless=True  # Chạy ẩn để nhanh hơn
            )
            
            # Chạy scraper
            await scraper.scrape()
            
            logging.info(f"✅ Hoàn thành thu thập cho '{keyword}'")
            
        except Exception as e:
            logging.error(f"❌ Lỗi khi thu thập '{keyword}': {e}")
            continue
        
        # Sleep giữa các request
        if idx < len(all_keywords):
            logging.info(f"⏳ Đang chờ {sleep_time} giây trước khi thu thập keyword tiếp theo...")
            await asyncio.sleep(sleep_time)
    
    logging.info(f"\n{'='*80}")
    logging.info(f"🎉 HOÀN THÀNH! Đã thu thập xong {len(all_keywords)} keywords")
    logging.info(f"{'='*80}")

if __name__ == "__main__":
    asyncio.run(run_batch_scraping())
